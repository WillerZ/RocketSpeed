//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "msg_loop.h"

#include "include/Logger.h"
#include "src/messages/serializer.h"
#include "src/messages/messages.h"
#include "src/util/common/base_env.h"

namespace rocketspeed {

thread_local int MsgLoop::worker_id_ = -1;

//
// This is registered with the event loop. The event loop invokes
// this call on every message received.
void
MsgLoop::EventCallback(std::unique_ptr<Message> msg) {
  // find the MsgLoop that we are working for
  assert(msg);

  // what message have we received?
  MessageType type = msg->GetMessageType();
  LOG_INFO(info_log_,
      "Received message %d at port %ld", type, (long)hostid_.port);

  // Search for a callback method corresponding to this msg type
  // Give up ownership of this message to the callback function
  std::map<MessageType, MsgCallbackType>::const_iterator iter =
    msg_callbacks_.find(type);
  if (iter != msg_callbacks_.end()) {
    iter->second(std::move(msg));
  } else {
    // If the user has not registered a message of this type,
    // then this msg will be droped silently.
    LOG_WARN(info_log_,
        "No registered msg callback for msg type %d", type);
    info_log_->Flush();
    assert(0);
  }
}

/**
 * Constructor for a Message Loop
 */
MsgLoop::MsgLoop(BaseEnv* env,
                 const EnvOptions& env_options,
                 int port,
                 int num_workers,
                 const std::shared_ptr<Logger>& info_log,
                 std::string name,
                 ClientID client_id):
  env_(env),
  env_options_(env_options),
  info_log_(info_log),
  name_(std::move(name)),
  next_worker_id_(0) {
  assert(num_workers >= 1);

  // Setup host id
  char myname[1024];
  gethostname(&myname[0], sizeof(myname));
  hostid_ = HostId(std::string(myname), port);

  // Generate client ID from the host ID if none was specified.
  if (client_id.empty()) {
    client_id = hostid_.ToClientId();
  }

  // Setup event loop client IDs.
  assert(num_workers < 256);
  worker_client_ids_.reset(new ClientID[num_workers]);
  for (int i = 0; i < num_workers; ++i) {
    worker_client_ids_[i] = client_id;
    worker_client_ids_[i].push_back(static_cast<char>(i));
  }

  auto event_callback = [this] (std::unique_ptr<Message> msg) {
    EventCallback(std::move(msg));
  };

  auto accept_callback = [this] (int fd) {
    // Assign the new connection to the least loaded event loop.
    event_loops_[LoadBalancedWorkerId()]->Accept(fd);
  };

  for (int i = 0; i < num_workers; ++i) {
    EventLoop* event_loop = new EventLoop(env,
                                          env_options,
                                          i == 0 ? port : 0,
                                          info_log,
                                          event_callback,
                                          accept_callback,
                                          "rocketspeed." + name);
    event_loops_.emplace_back(event_loop);
  }

  // log an informational message
  LOG_INFO(info_log,
      "Created a new Message Loop at port %ld with %zu callbacks",
      (long)hostid_.port, msg_callbacks_.size());
}

void
MsgLoop::RegisterCallbacks(
    const std::map<MessageType, MsgCallbackType>& callbacks) {
  // Cannot call this when it is already running.
  assert(!IsRunning());

  // Add each callback to the registered callbacks.
  for (auto& elem : callbacks) {
    // Not allowed to add duplicates.
    assert(msg_callbacks_.find(elem.first) == msg_callbacks_.end());
    msg_callbacks_[elem.first] = elem.second;
  }
}

void MsgLoop::Run() {
  LOG_INFO(info_log_, "Starting Message Loop at port %ld", (long)hostid_.port);
  env_->SetCurrentThreadName(name_ + "-0");

  // Add ping callback if it hasn't already been added.
  if (msg_callbacks_.find(MessageType::mPing) == msg_callbacks_.end()) {
    msg_callbacks_[MessageType::mPing] = [this] (std::unique_ptr<Message> msg) {
      ProcessPing(std::move(msg));
    };
  }

  // Starting from 1, run worker loops on new threads.
  for (size_t i = 1; i < event_loops_.size(); ++i) {
    Env::ThreadId tid = env_->StartThread(
      [this, i] () {
        worker_id_ = i;  // Set this thread's worker index.
        event_loops_[i]->Run();
        worker_id_ = -1;  // No longer running an event loop.
      },
      name_ + "-" + std::to_string(i));
    worker_threads_.push_back(tid);
  }

  // Main loop run on this thread.
  assert(event_loops_.size() >= 1);

  worker_id_ = 0;  // This thread is worker 0.
  event_loops_[0]->Run();
  worker_id_ = -1;  // No longer running the event loop.
}

void MsgLoop::Stop() {
  for (auto& event_loop : event_loops_) {
    event_loop->Stop();
  }

  for (Env::ThreadId tid : worker_threads_) {
    env_->WaitForJoin(tid);
  }
  worker_threads_.clear();

  LOG_INFO(info_log_, "Stopped a Message Loop at port %ld", (long)hostid_.port);
  info_log_->Flush();
}

MsgLoop::~MsgLoop() {
  Stop();
}

//
// This is the system's handling of the ping message.
// Applications can override this behaviour if desired.
void
MsgLoop::ProcessPing(std::unique_ptr<Message> msg) {
  // get the ping request message
  ThreadCheck();
  MessagePing* request = static_cast<MessagePing*>(msg.get());
  const ClientID origin = request->GetOrigin();

  // change it to a ping response message
  request->SetPingType(MessagePing::Response);

  // serialize response
  std::string serial;
  request->SerializeToString(&serial);

  // send reponse
  std::unique_ptr<Command> cmd(new PingCommand(std::move(serial),
                                               origin,
                                               env_->NowMicros()));
  Status st = SendCommand(std::move(cmd));

  if (!st.ok()) {
    LOG_INFO(info_log_,
        "Unable to send ping response to %s",
        origin.c_str());
  } else {
    LOG_INFO(info_log_,
        "Send ping response to %s",
        origin.c_str());
  }
  info_log_->Flush();
}

int MsgLoop::LoadBalancedWorkerId() const {
  // Find the event loop with minimum load.
  int worker_id = next_worker_id_++ % event_loops_.size();
  uint64_t load_factor = event_loops_[worker_id]->GetLoadFactor();
  for (int i = 0; i < static_cast<int>(event_loops_.size()); ++i) {
    uint64_t next_load_factor = event_loops_[i]->GetLoadFactor();
    if (next_load_factor < load_factor) {
      worker_id = i;
      load_factor = next_load_factor;
    }
  }
  return worker_id;
}

}  // namespace rocketspeed
