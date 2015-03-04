//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "msg_loop.h"

#include <chrono>
#include <numeric>
#include <thread>
#include "include/Logger.h"
#include "src/port/port.h"
#include "src/messages/serializer.h"
#include "src/messages/messages.h"
#include "src/util/common/base_env.h"

namespace {
// free up any thread local storage for worker_ids.
void free_thread_local(void *ptr) {
  int* intp = static_cast<int *>(ptr);
  delete intp;
}
}

namespace rocketspeed {

//
// This is registered with the event loop. The event loop invokes
// this call on every message received.
void
MsgLoop::EventCallback(std::unique_ptr<Message> msg) {
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
  MsgLoopBase(env),
  worker_id_(new ThreadLocalPtr(free_thread_local)),
  env_options_(env_options),
  info_log_(info_log),
  name_(name),
  next_worker_id_(0),
  stats_("rocketspeed." + name) {
  assert(info_log);
  assert(num_workers >= 1);

  // Setup host id
  char myname[1024];
  gethostname(&myname[0], sizeof(myname));
  hostid_ = HostId(std::string(myname), port);

  // Generate client ID from the host ID if none was specified.
  if (client_id.empty()) {
    client_id = hostid_.ToClientId();
  } else {
    // Provided client ID shouldn't include the worker byte.
    client_id.push_back('a');
  }

  // Setup event loop client IDs.
  assert(num_workers < 256);
  worker_client_ids_.reset(new ClientID[num_workers]);
  for (int i = 0; i < num_workers; ++i) {
    worker_client_ids_[i] = client_id;
    worker_client_ids_[i].back() = static_cast<char>('a' + i);
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

void MsgLoop::RegisterCommandCallback(CommandType type,
                                      CommandCallbackType callback) {
  // Cannot call this when it is already running.
  assert(!IsRunning());

  for (auto& loop : event_loops_) {
    loop->RegisterCallback(type, callback);
  }
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

  // Add goodbye callback if it hasn't already been added.
  if (msg_callbacks_.find(MessageType::mGoodbye) == msg_callbacks_.end()) {
    msg_callbacks_[MessageType::mGoodbye] =
      [this] (std::unique_ptr<Message> msg) {
        // Ignore, just log it.
        MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
        LOG_INFO(info_log_, "Goodbye %d received for client %s",
          static_cast<int>(goodbye->GetCode()),
          goodbye->GetOrigin().c_str());
      };
  }

  // Starting from 1, run worker loops on new threads.
  for (size_t i = 1; i < event_loops_.size(); ++i) {
    BaseEnv::ThreadId tid = env_->StartThread(
      [this, i] () {
        // Set this thread's worker index.
        worker_id_->Reset(new int(static_cast<int>(i)));

        event_loops_[i]->Run();

        //No longer running event loop.
        worker_id_->Reset(new int(-1));
      },
      name_ + "-" + std::to_string(i));
    worker_threads_.push_back(tid);
  }

  // Main loop run on this thread.
  assert(event_loops_.size() >= 1);

  worker_id_->Reset(new int(0)); // This thread is worker 0.
  event_loops_[0]->Run();
  worker_id_->Reset(new int(-1)); // No longer running event loop.
}

void MsgLoop::Stop() {
  for (auto& event_loop : event_loops_) {
    event_loop->Stop();
  }

  for (BaseEnv::ThreadId tid : worker_threads_) {
    env_->WaitForJoin(tid);
  }
  worker_threads_.clear();

  LOG_INFO(info_log_, "Stopped a Message Loop at port %ld", (long)hostid_.port);
  info_log_->Flush();
}

MsgLoop::~MsgLoop() {
  Stop();
}

void MsgLoop::SendCommandToSelf(std::unique_ptr<Command> command) {
  event_loops_[GetThreadWorkerIndex()]->Dispatch(std::move(command),
                                                 env_->NowMicros());
}

Status MsgLoop::SendMessage(const Message& msg,
                            ClientID recipient,
                            int worker_id,
                            bool is_new_request) {
  std::string serial;
  msg.SerializeToString(&serial);
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(serial),
                              std::move(recipient),
                              is_new_request));
  return SendCommand(std::move(cmd), worker_id);
}

//
// This is the system's handling of the ping message.
// Applications can override this behaviour if desired.
void
MsgLoop::ProcessPing(std::unique_ptr<Message> msg) {
  // get the ping request message
  ThreadCheck();
  MessagePing* request = static_cast<MessagePing*>(msg.get());
  if (request->GetPingType() == MessagePing::Response) {
    LOG_INFO(info_log_, "Received ping response");
  } else {
    const ClientID& origin = request->GetOrigin();

    // change it to a ping response message
    request->SetPingType(MessagePing::Response);

    // send response to origin.
    Status st = SendResponse(*request, origin, GetThreadWorkerIndex());

    if (!st.ok()) {
      LOG_WARN(info_log_,
          "Unable to send ping response to %s",
          origin.c_str());
    } else {
      LOG_INFO(info_log_,
          "Send ping response to %s",
          origin.c_str());
    }
  }
}

int MsgLoop::LoadBalancedWorkerId() const {
  // Find the event loop with minimum load.
  int worker_id = static_cast<int>(next_worker_id_++ % event_loops_.size());
  /*uint64_t load_factor = event_loops_[worker_id]->GetLoadFactor();
  for (int i = 0; i < static_cast<int>(event_loops_.size()); ++i) {
    uint64_t next_load_factor = event_loops_[i]->GetLoadFactor();
    if (next_load_factor < load_factor) {
      worker_id = i;
      load_factor = next_load_factor;
    }
  }*/
  return worker_id;
}

bool MsgLoop::CheckMessageOrigin(const Message* msg) {
  const int worker_id = GetThreadWorkerIndex();

  if (msg->GetOrigin() != GetClientId(worker_id)) {
    stats_.bad_origin->Add(1);
    LOG_ERROR(info_log_,
      "Received message with incorrect origin. Expected '%s', received '%s'",
      GetClientId(worker_id).c_str(),
      msg->GetOrigin().c_str());
    info_log_->Flush();
    assert(false);
    return false;
  }
  return true;
}

Status MsgLoop::WaitUntilRunning(std::chrono::seconds timeout) {
  for (auto& event_loop : event_loops_) {
    Status st = event_loop->WaitUntilRunning(timeout);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

int MsgLoop::GetNumClientsSync() {
  int result = 0;
  port::Semaphore done;
  Status st;
  do {
    // Attempt to gather num clients from each event loop.
    st = Gather([this] (int i) { return event_loops_[i]->GetNumClients(); },
                [&done, &result] (std::vector<int> clients) {
                  result = std::accumulate(clients.begin(), clients.end(), 0);
                  done.Post();
                });
    if (!st.ok()) {
      // Failed, delay for a while.
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  } while (!st.ok());
  done.Wait();
  return result;
}


}  // namespace rocketspeed
