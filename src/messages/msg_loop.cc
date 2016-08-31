//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "msg_loop.h"

#include <algorithm>
#include <chrono>
#include <numeric>
#include <thread>

#include "include/BaseEnv.h"
#include "include/Logger.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/load_balancer.h"
#include "src/messages/messages.h"
#include "src/messages/queues.h"
#include "src/messages/serializer.h"
#include "src/messages/stream_allocator.h"
#include "src/port/port.h"
#include "external/folly/Memory.h"

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
void MsgLoop::EventCallback(Flow* flow,
                            std::unique_ptr<Message> msg,
                            StreamID origin) {
  RS_ASSERT(msg);

  // what message have we received?
  MessageType type = msg->GetMessageType();
  LOG_DEBUG(info_log_,
            "Received message %s at %s",
            MessageTypeName(type),
            GetHostId().ToString().c_str());

  // Search for a callback method corresponding to this msg type
  // Give up ownership of this message to the callback function
  std::map<MessageType, MsgCallbackType>::const_iterator iter =
    msg_callbacks_.find(type);
  if (iter != msg_callbacks_.end()) {
    iter->second(flow, std::move(msg), origin);
  } else {
    // If the user has not registered a message of this type,
    // then this msg will be dropped silently.
    LOG_WARN(info_log_,
        "No registered msg callback for msg type %d",
        static_cast<int>(type));
    info_log_->Flush();
    RS_ASSERT(0);
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
                 std::string stats_prefix,
                 MsgLoop::Options options)
    : env_(env)
    , worker_index_(&free_thread_local)
    , env_options_(env_options)
    , info_log_(info_log)
    , stats_prefix_(stats_prefix) {
  RS_ASSERT(info_log);
  RS_ASSERT(num_workers >= 1);

  auto get_load = [this](size_t worker_id) {
    return this->event_loops_[worker_id]->GetLoadFactor();
  };
  load_balancer_ = folly::make_unique<PowerOfTwoLoadBalancer>(get_load);

  const auto event_callback = [this](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    EventCallback(flow, std::move(msg), origin);
  };

  const auto accept_callback = [this](int fd) {
    // Assign the new connection to the least loaded event loop.
    event_loops_[LoadBalancedWorkerId()]->Accept(fd);
  };

  // Setup EventLoop options.
  options.event_loop.env = env;
  options.event_loop.env_options = env_options;
  options.event_loop.info_log = info_log;
  options.event_loop.stats_prefix = stats_prefix;
  options.event_loop.event_callback = event_callback;
  options.event_loop.accept_callback = accept_callback;
  // Create a stream allocator for the entire stream ID space and split it
  // between each loop.
  auto allocs = stream_allocator_.Divide(num_workers, &stream_mapping_);
  for (int i = 0; i < num_workers; ++i) {
    // Only the first loop will be listening for incoming connections.
    options.event_loop.listener_port = i == 0 ? port : -1;
    // Create the loop.
    auto event_loop = new EventLoop(options.event_loop, std::move(allocs[i]));
    event_loops_.emplace_back(event_loop);
    load_balancer_->AddShard(i);
  }

  // log an informational message
  LOG_INFO(info_log,
           "Created a new Message Loop listening at %s with %zu callbacks",
           GetHostId().ToString().c_str(),
           msg_callbacks_.size());
}

void MsgLoop::RegisterCallbacks(
    const std::map<MessageType, MsgCallbackType>& callbacks) {
  // Cannot call this when it is already running.
  RS_ASSERT(!IsRunning());

  // Add each callback to the registered callbacks.
  for (auto& elem : callbacks) {
    // Not allowed to add duplicates.
    RS_ASSERT(msg_callbacks_.find(elem.first) == msg_callbacks_.end());
    msg_callbacks_[elem.first] = elem.second;
  }
}

Status MsgLoop::RegisterTimerCallback(TimerCallbackType callback,
                                      std::chrono::microseconds period) {
  // Cannot call this when it is already running.
  RS_ASSERT(!IsRunning());

  for (const std::unique_ptr<EventLoop>& loop: event_loops_) {
    auto timed_event = loop->RegisterTimerCallback(callback, period);
    if (timed_event == nullptr) {
      return Status::InternalError("Error creating timed event.");
    }
    timer_callbacks_.emplace_back(std::move(timed_event));
  }
  return Status::OK();
}

int MsgLoop::GetThreadWorkerIndex() const {
  const int* ptr = static_cast<int*>(worker_index_.Get());
  if (ptr && *ptr != -1) {
    return *ptr;
  }
  RS_ASSERT(false);
  return -1;
}

void MsgLoop::SetThreadWorkerIndex(int worker_index) {
  if (int* ptr = static_cast<int*>(worker_index_.Get())) {
    *ptr = worker_index;
  } else {
    worker_index_.Reset(new int(worker_index));
  }
}

size_t MsgLoop::GetQueueSize(int worker_id) const {
  return event_loops_[worker_id]->GetQueueSize();
}

Status MsgLoop::Initialize() {
  LOG_VITAL(info_log_, "Initializing MsgLoop");
  for (auto& event_loop : event_loops_) {
    Status st = event_loop->Initialize();
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

void MsgLoop::Run() {
  const std::string base_name = env_->GetCurrentThreadName();
  env_->SetCurrentThreadName(base_name + "-0");
  LOG_INFO(
      info_log_, "Starting Message Loop at %s", GetHostId().ToString().c_str());

  // Add ping callback if it hasn't already been added.
  if (msg_callbacks_.find(MessageType::mPing) == msg_callbacks_.end()) {
    msg_callbacks_[MessageType::mPing] = [this](
        Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
      ProcessPing(std::move(msg), origin);
    };
  }

  // Add heartbeat callback if it hasn't already been added.
  if (msg_callbacks_.find(MessageType::mHeartbeat) == msg_callbacks_.end()) {
    msg_callbacks_[MessageType::mHeartbeat] = [](
        Flow*, std::unique_ptr<Message>, StreamID) {
      // ignore heartbeats
    };
  }

  // Add goodbye callback if it hasn't already been added.
  if (msg_callbacks_.find(MessageType::mGoodbye) == msg_callbacks_.end()) {
    msg_callbacks_[MessageType::mGoodbye] = [this](
        Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
      // Ignore, just log it.
      MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
      LOG_INFO(info_log_,
               "Goodbye %d received for client %llu",
               static_cast<int>(goodbye->GetCode()),
               origin);
    };
  }

  // Starting from 1, run worker loops on new threads.
  for (size_t i = 1; i < event_loops_.size(); ++i) {
    BaseEnv::ThreadId tid = env_->StartThread(
      [this, i] () {
        // Set this thread's worker index.
        SetThreadWorkerIndex(static_cast<int>(i));

        event_loops_[i]->Run();

        // No longer running event loop.
        SetThreadWorkerIndex(-1);
      },
      base_name + "-" + std::to_string(i));
    worker_threads_.push_back(tid);
  }

  // Main loop run on this thread.
  RS_ASSERT(event_loops_.size() >= 1);

  SetThreadWorkerIndex(0);  // This thread is worker 0.
  event_loops_[0]->Run();
  SetThreadWorkerIndex(-1); // No longer running event loop.
}

void MsgLoop::Stop() {
  LOG_VITAL(info_log_, "Stopping MsgLoop");
  for (auto& event_loop : event_loops_) {
    event_loop->Stop();
  }

  for (BaseEnv::ThreadId tid : worker_threads_) {
    env_->WaitForJoin(tid);
  }
  worker_threads_.clear();

  LOG_INFO(info_log_,
           "Stopped a Message Loop at %s",
           GetHostId().ToString().c_str());
  info_log_->Flush();
}

MsgLoop::~MsgLoop() {
  LOG_VITAL(info_log_, "Destroying MsgLoop");
  RS_ASSERT(!IsRunning());
}

StreamAllocator* MsgLoop::GetOutboundStreamAllocator(int worker_id) {
  RS_ASSERT(worker_id >= 0 &&
            worker_id < static_cast<int>(event_loops_.size()));
  return event_loops_[worker_id]->GetOutboundStreamAllocator();
}

StreamSocket MsgLoop::CreateOutboundStream(HostId destination,
                                           int worker_id) {
  RS_ASSERT(worker_id >= 0 &&
            worker_id < static_cast<int>(event_loops_.size()));
  // Corresponding call in event loop is not thread safe, so we need to provide
  // external synchronisation.
  std::lock_guard<std::mutex> lock(stream_allocation_mutex_);
  return event_loops_[worker_id]->CreateOutboundStream(std::move(destination));
}

void MsgLoop::SendCommandToSelf(std::unique_ptr<Command> command) {
  auto loop = event_loops_[GetThreadWorkerIndex()].get();
  SourcelessFlow flow(loop->GetFlowControl());
  loop->Dispatch(&flow, std::move(command));
}

Status MsgLoop::SendCommand(std::unique_ptr<Command> command, int worker_id) {
  RS_ASSERT(worker_id >= 0 &&
            worker_id < static_cast<int>(event_loops_.size()));
  return event_loops_[worker_id]->SendCommand(command);
}

void MsgLoop::SendControlCommand(std::unique_ptr<Command> command,
                                 int worker_id) {
  RS_ASSERT(worker_id >= 0 &&
            worker_id < static_cast<int>(event_loops_.size()));
  event_loops_[worker_id]->SendControlCommand(std::move(command));
}

Status MsgLoop::SendRequest(const Message& msg,
                            StreamSocket* socket,
                            int worker_id) {
  // Create command and append it to the proper event loop.
  RS_ASSERT(event_loops_[worker_id]->IsOutboundStream(socket->GetStreamID()));
  Status st = SendCommand(RequestCommand(msg, socket), worker_id);
  if (st.ok()) {
    socket->Open();
  }
  return st;
}

Status MsgLoop::SendResponse(const Message& msg,
                             StreamID stream,
                             int worker_id) {
  // Create command and append it to the proper event loop.
  RS_ASSERT(event_loops_[worker_id]->IsInboundStream(stream));
  return SendCommand(ResponseCommand(msg, stream), worker_id);
}

Status MsgLoop::SendRequest(const Message& msg,
                            StreamSocket* socket) {
  int worker_id = static_cast<int>(stream_mapping_(socket->GetStreamID()));
  return SendRequest(msg, socket, worker_id);
}

Status MsgLoop::SendResponse(const Message& msg,
                             StreamID stream) {
  int worker_id = static_cast<int>(stream_mapping_(stream));
  return SendResponse(msg, stream, worker_id);
}

std::unique_ptr<Command> MsgLoop::RequestCommand(const Message& msg,
                                                 StreamSocket* socket) {
  // Serialize the message.
  std::string serial;
  msg.SerializeToString(&serial);
  return SerializedSendCommand::Request(std::move(serial), {socket});
}

std::unique_ptr<Command> MsgLoop::ResponseCommand(const Message& msg,
                                                  StreamID stream) {
  // Serialize the message.
  std::string serial;
  msg.SerializeToString(&serial);
  return SerializedSendCommand::Response(std::move(serial), {stream});
}

//
// This is the system's handling of the ping message.
// Applications can override this behaviour if desired.
void
MsgLoop::ProcessPing(std::unique_ptr<Message> msg, StreamID origin) {
  // get the ping request message
  ThreadCheck();
  MessagePing* request = static_cast<MessagePing*>(msg.get());
  if (request->GetPingType() == MessagePing::Response) {
    LOG_INFO(info_log_, "Received ping response");
  } else {
    // Change it to a ping response message
    request->SetPingType(MessagePing::Response);

    // Send response back to the stream.
    Status st = SendResponse(*request, origin, GetThreadWorkerIndex());

    if (!st.ok()) {
      LOG_WARN(
          info_log_, "Unable to send ping response to stream (%llu)", origin);
    } else {
      LOG_INFO(info_log_, "Send ping response to stream (%llu)", origin);
    }
  }
}

int MsgLoop::LoadBalancedWorkerId() const {
  return static_cast<int>(load_balancer_->GetPreferredShard());
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

std::shared_ptr<CommandQueue> MsgLoop::CreateCommandQueue(int worker_id,
                                                          size_t size) {
  RS_ASSERT(worker_id < GetNumWorkers());
  return event_loops_[worker_id]->CreateCommandQueue(size);
}

std::vector<std::shared_ptr<CommandQueue>>
MsgLoop::CreateWorkerQueues(size_t size) {
  std::vector<std::shared_ptr<CommandQueue>> queues;
  for (int i = 0; i < GetNumWorkers(); ++i) {
    queues.emplace_back(CreateCommandQueue(i, size));
  }
  return queues;
}

std::unique_ptr<ThreadLocalCommandQueues>
MsgLoop::CreateThreadLocalQueues(int worker_id, size_t size) {
  return std::unique_ptr<ThreadLocalCommandQueues>(
    new ThreadLocalCommandQueues([this, worker_id, size] () {
      return CreateCommandQueue(worker_id, size);
    }));
}

Statistics MsgLoop::GetStatisticsSync() {
  return AggregateStatsSync([this] (int i) {
    return event_loops_[i]->GetStatistics();
  });
}

Statistics MsgLoop::AggregateStatsSync(WorkerStatsProvider stats_provider) {
  Statistics aggregated_stats;
  port::Semaphore done;

  // Attempt to gather num clients from each event loop.
  ReliableGather(stats_provider,
    [&done, &aggregated_stats] (std::vector<Statistics> clients) {
      for (Statistics& stat: clients) {
        aggregated_stats.Aggregate(stat.MoveThread());
      }
      done.Post();
    });
  done.Wait();

  return aggregated_stats.MoveThread();
}

}  // namespace rocketspeed
