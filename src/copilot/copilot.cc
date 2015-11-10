// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "copilot.h"

#include <map>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "src/client/client.h"
#include "src/copilot/control_tower_router.h"
#include "src/util/common/fixed_configuration.h"
#include "src/util/memory.h"
#include "src/util/storage.h"

namespace rocketspeed {

namespace {
class EmptyTowerRouter : public ControlTowerRouter {
 public:
  Status GetControlTowers(LogID logID,
                          std::vector<HostId const*>* out) const override {
    return Status::NotFound("Empty CT router");
  }
};
}  // namespace

/**
 * Sanitize user-specified options
 */
CopilotOptions Copilot::SanitizeOptions(CopilotOptions options) {
  if (options.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(options.env,
                                       options.log_dir,
                                       "LOG.copilot",
                                       options.log_file_time_to_roll,
                                       options.max_log_file_size,
                                       options.info_log_level,
                                       &options.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      options.info_log = std::make_shared<NullLogger>();
    }
  }

  if (options.control_tower_router == nullptr) {
    options.control_tower_router = std::make_shared<EmptyTowerRouter>();
  }

  return options;
}

/**
 * Private constructor for a Copilot
 */
Copilot::Copilot(CopilotOptions options, std::unique_ptr<ClientImpl> client):
  options_(SanitizeOptions(std::move(options))),
  client_(std::move(client)) {
  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());
  options_.msg_loop->RegisterTimerCallback(
    [this] () {
      ProcessTimerTick();
    },
    std::chrono::microseconds(options_.timer_interval_micros));

  // Create workers.
  const int num_workers = options_.msg_loop->GetNumWorkers();
  for (int i = 0; i < num_workers; ++i) {
    workers_.emplace_back(new CopilotWorker(options_,
                                            options_.control_tower_router,
                                            i,
                                            this,
                                            client_));
  }
  sub_id_map_.resize(num_workers);

  // Create queues.
  for (int i = 0; i < num_workers; ++i) {
    client_to_worker_queues_.emplace_back(
      options_.msg_loop->CreateWorkerQueues());
    tower_to_worker_queues_.emplace_back(
      options_.msg_loop->CreateWorkerQueues());
    router_update_queues_.emplace_back(
      options_.msg_loop->CreateThreadLocalQueues(i));
  }

  LOG_VITAL(options_.info_log, "Created a new Copilot");
  options_.info_log->Flush();
}

Copilot::~Copilot() {
  // Must be stopped first.
  assert(workers_.empty());
  options_.info_log->Flush();
}

void Copilot::Stop() {
  assert(!options_.msg_loop->IsRunning());

  if (client_) {
    client_->Stop();
  }
  workers_.clear();
  options_.log_router.reset();
}

/**
 * This is a static method to create a Copilot
 */
Status Copilot::CreateNewInstance(CopilotOptions options,
                                  Copilot** copilot) {
  if (!options.log_router) {
    assert(false);
    return Status::InvalidArgument("Log router must be provided");
  }
  std::unique_ptr<ClientImpl> client;
  if (options.rollcall_enabled) {
    // Publishing to the rollcall topic needs a pilot.
    if (options.pilots.size() <= 0) {
      assert(options.pilots.size());
      return Status::InvalidArgument("At least one pilot much be provided.");
    }
    // Create a configuration to determine the identity of a pilot.
    // Use a dummy copilot identifier, this is not needed and can be
    // removed in the future.
    ClientOptions client_options;
    client_options.config =
      std::make_shared<FixedConfiguration>(options.pilots[0], HostId());

    // Create a client to write rollcall topic.
    Status status = ClientImpl::Create(std::move(client_options),
                                       &client,
                                       true);
    if (!status.ok()) {
      return status;
    }
  }
  *copilot = new Copilot(std::move(options), std::move(client));
  return Status::OK();
}

// A static callback method to process MessageData
void Copilot::ProcessDeliver(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();

  const int event_loop_worker = options_.msg_loop->GetThreadWorkerIndex();

  // get the data message
  MessageDeliverData* data = static_cast<MessageDeliverData*>(msg.get());
  int worker_id = CopilotWorker::SubscriptionIDWorker(data->GetSubID(),
                                                      workers_.size());
  LOG_DEBUG(options_.info_log,
            "Received deliver (%.16s)@%" PRIu64 " for worker %d",
            data->GetPayload().ToString().c_str(),
            data->GetSequenceNumber(),
            worker_id);

  // forward message to worker
  auto& worker = workers_[worker_id];
  auto command =
    worker->WorkerCommand(LogID(0), std::move(msg), event_loop_worker, origin);
  auto& queue = tower_to_worker_queues_[event_loop_worker][worker_id];
  if (!queue->Write(command)) {
    LOG_WARN(options_.info_log,
             "Worker %d queue is full.",
             static_cast<int>(worker_id));
  }
}

void Copilot::ProcessGap(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();

  const int event_loop_worker = options_.msg_loop->GetThreadWorkerIndex();

  // get the gap message
  MessageDeliverGap* gap = static_cast<MessageDeliverGap*>(msg.get());
  int worker_id = CopilotWorker::SubscriptionIDWorker(gap->GetSubID(),
                                                      workers_.size());
  LOG_DEBUG(options_.info_log,
            "Received gap %" PRIu64 "-%" PRIu64 " for worker %d",
            gap->GetFirstSequenceNumber(),
            gap->GetLastSequenceNumber(),
            worker_id);

  // forward message to worker
  auto& worker = workers_[worker_id];
  auto command =
    worker->WorkerCommand(LogID(0), std::move(msg), event_loop_worker, origin);
  auto& queue = tower_to_worker_queues_[event_loop_worker][worker_id];
  if (!queue->Write(command)) {
    LOG_WARN(options_.info_log,
             "Worker %d queue is full.",
             static_cast<int>(worker_id));
  }
}

void Copilot::ProcessTailSeqno(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();

  const int event_loop_worker = options_.msg_loop->GetThreadWorkerIndex();

  // get the tail seqno message
  MessageTailSeqno* tail_seqno = static_cast<MessageTailSeqno*>(msg.get());

  LOG_DEBUG(options_.info_log,
            "Received tail seqno %" PRIu64 " for Topic(%s,%s)",
            tail_seqno->GetSequenceNumber(),
            tail_seqno->GetNamespace().c_str(),
            tail_seqno->GetTopicName().c_str());

  // map the topic to a logid
  LogID logid;
  Status st = options_.log_router->GetLogID(tail_seqno->GetNamespace(),
                                            tail_seqno->GetTopicName(),
                                            &logid);
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
             "Unable to map msg to logid %s",
             st.ToString().c_str());
    return;
  }

  // calculate the destination worker
  int worker_id = GetLogWorker(logid);
  auto& worker = workers_[worker_id];

  // forward message to worker
  auto command =
    worker->WorkerCommand(logid, std::move(msg), event_loop_worker, origin);
  auto& queue = tower_to_worker_queues_[event_loop_worker][worker_id];
  if (!queue->Write(command)) {
    LOG_WARN(options_.info_log,
             "Worker %d queue is full.",
             static_cast<int>(worker_id));
  }
}

void Copilot::ProcessSubscribe(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();

  auto subscribe = static_cast<MessageSubscribe*>(msg.get());
  LOG_DEBUG(options_.info_log,
            "Received subscribe request for Topic(%s, %s)@%" PRIu64,
            subscribe->GetNamespace().c_str(),
            subscribe->GetTopicName().c_str(),
            subscribe->GetStartSequenceNumber());

  // Calculate log ID for this topic.
  LogID logid;
  Status st = options_.log_router->GetLogID(subscribe->GetNamespace(),
                                            subscribe->GetTopicName(),
                                            &logid);
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
             "Unable to map Topic(%s, %s) to LogID: %s",
             subscribe->GetNamespace().c_str(),
             subscribe->GetTopicName().c_str(),
             st.ToString().c_str());
    return;
  }

  // Calculate the destination worker.
  auto dest_worker_id = GetLogWorker(logid);
  auto& worker = workers_[dest_worker_id];

  auto worker_id = options_.msg_loop->GetThreadWorkerIndex();
  sub_id_map_[worker_id].Insert(origin, subscribe->GetSubID(), dest_worker_id);

  auto command =
    worker->WorkerCommand(logid, std::move(msg), worker_id, origin);
  auto& queue = client_to_worker_queues_[worker_id][dest_worker_id];

  // Forward message to responsible worker.
  if (!queue->Write(command)) {
    LOG_WARN(options_.info_log, "Worker %d queue is full.", worker_id);
  }
}

void Copilot::ProcessUnsubscribe(std::unique_ptr<Message> msg,
                                 StreamID origin) {
  options_.msg_loop->ThreadCheck();

  const int this_worker = options_.msg_loop->GetThreadWorkerIndex();
  auto unsubscribe = static_cast<MessageUnsubscribe*>(msg.get());
  const auto sub_id = unsubscribe->GetSubID();

  int worker_id = -1;
  switch (unsubscribe->GetReason()) {
    case MessageUnsubscribe::Reason::kRequested:
      // Inbound unsubscription from client.
      LOG_DEBUG(options_.info_log,
                "Received client unsubscribe for subscription (%" PRIu64
                ") at stream (%llu)",
                sub_id,
                origin);

      // Use client subscription ID map to find worker.
      if (!sub_id_map_[this_worker].MoveOut(origin, sub_id, &worker_id)) {
        return;
      }
      break;

    case MessageUnsubscribe::Reason::kBackOff:
    case MessageUnsubscribe::Reason::kInvalid:
      // Outbound unsubscription from control tower.
      LOG_WARN(options_.info_log,
               "Received server unsubscribe for subscription (%" PRIu64
               ") at stream (%llu)",
               sub_id,
               origin);

      // Map subscription ID back using CopilotWorker mapping.
      worker_id = CopilotWorker::SubscriptionIDWorker(sub_id, workers_.size());
      break;
  }

  assert(worker_id != -1);
  auto command = workers_[worker_id]->WorkerCommand(LogID(0),
                                                    std::move(msg),
                                                    this_worker,
                                                    origin);
  auto& queue = client_to_worker_queues_[this_worker][worker_id];
  queue->Write(command);
}

void Copilot::ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();
  int event_loop_worker = options_.msg_loop->GetThreadWorkerIndex();

  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
  switch (goodbye->GetOriginType()) {
    case MessageGoodbye::OriginType::Client: {
      sub_id_map_[event_loop_worker].Remove(origin);
      LOG_DEBUG(options_.info_log, "Received goodbye for client %llu", origin);
      break;
    }

    case MessageGoodbye::OriginType::Server: {
      LOG_DEBUG(options_.info_log, "Received goodbye for server %llu", origin);
      break;
    }
  }

  // Inform all workers.
  for (int i = 0; i < options_.msg_loop->GetNumWorkers(); ++i) {
    std::unique_ptr<Message> new_msg(
      new MessageGoodbye(goodbye->GetTenantID(),
                         goodbye->GetCode(),
                         goodbye->GetOriginType()));
    auto command = workers_[i]->WorkerCommand(LogID(0),
                                              std::move(new_msg),
                                              event_loop_worker,
                                              origin);
    auto& queue =
      goodbye->GetOriginType() == MessageGoodbye::OriginType::Client ?
      client_to_worker_queues_[event_loop_worker][i] :
      tower_to_worker_queues_[event_loop_worker][i];
    queue->Write(command);
  }
}

void Copilot::ProcessTimerTick() {
  // This is invoked once per MsgLoop worker thread.
  const int worker_id = options_.msg_loop->GetThreadWorkerIndex();
  workers_[worker_id]->ProcessTimerTick();
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType> Copilot::InitializeCallbacks() {
  using namespace std::placeholders;
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mDeliverData] = [this](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    ProcessDeliver(std::move(msg), origin);
  };
  cb[MessageType::mDeliverGap] = [this](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    ProcessGap(std::move(msg), origin);
  };
  cb[MessageType::mTailSeqno] = [this](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    ProcessTailSeqno(std::move(msg), origin);
  };
  cb[MessageType::mGoodbye] = [this](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    ProcessGoodbye(std::move(msg), origin);
  };
  cb[MessageType::mSubscribe] = [this](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    ProcessSubscribe(std::move(msg), origin);
  };
  cb[MessageType::mUnsubscribe] = [this](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    ProcessUnsubscribe(std::move(msg), origin);
  };
  return cb;
}

int Copilot::GetLogWorker(LogID log_id) const {
  return static_cast<int>(log_id % options_.msg_loop->GetNumWorkers());
}

int Copilot::GetTowerWorker(LogID log_id, const HostId& tower) const {
  // Hash control tower to a worker.
  const int num_workers = options_.msg_loop->GetNumWorkers();
  const size_t connection = log_id % options_.control_tower_connections;
  const size_t hash = tower.Hash();
  return static_cast<int>((hash + connection) % num_workers);
}

Statistics Copilot::GetStatisticsSync() const {
  return options_.msg_loop->AggregateStatsSync(
      [this](int i) { return workers_[i]->GetStatistics(); });
}

std::string Copilot::GetInfoSync(std::vector<std::string> args) {
  if (args.size() >= 1) {
    if (args[0] == "towers_for_log" && args.size() == 2 && !args[1].empty()) {
      // towers_for_log n  -- what towers are serving a log.
      const LogID log_id { strtoul(&*args[1].begin(), nullptr, 10) };
      std::string result;
      auto worker_id = GetLogWorker(log_id);
      Status st =
        options_.msg_loop->WorkerRequestSync(
          [this, log_id, worker_id] () {
            return workers_[worker_id]->GetTowersForLog(log_id);
          },
          worker_id,
          &result);
      return st.ok() ? result : st.ToString();
    } else if (args[0] == "log_for_topic" && args.size() == 3) {
      // log_for_topic namespace topic_name
      LogID log_id;
      Status st = options_.log_router->GetLogID(args[1], args[2], &log_id);
      return st.ok() ? std::to_string(log_id) : st.ToString();
    } else if (args[0] == "subscriptions" && args.size() >= 2) {
      // subscriptions FILTER MAX -- information about topics.
      std::string result;
      std::string filter = args[1];
      int max = args.size() == 3 ? atoi(&*args[2].begin()) : 1;
      Status st =
        options_.msg_loop->MapReduceSync(
          [this, filter, max] (int worker_id) {
            return workers_[worker_id]->GetSubscriptionInfo(filter, max);
          },
          [] (std::vector<std::string> infos) {
            return std::accumulate(infos.begin(), infos.end(), std::string());
          },
          &result);
      return st.ok() ? result : st.ToString();
    }
  }
  return "Unknown info for copilot";
}

Status Copilot::UpdateTowerRouter(std::shared_ptr<ControlTowerRouter> router) {
  Status result;
  // Send the new nodes to all workers.
  // If we fail to forward to any single worker then return failure so that
  // whoever is providing the updated hosts can retry later.
  for (int i = 0; i < options_.msg_loop->GetNumWorkers(); ++i) {
    // Send command to worker on the thread-local queue.
    auto command = workers_[i]->WorkerCommand(router);
    if (!router_update_queues_[i]->GetThreadLocal()->Write(command)) {
      LOG_WARN(options_.info_log,
        "Failed to forward control tower update to worker %" PRIu32, i);
      result = Status::NoBuffer();
    }
  }
  return result;
}

}  // namespace rocketspeed
