// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/copilot/copilot.h"
#include <map>
#include <string>
#include <thread>
#include <vector>
#include "src/client/client.h"
#include "src/util/memory.h"

namespace rocketspeed {

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

  return std::move(options);
}

void Copilot::StartWorkers() {
  Env* env = options_.env;

  // Start worker threads.
  for (size_t i = 0; i < workers_.size(); ++i) {
    std::string thread_name = "copilotw-" + std::to_string(i);
    auto entry_point = [] (void* arg) {
      CopilotWorker* worker = static_cast<CopilotWorker*>(arg);
      worker->Run();
    };
    worker_threads_.emplace_back(
      env->StartThread(entry_point, (void*)workers_[i].get(), thread_name)
    );
  }

  // Wait for them to start.
  for (const auto& worker : workers_) {
    while (!worker->IsRunning()) {
      std::this_thread::yield();
    }
  }
}

/**
 * Private constructor for a Copilot
 */
Copilot::Copilot(CopilotOptions options, std::unique_ptr<ClientImpl> client):
  options_(SanitizeOptions(std::move(options))),
  control_tower_router_(options_.control_towers,
                        options_.consistent_hash_replicas,
                        options_.control_towers_per_log) {

  stats_.resize(options_.msg_loop->GetNumWorkers());
  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());

  // Create workers.
  for (uint32_t i = 0; i < options_.num_workers; ++i) {
    workers_.emplace_back(new CopilotWorker(options_,
                                            &control_tower_router_,
                                            i,
                                            this));
  }
  // Create Rollcall topic writer
  rollcall_.reset(new RollcallImpl(std::move(client),
                                   InvalidNamespace,
                                   SubscriptionStart(0),
                                   nullptr));

  LOG_INFO(options_.info_log, "Created a new Copilot");
  options_.info_log->Flush();
}

Copilot::~Copilot() {
  Env* env = options_.env;
  // Stop all the workers.
  for (auto& worker : workers_) {
    worker->Stop();
  }

  // Join their threads.
  for (auto& worker_thread : worker_threads_) {
    env->WaitForJoin(worker_thread);
  }

  options_.info_log->Flush();
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
  // Publishing to the rollcall topic needs a pilot.
  if (options.pilots.size() <= 0) {
    assert(options.pilots.size());
    return Status::InvalidArgument("At least one pilot much be provided.");
  }
  // Create a configuration to determine the identity of a pilot.
  // Use a dummy copilot identifier, this is not needed and can be
  // removed in the future.
  std::unique_ptr<Configuration> conf(Configuration::Create(
                                      options.pilots,
                                      { HostId() },
                                      SystemTenant));
  ClientOptions client_options(*conf.get(), "DummyClientId");

  // Create a client to write rollcall topic.
  std::unique_ptr<ClientImpl> client;
  Status status = ClientImpl::Create(std::move(client_options),
                                     &client,
                                     true);
  if (!status.ok()) {
    return status;
  }
  *copilot = new Copilot(std::move(options), std::move(client));
  (*copilot)->StartWorkers();
  return Status::OK();
}

// A static callback method to process MessageData
void Copilot::ProcessDeliver(std::unique_ptr<Message> msg) {
  options_.msg_loop->ThreadCheck();

  const int event_loop_worker = options_.msg_loop->GetThreadWorkerIndex();

  // Check that message has correct origin.
  if (!options_.msg_loop->CheckMessageOrigin(msg.get())) {
    return;
  }

  // get the request message
  MessageData* data = static_cast<MessageData*>(msg.get());

  LOG_INFO(options_.info_log,
      "Received data (%.16s)@%" PRIu64 " for Topic(%s)",
      data->GetPayload().ToString().c_str(),
      data->GetSequenceNumber(),
      data->GetTopicName().ToString().c_str());

  // map the topic to a logid
  LogID logid;
  Status st = options_.log_router->GetLogID(data->GetNamespaceId(),
                                            data->GetTopicName(),
                                            &logid);
  if (!st.ok()) {
    LOG_INFO(options_.info_log,
        "Unable to map msg to logid %s", st.ToString().c_str());
    return;
  }

  // calculate the destination worker
  int worker_id = static_cast<int>(logid % options_.num_workers);
  auto& worker = workers_[worker_id];

  // forward message to worker
  if (!worker->Forward(logid, std::move(msg), event_loop_worker)) {
    LOG_WARN(options_.info_log,
        "Worker %d queue is full.",
        static_cast<int>(worker_id));
  }
}

// A static callback method to process MessageMetadata
void Copilot::ProcessMetadata(std::unique_ptr<Message> msg) {
  options_.msg_loop->ThreadCheck();

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());

  if (request->GetMetaType() == MessageMetadata::MetaType::Response) {
    // Check that message has correct origin.
    if (!options_.msg_loop->CheckMessageOrigin(msg.get())) {
      return;
    }
  }

  // Process each topic
  for (size_t i = 0; i < request->GetTopicInfo().size(); i++) {
    // map the topic to a logid
    const TopicPair& topic = request->GetTopicInfo()[i];

    LOG_INFO(options_.info_log,
      "Received %s %s for Topic(%s)@%" PRIu64,
      topic.topic_type == MetadataType::mSubscribe
        ? "subscribe" : "unsubscribe",
      request->GetMetaType() == MessageMetadata::MetaType::Request
        ? "request" : "response",
      topic.topic_name.c_str(),
      topic.seqno);

    LogID logid;
    Status st = options_.log_router->GetLogID(topic.namespace_id,
                                              topic.topic_name,
                                              &logid);
    if (!st.ok()) {
      LOG_INFO(options_.info_log,
          "Unable to map msg to logid %s", st.ToString().c_str());
      continue;
    }
    // calculate the destination worker
    int worker_id = static_cast<int>(logid % options_.num_workers);
    auto& worker = workers_[worker_id];

    // Copy out only the ith topic into a new message.
    MessageMetadata* newmsg = new MessageMetadata(
                            request->GetTenantID(),
                            request->GetMetaType(),
                            request->GetOrigin(),
                            std::vector<TopicPair> {topic});
    std::unique_ptr<Message> newmessage(newmsg);

    // forward message to worker
    int event_loop_worker = options_.msg_loop->GetThreadWorkerIndex();
    worker->Forward(logid, std::move(newmessage), event_loop_worker);
  }
}

void Copilot::ProcessGoodbye(std::unique_ptr<Message> msg) {
  options_.msg_loop->ThreadCheck();

  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
  if (goodbye->GetOriginType() == MessageGoodbye::OriginType::Client) {
    LOG_INFO(options_.info_log,
      "Received goodbye for client %s",
      goodbye->GetOrigin().c_str());

    // Inform all workers.
    for (uint32_t i = 0; i < options_.num_workers; ++i) {
      std::unique_ptr<Message> new_msg(
        new MessageGoodbye(goodbye->GetTenantID(),
                           goodbye->GetOrigin(),
                           goodbye->GetCode(),
                           goodbye->GetOriginType()));
      LogID logid = 0;  // unused
      int event_loop_worker = options_.msg_loop->GetThreadWorkerIndex();
      workers_[i]->Forward(logid, std::move(new_msg), event_loop_worker);
    }
  }
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType> Copilot::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mDeliver] = [this] (std::unique_ptr<Message> msg) {
    ProcessDeliver(std::move(msg));
  };
  cb[MessageType::mMetadata] = [this] (std::unique_ptr<Message> msg) {
    ProcessMetadata(std::move(msg));
  };
  cb[MessageType::mGoodbye] = [this] (std::unique_ptr<Message> msg) {
    ProcessGoodbye(std::move(msg));
  };
  return cb;
}

int Copilot::GetLogWorker(LogID logid) const {
  const int num_workers = options_.msg_loop->GetNumWorkers();

  // First map logid to control tower.
  ClientID const* control_tower = nullptr;
  Status st = control_tower_router_.GetControlTower(logid, &control_tower);
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
      "Failed to map log ID %" PRIu64 " to a control tower",
      logid);

    // Fallback to log ID-based allocation.
    // This is less efficient (multiple workers talking to same tower)
    // but no less correct.
    return static_cast<int>(logid % num_workers);
  }
  assert(control_tower);

  // Hash control tower to a worker.
  size_t connection = logid % options_.control_tower_connections;
  size_t hash = MurmurHash2<std::string>()(*control_tower);
  return static_cast<int>((hash + connection) % num_workers);
}

Statistics Copilot::GetStatistics() const {
  Statistics aggr;
  for (const Stats& s : stats_) {
    aggr.Aggregate(s.all);
  }
  return aggr;
}

}  // namespace rocketspeed
