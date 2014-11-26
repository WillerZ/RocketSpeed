// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/copilot/copilot.h"
#include <map>
#include <string>
#include <thread>
#include <vector>
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
      options.info_log = nullptr;
    }
  }

  return std::move(options);
}

void Copilot::StartWorkers() {
  // Start worker threads.
  for (size_t i = 0; i < workers_.size(); ++i) {
    worker_threads_.emplace_back(
      [this, i] (CopilotWorker* worker) {
        options_.env->SetCurrentThreadName("copilot-" + std::to_string(i));
        worker->Run();
      },
      workers_[i].get());
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
Copilot::Copilot(CopilotOptions options):
  options_(SanitizeOptions(std::move(options))),
  log_router_(options_.log_range.first, options_.log_range.second),
  control_tower_router_(options_.control_towers,
                        options_.consistent_hash_replicas,
                        options_.control_towers_per_log) {

  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());

  // Create workers.
  for (uint32_t i = 0; i < options_.num_workers; ++i) {
    workers_.emplace_back(new CopilotWorker(options_,
                                            &control_tower_router_,
                                            this));
  }

  LOG_INFO(options_.info_log, "Created a new Copilot");
  options_.info_log->Flush();
}

Copilot::~Copilot() {
  // Stop all the workers.
  for (auto& worker : workers_) {
    worker->Stop();
  }

  // Join their threads.
  for (auto& worker_thread : worker_threads_) {
    worker_thread.join();
  }

  options_.info_log->Flush();
}

/**
 * This is a static method to create a Copilot
 */
Status Copilot::CreateNewInstance(CopilotOptions options,
                                  Copilot** copilot) {
  *copilot = new Copilot(std::move(options));
  (*copilot)->StartWorkers();
  return Status::OK();
}

// A static callback method to process MessageData
void Copilot::ProcessDeliver(std::unique_ptr<Message> msg) {
  options_.msg_loop->ThreadCheck();

  // get the request message
  MessageData* data = static_cast<MessageData*>(msg.get());

  LOG_INFO(options_.info_log,
      "Received data (%.16s)@%lu for Topic(%s)",
      data->GetPayload().ToString().c_str(),
      data->GetSequenceNumber(),
      data->GetTopicName().ToString().c_str());

  // map the topic to a logid
  LogID logid;
  Status st = log_router_.GetLogID(data->GetTopicName(), &logid);
  if (!st.ok()) {
    LOG_INFO(options_.info_log,
        "Unable to map msg to logid %s", st.ToString().c_str());
    return;
  }

  // calculate the destination worker
  int worker_id = logid % options_.num_workers;
  auto& worker = workers_[worker_id];

  // forward message to worker
  int event_loop_worker = MsgLoop::GetThreadWorkerIndex();
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

  // Process each topic
  for (size_t i = 0; i < request->GetTopicInfo().size(); i++) {
    // map the topic to a logid
    TopicPair topic = request->GetTopicInfo()[i];

    LOG_INFO(options_.info_log,
      "Received %s %s for Topic(%s)@%lu",
      topic.topic_type == MetadataType::mSubscribe
        ? "subscribe" : "unsubscribe",
      request->GetMetaType() == MessageMetadata::MetaType::Request
        ? "request" : "response",
      topic.topic_name.c_str(),
      topic.seqno);

    LogID logid;
    Status st = log_router_.GetLogID(topic.topic_name, &logid);
    if (!st.ok()) {
      LOG_INFO(options_.info_log,
          "Unable to map msg to logid %s", st.ToString().c_str());
      continue;
    }
    // calculate the destination worker
    int worker_id = logid % options_.num_workers;
    auto& worker = workers_[worker_id];

    // Copy out only the ith topic into a new message.
    MessageMetadata* newmsg = new MessageMetadata(
                            request->GetTenantID(),
                            request->GetMetaType(),
                            request->GetOrigin(),
                            std::vector<TopicPair> {topic});
    std::unique_ptr<Message> newmessage(newmsg);

    // forward message to worker
    int event_loop_worker = MsgLoop::GetThreadWorkerIndex();
    worker->Forward(logid, std::move(newmessage), event_loop_worker);
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
  return cb;
}

}  // namespace rocketspeed
