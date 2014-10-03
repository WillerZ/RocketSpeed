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

void Copilot::Run() {
  // Start worker threads.
  for (auto& worker : workers_) {
    worker_threads_.emplace_back(
      [] (CopilotWorker* worker) { worker->Run(); },
      worker.get());
  }

  // Wait for them to start.
  for (const auto& worker : workers_) {
    while (!worker->IsRunning()) {
      std::this_thread::yield();
    }
  }

  // Start our message loop now.
  msg_loop_.Run();
}

/**
 * Private constructor for a Copilot
 */
Copilot::Copilot(CopilotOptions options):
  options_(SanitizeOptions(std::move(options))),
  callbacks_(InitializeCallbacks()),
  msg_loop_(options_.env,
            options_.env_options,
            HostId(options_.copilotname, options_.port_number),
            options_.info_log,
            static_cast<ApplicationCallbackContext>(this),
            callbacks_),
  log_router_(options_.log_range.first, options_.log_range.second),
  control_tower_router_(options_.control_towers,
                        options_.consistent_hash_replicas,
                        options_.control_towers_per_log) {

  // Create workers.
  for (uint32_t i = 0; i < options_.num_workers; ++i) {
    workers_.emplace_back(new CopilotWorker(options_,
                                            &control_tower_router_,
                                            &msg_loop_.GetClient()));
  }

  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Created a new Copilot");
  options_.info_log->Flush();
}

Copilot::~Copilot() {
  // Stop the main Copilot loop.
  msg_loop_.Stop();

  // Stop all the workers.
  for (auto& worker : workers_) {
    worker->Stop();
  }

  // Join their threads.
  for (auto& worker_thread : worker_threads_) {
    worker_thread.join();
  }
}

/**
 * This is a static method to create a Copilot
 */
Status Copilot::CreateNewInstance(CopilotOptions options,
                                  Copilot** copilot) {
  *copilot = new Copilot(std::move(options));
  return Status::OK();
}

// A static callback method to process MessageData
void Copilot::ProcessData(ApplicationCallbackContext ctx,
                          std::unique_ptr<Message> msg) {
  Copilot* copilot = static_cast<Copilot*>(ctx);

  // get the request message
  MessageData* data = static_cast<MessageData*>(msg.get());

  Log(InfoLogLevel::INFO_LEVEL, copilot->options_.info_log,
      "Received data message for topic '%s'",
      data->GetTopicName().ToString().c_str());

  // map the topic to a logid
  LogID logid;
  Status st = copilot->log_router_.GetLogID(data->GetTopicName().ToString(),
                                            &logid);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, copilot->options_.info_log,
        "Unable to map msg to logid %s", st.ToString().c_str());
    return;
  }

  // calculate the destination worker
  int worker_id = logid % copilot->options_.num_workers;
  auto& worker = copilot->workers_[worker_id];

  // forward message to worker
  worker->Forward(logid, std::move(msg));
}

// A static callback method to process MessageMetadata
void Copilot::ProcessMetadata(ApplicationCallbackContext ctx,
                              std::unique_ptr<Message> msg) {
  Copilot* copilot = static_cast<Copilot*>(ctx);

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());

  // Process each topic
  for (size_t i = 0; i < request->GetTopicInfo().size(); i++) {
    // map the topic to a logid
    TopicPair topic = request->GetTopicInfo()[i];

    Log(InfoLogLevel::INFO_LEVEL, copilot->options_.info_log,
      "Received %s %s for topic '%s'",
      topic.topic_type == MetadataType::mSubscribe
        ? "subscribe" : "unsubscribe",
      request->GetMetaType() == MessageMetadata::MetaType::Request
        ? "request" : "response",
      topic.topic_name.c_str());

    LogID logid;
    Status st = copilot->log_router_.GetLogID(topic.topic_name, &logid);
    if (!st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, copilot->options_.info_log,
          "Unable to map msg to logid %s", st.ToString().c_str());
      continue;
    }
    // calculate the destination worker
    int worker_id = logid % copilot->options_.num_workers;
    auto& worker = copilot->workers_[worker_id];

    // Copy out only the ith topic into a new message.
    MessageMetadata* newmsg = new MessageMetadata(
                            request->GetTenantID(),
                            request->GetMetaType(),
                            request->GetOrigin(),
                            std::vector<TopicPair> {topic});
    std::unique_ptr<Message> newmessage(newmsg);

    // forward message to worker
    worker->Forward(logid, std::move(newmessage));
  }
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType> Copilot::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mData] = MsgCallbackType(&ProcessData);
  cb[MessageType::mMetadata] = MsgCallbackType(&ProcessMetadata);
  return cb;
}
}  // namespace rocketspeed
