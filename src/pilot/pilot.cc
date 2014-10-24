//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/pilot/pilot.h"
#include <map>
#include <string>
#include <thread>
#include <vector>
#include "src/util/logdevice.h"
#include "src/util/memory.h"

namespace rocketspeed {

/**
 * Sanitize user-specified options
 */
PilotOptions Pilot::SanitizeOptions(PilotOptions options) {
  if (options.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(options.env,
                                       options.log_dir,
                                       "LOG.pilot",
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

void Pilot::StartWorkers() {
  // Start worker threads.
  for (size_t i = 0; i < workers_.size(); ++i) {
    worker_threads_.emplace_back(
      [this, i] (PilotWorker* worker) {
        options_.env->SetThreadName(options_.env->GetCurrentThreadId(),
                                    "pilot-" + std::to_string(i));
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
 * Private constructor for a Pilot
 */
Pilot::Pilot(PilotOptions options):
  options_(SanitizeOptions(std::move(options))),
  log_router_(options_.log_range.first, options_.log_range.second) {
  assert(options_.msg_loop);

  if (options_.storage == nullptr) {
    // Create log device client
    LogDeviceStorage* storage = nullptr;
    std::unique_ptr<facebook::logdevice::ClientSettings> clientSettings(
      facebook::logdevice::ClientSettings::create());
    rocketspeed::LogDeviceStorage::Create(
      "rocketspeed.logdevice.primary",
      options_.storage_url,
      "",
      std::chrono::milliseconds(1000),
      std::move(clientSettings),
      options_.env,
      &storage);
    log_storage_.reset(storage);
  } else {
    log_storage_ = options_.storage;
  }

  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());

  // Create workers.
  for (uint32_t i = 0; i < options_.num_workers; ++i) {
    workers_.emplace_back(new PilotWorker(options_,
                                          log_storage_.get(),
                                          this));
  }

  LOG_INFO(options_.info_log, "Created a new Pilot");
  options_.info_log->Flush();
}

Pilot::~Pilot() {
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
 * This is a static method to create a Pilot
 */
Status Pilot::CreateNewInstance(PilotOptions options,
                                Pilot** pilot) {
  *pilot = new Pilot(std::move(options));

  // Ensure we managed to connect to the log storage.
  if ((*pilot)->log_storage_ == nullptr) {
    delete *pilot;
    *pilot = nullptr;
    return Status::NotInitialized();
  }

  (*pilot)->StartWorkers();

  return Status::OK();
}

// A callback method to process MessageData
void Pilot::ProcessPublish(std::unique_ptr<Message> msg) {
  // Sanity checks.
  assert(msg);
  assert(msg->GetMessageType() == MessageType::mPublish);

  // Route topic to log ID.
  auto msg_data = unique_static_cast<MessageData>(std::move(msg));
  LogID logid;
  std::string topic_name = msg_data->GetTopicName().ToString();
  if (!log_router_.GetLogID(topic_name, &logid).ok()) {
    assert(false);  // GetLogID should never fail.
    return;
  }

  LOG_INFO(options_.info_log,
      "Received data (%.16s) for Topic(%s)",
      msg_data->GetPayload().ToString().c_str(),
      msg_data->GetTopicName().ToString().c_str());

  // Forward to worker.
  size_t worker_id = logid % workers_.size();
  if (!workers_[worker_id]->Forward(logid, std::move(msg_data))) {
    LOG_WARN(options_.info_log,
        "Worker %d queue is full.",
        static_cast<int>(worker_id));
  }
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType> Pilot::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mPublish] = [this] (std::unique_ptr<Message> msg) {
    ProcessPublish(std::move(msg));
  };

  // return the updated map
  return cb;
}
}  // namespace rocketspeed
