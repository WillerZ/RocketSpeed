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
#include "src/util/memory.h"

namespace rocketspeed {

/**
 * Sanitize user-specified options
 */
PilotOptions Pilot::SanitizeOptions(PilotOptions options) {
  if (options.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(options.env,
                                       options.log_dir,
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

void Pilot::Run() {
  // Start worker threads.
  for (auto& worker : workers_) {
    worker_threads_.emplace_back(
      [] (PilotWorker* worker) { worker->Run(); },
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
 * Private constructor for a Pilot
 */
Pilot::Pilot(PilotOptions options,
             const Configuration* conf):
  options_(SanitizeOptions(std::move(options))),
  conf_(conf),
  callbacks_(InitializeCallbacks()),
  msg_loop_(options_.env,
            options_.env_options,
            HostId(options_.pilotname, options_.port_number),
            options_.info_log,
            static_cast<ApplicationCallbackContext>(this),
            callbacks_),
  log_storage_(std::move(options_.log_storage)),
  log_router_(options_.log_range.first, options_.log_range.second) {
  // Create workers.
  for (uint32_t i = 0; i < options_.num_workers_; ++i) {
    workers_.emplace_back(new PilotWorker(options_, log_storage_.get()));
  }

  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Created a new Pilot");
  options_.info_log->Flush();
}

Pilot::~Pilot() {
  // Stop the main Pilot loop.
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
 * This is a static method to create a Pilot
 */
Status Pilot::CreateNewInstance(PilotOptions options,
                                const Configuration* conf,
                                Pilot** pilot) {
  *pilot = new Pilot(std::move(options), conf);

  // Ensure we managed to connect to the log storage.
  if ((*pilot)->log_storage_ == nullptr) {
    delete *pilot;
    *pilot = nullptr;
    return Status::NotInitialized();
  }

  return Status::OK();
}

// A static callback method to process MessageData
void Pilot::ProcessData(ApplicationCallbackContext ctx,
                        std::unique_ptr<Message> msg) {
  Pilot* pilot = static_cast<Pilot*>(ctx);

  // Sanity checks.
  assert(msg);
  assert(msg->GetMessageType() == MessageType::mData);

  // Route topic to log ID.
  auto msg_data = unique_static_cast<MessageData>(std::move(msg));
  LogID logid;
  std::string topic_name = msg_data->GetTopicName().ToString();
  if (!pilot->log_router_.GetLogID(topic_name, &logid).ok()) {
    assert(false);  // GetLogID should never fail.
    return;
  }

  // Forward to worker.
  size_t worker_id = logid % pilot->workers_.size();
  pilot->workers_[worker_id]->Forward(logid, std::move(msg_data));
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType> Pilot::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mData] = MsgCallbackType(&ProcessData);

  // return the updated map
  return cb;
}
}  // namespace rocketspeed
