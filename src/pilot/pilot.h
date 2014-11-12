// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <thread>
#include <vector>
#include "src/messages/serializer.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/log_router.h"
#include "src/util/storage.h"
#include "src/pilot/options.h"
#include "src/pilot/worker.h"

namespace rocketspeed {

class Pilot {
 public:
  static const int DEFAULT_PORT = 58600;

  // A new instance of a Pilot
  static Status CreateNewInstance(PilotOptions options,
                                  Pilot** pilot);
  virtual ~Pilot();

  // Returns the sanitized options used by the pilot
  PilotOptions& GetOptions() { return options_; }

  // Get HostID
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  // Get the subscriber id of this pilot
  const ClientID& GetPilotId() const {
    return pilot_id_;
  }

  // Sends a command to the msgloop
  Status SendCommand(std::unique_ptr<Command> command) {
    return options_.msg_loop->SendCommand(std::move(command));
  }

  Statistics GetStatistics() const;

 private:
  // The options used by the Pilot
  PilotOptions options_;

  // Interface with LogDevice
  std::shared_ptr<LogStorage> log_storage_;

  // Log router for mapping topic names to logs.
  LogRouter log_router_;

  // Worker objects and threads, these have their own message loops.
  std::vector<std::unique_ptr<PilotWorker>> workers_;
  std::vector<std::thread> worker_threads_;

 // My subscriber id
 const ClientID pilot_id_;

  // private Constructor
  explicit Pilot(PilotOptions options);

  // Sanitize input options if necessary
  PilotOptions SanitizeOptions(PilotOptions options);

  // Start the worker threads in the background.
  void StartWorkers();

  // callbacks to process incoming messages
  void ProcessPublish(std::unique_ptr<Message> msg);

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
