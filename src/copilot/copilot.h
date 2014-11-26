// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <thread>
#include <vector>
#include "src/copilot/options.h"
#include "src/copilot/worker.h"
#include "src/messages/serializer.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/log_router.h"
#include "src/util/storage.h"
#include "src/util/control_tower_router.h"

namespace rocketspeed {

class Copilot {
 public:
  static const int DEFAULT_PORT = 58600;

  // A new instance of a Copilot
  static Status CreateNewInstance(CopilotOptions options,
                                  Copilot** copilot);
  virtual ~Copilot();

  // Returns the sanitized options used by the copilot
  CopilotOptions& GetOptions() { return options_; }

  // Get HostID
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  // Get the client id of a worker thread on this copilot
  const ClientID& GetClientId(int worker_id) const {
    return options_.msg_loop->GetClientId(worker_id);
  }

  // Sends a command to a msgloop worker.
  Status SendCommand(std::unique_ptr<Command> command, int worker_id) {
    return options_.msg_loop->SendCommand(std::move(command), worker_id);
  }

  // Get the worker loop associated with a log.
  int GetLogWorker(LogID logid) const;

  const ControlTowerRouter& GetControlTowerRouter() const {
    return control_tower_router_;
  }

 private:
  // The options used by the Copilot
  CopilotOptions options_;

  // Log router for mapping topic names to logs.
  LogRouter log_router_;

  // Worker objects and threads, these have their own message loops.
  std::vector<std::unique_ptr<CopilotWorker>> workers_;
  std::vector<std::thread> worker_threads_;

  // Control tower router. Workers will access this, but don't own it.
  ControlTowerRouter control_tower_router_;

  // private Constructor
  explicit Copilot(CopilotOptions options);

  // Sanitize input options if necessary
  CopilotOptions SanitizeOptions(CopilotOptions options);

  // Start the background workers.
  void StartWorkers();

  // callbacks to process incoming messages
  void ProcessDeliver(std::unique_ptr<Message> msg);
  void ProcessMetadata(std::unique_ptr<Message> msg);

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
