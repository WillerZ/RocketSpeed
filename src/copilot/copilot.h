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
  // A new instance of a Copilot
  static Status CreateNewInstance(CopilotOptions options,
                                  Copilot** copilot);
  virtual ~Copilot();

  // Start this instance of the Copilot
  void Run();

  // Is the Copilot up and running?
  bool IsRunning() { return msg_loop_.IsRunning(); }

  // Returns the sanitized options used by the copilot
  CopilotOptions& GetOptions() { return options_; }

  // Get HostID
  const HostId& GetHostId() const { return msg_loop_.GetHostId(); }

  // Sends a command to the msgloop
  Status SendCommand(std::unique_ptr<Command> command) {
    return msg_loop_.SendCommand(std::move(command));
  }

 private:
  // The options used by the Copilot
  CopilotOptions options_;

  // Message specific callbacks stored here
  const std::map<MessageType, MsgCallbackType> callbacks_;

  // The message loop base.
  MsgLoop msg_loop_;

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

  // callbacks to process incoming messages
  static void ProcessData(ApplicationCallbackContext ctx,
                          std::unique_ptr<Message> msg);
  static void ProcessMetadata(ApplicationCallbackContext ctx,
                              std::unique_ptr<Message> msg);

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
