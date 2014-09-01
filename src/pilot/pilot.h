// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include "src/messages/serializer.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/log_router.h"
#include "src/util/storage.h"
#include "src/pilot/options.h"

namespace rocketspeed {

class Pilot {
 public:
  // A new instance of a Pilot
  static Status CreateNewInstance(PilotOptions options,
                                  const Configuration& conf,
                                  Pilot** pilot);
  virtual ~Pilot();

  // Start this instance of the Pilot
  void Run();

  // Is the Pilot up and running?
  bool IsRunning() { return msg_loop_.IsRunning(); }

  // Returns the sanitized options used by the pilot
  PilotOptions& GetOptions() { return options_; }

 private:
  // The options used by the Pilot
  PilotOptions options_;

  // The configuration of this rocketspeed instance
  Configuration conf_;

  // Message specific callbacks stored here
  const std::map<MessageType, MsgCallbackType> callbacks_;

  // The message loop base.
  MsgLoop msg_loop_;

  // Interface with LogDevice
  std::unique_ptr<LogStorage> log_storage_;

  // Log router for mapping topic names to logs.
  LogRouter log_router_;

  // private Constructor
  Pilot(PilotOptions options,
        const Configuration& conf);

  // Sanitize input options if necessary
  PilotOptions SanitizeOptions(PilotOptions options);

  // callbacks to process incoming messages
  static void ProcessData(ApplicationCallbackContext ctx,
                          std::unique_ptr<Message> msg);

  void SendAck(const HostId& host,
               const MsgId& msgid,
               MessageDataAck::AckStatus status);

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
