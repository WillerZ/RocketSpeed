// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <inttypes.h>
#include <sys/types.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/thread.h>
#include <event2/util.h>
#include <memory>
#include <map>
#include "include/Env.h"
#include "src/messages/serializer.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/auto_roll_logger.h"
#include "src/controltower/options.h"

namespace rocketspeed {

class ControlTower {
 public:
  // A new instance of a Control Tower
  static Status CreateNewInstance(const ControlTowerOptions& options,
                                  const Configuration& conf,
                                  ControlTower** ct);
  virtual ~ControlTower();

  // Start this instance of the Control Tower
  void Run(void);

  // Is the ControlTower up and running?
  bool IsRunning() { return msg_loop_.IsRunning(); }

  // Returns the sanitized options used by the control tower
  ControlTowerOptions& GetOptions() {return options_;}

 private:
  // The options used by the Control Tower
  ControlTowerOptions options_;

  // The configuration of this rocketspeed instance
  Configuration conf_;

  // Message specific callbacks stored here
  const std::map<MessageType, MsgCallbackType> callbacks_;

  // The message loop base.
  MsgLoop msg_loop_;

  // private Constructor
  ControlTower(const ControlTowerOptions& options,
               const Configuration& conf);

  // Sanitize input options if necessary
  ControlTowerOptions SanitizeOptions(const ControlTowerOptions& src);

  // callbacks to process incoming messages
  static void ProcessData(std::unique_ptr<Message> msg);
  static void ProcessMetadata(std::unique_ptr<Message> msg);
  static std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
