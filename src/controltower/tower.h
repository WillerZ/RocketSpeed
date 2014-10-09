// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <vector>
#include "src/messages/msg_loop.h"
#include "src/util/log_router.h"
#include "src/controltower/options.h"
#include "src/controltower/room.h"
#include "src/controltower/tailer.h"

namespace rocketspeed {

class ControlTower {
 public:
  // A new instance of a Control Tower
  static Status CreateNewInstance(const ControlTowerOptions& options,
                                  ControlTower** ct);
  virtual ~ControlTower();

  // Start this instance of the Control Tower
  void Run(void);

  // Is the ControlTower up and running?
  bool IsRunning() { return msg_loop_.IsRunning(); }

  // Returns the sanitized options used by the control tower
  ControlTowerOptions& GetOptions() {return options_;}

  // Sends a command to the msgloop
  Status SendCommand(std::unique_ptr<Command> command) {
    return msg_loop_.SendCommand(std::move(command));
  }

  // Returns the HostId to HostNumber mapping
  HostMap& GetHostMap() { return hostmap_; }

  // Returns the logic to map a topic name to a logid
  const LogRouter& GetLogRouter() { return log_router_; }

  // The Storage Reader
  const Tailer* GetTailer() const { return tailer_.get(); }

  // Get HostID
  const HostId& GetHostId() const { return msg_loop_.GetHostId(); }

 private:
  // The options used by the Control Tower
  ControlTowerOptions options_;

  // Message specific callbacks stored here
  const std::map<MessageType, MsgCallbackType> callbacks_;

  // Maps a topic to a log
  const LogRouter log_router_;

  // Maps a HostId to a HostNumber.
  HostMap hostmap_;

  // A control tower has multiple ControlRooms.
  // Each Room handles its own set of topics. Each room has its own
  // room number. Each room also has its own MsgLoop.
  std::vector<unique_ptr<ControlRoom>> rooms_;

  // The Tailer to feed in data from LogStorage to Rooms
  unique_ptr<Tailer> tailer_;

  // The message loop base.
  // This is used to receive subscribe/unsubscribe/data messages
  // from other processes. The MsgLoop should be the last field of
  // ControlTower so that it gets destructed first.
  MsgLoop msg_loop_;

  // private Constructor
  explicit ControlTower(const ControlTowerOptions& options);

  // Sanitize input options if necessary
  ControlTowerOptions SanitizeOptions(const ControlTowerOptions& src);

  // callbacks to process incoming messages
  static void ProcessMetadata(ApplicationCallbackContext ctx,
                              std::unique_ptr<Message> msg);
  static std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
