// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include "include/Env.h"
#include "src/messages/msg_loop.h"
#include "src/controltower/options.h"

namespace rocketspeed {

class ControlTower;

//
// A single instance of a ControlRoom.
// A ControlRoom processes a specific subset of all the topics
// managed by a ControlRoom.
// A ControlRoom is oblivious of the fact that there are other
// ControlRooms in the same ControlTower
//
class ControlRoom {
 public:
  ControlRoom(const ControlTowerOptions& options,
              ControlTower* control_tower,
              int room_number,
              int port_number);
  virtual ~ControlRoom();

  // Start this instance of the Control Room Msg Loop
  static void Run(void* arg);

  // Is the ControlRoom up and running?
  bool IsRunning() const { return room_loop_.IsRunning(); }

  // The Room Identifier
  const HostId& GetRoomId() const { return room_id_; }

  // Forwards a message to this Room
  Status Forward(Message* msg);

 private:
  // I am part of this control tower
  ControlTower* control_tower_;

  // My room number
  int room_number_;

  // The HostId of this msg loop
  HostId room_id_;

  // Message specific callbacks stored here
  const std::map<MessageType, MsgCallbackType> callbacks_;

  // The message loop base.
  // This is used to receive subscribe/unsubscribe/data messages
  // from the ControlTower.
  MsgLoop room_loop_;

  // callbacks to process incoming messages
  static void ProcessData(ApplicationCallbackContext ctx,
                          std::unique_ptr<Message> msg);
  static void ProcessMetadata(ApplicationCallbackContext ctx,
                              std::unique_ptr<Message> msg);
  static std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
