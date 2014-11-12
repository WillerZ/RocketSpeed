// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include "src/port/Env.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/util/storage.h"
#include "src/util/worker_loop.h"
#include "src/controltower/options.h"
#include "src/controltower/topic.h"

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
              unsigned int room_number);
  virtual ~ControlRoom();

  // Start this instance of the Control Room Msg Loop
  static void Run(void* arg);

  // Is the ControlRoom up and running?
  bool IsRunning() const { return room_loop_.IsRunning(); }

  // The Room Number [0...n)
  unsigned int GetRoomNumber() const { return room_number_; }

  // Forwards a message to this Room
  Status Forward(std::unique_ptr<Message> msg, LogID logid);

  // The Commands sent to the ControlRoom.
  // The ControlTower sends subscribe/unsubscribe messages to ControlRoom.
  // The Tailer sends data messages to ControlRoom.
  class RoomCommand {
   public:
    RoomCommand() = default;

    RoomCommand(std::unique_ptr<Message> message, LogID logid):
      message_(std::move(message)),
      logid_(logid) {
    }

    std::unique_ptr<Message> GetMessage() {
      return std::move(message_);
    }

    const LogID GetLogId() const {
      return logid_;
    }

   private:
    std::unique_ptr<Message> message_;
    LogID logid_;
  };

  // These Commands sent from the ControlRoom to the ControlTower
  class TowerCommand : public Command {
   public:
    TowerCommand(std::string message,
                 const Recipients& recipient,
                 uint64_t issued_time):
      Command(issued_time),
      recipient_(recipient),
      message_(std::move(message)) {
    }
    TowerCommand(std::string message,
                 const ClientID& recipient,
                 uint64_t issued_time):
      Command(issued_time),
      message_(std::move(message)) {
      recipient_.push_back(recipient);
    }
    void GetMessage(std::string* out) {
      out->assign(std::move(message_));
    }
    // return the Destination HostId, otherwise returns null.
    const Recipients& GetDestination() const {
      return recipient_;
    }
    bool IsSendCommand() const  {
      return true;
    }
   private:
    Recipients recipient_;
    std::string message_;
  };

 private:
  // I am part of this control tower
  ControlTower* control_tower_;

  // My room number
  unsigned int room_number_;

  // Subscription information per topic
  TopicManager topic_map_;

  // The message loop base.
  // This is used to receive subscribe/unsubscribe/data messages
  // from the ControlTower.
  WorkerLoop<RoomCommand> room_loop_;

  // callbacks to process incoming messages
  void ProcessMetadata(std::unique_ptr<Message> msg, LogID logid);
  void ProcessDeliver(std::unique_ptr<Message> msg, LogID logid);
  void ProcessGap(std::unique_ptr<Message> msg, LogID logid);
};

}  // namespace rocketspeed
