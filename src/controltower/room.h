// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <unordered_map>
#include "src/port/Env.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/util/hostmap.h"
#include "src/util/worker_loop.h"
#include "src/controltower/options.h"

namespace rocketspeed {

class ControlTower;
class TopicTailer;

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
  Status Forward(std::unique_ptr<Message> msg, int worker_id, StreamID origin);

  // Processes a message from the tailer.
  Status OnTailerMessage(std::unique_ptr<Message> msg,
                         std::vector<HostNumber> hosts);

  // Stop the room worker loop
  void Stop() { room_loop_.Stop();}

  // The Commands sent to the ControlRoom.
  // The ControlTower sends subscribe/unsubscribe messages to ControlRoom.
  // The Tailer sends data messages to ControlRoom.
  class RoomCommand {
   public:
    RoomCommand() {}

    RoomCommand(std::unique_ptr<Message> message,
                int worker_id,
                StreamID origin):
      message_(std::move(message)),
      worker_id_(worker_id),
      origin_(origin) {
    }

    RoomCommand(std::unique_ptr<Message> message,
                std::vector<HostNumber> hosts)
    : message_(std::move(message))
    , hosts_(std::move(hosts)) {
    }

    std::unique_ptr<Message> GetMessage() {
      return std::move(message_);
    }

    int GetWorkerId() const {
      return worker_id_;
    }

    StreamID GetOrigin() const {
      return origin_;
    }

    const std::vector<HostNumber>& GetHosts() const {
      return hosts_;
    }

   private:
    std::unique_ptr<Message> message_;
    int worker_id_;
    StreamID origin_;
    std::vector<HostNumber> hosts_;
  };

 private:
  // I am part of this control tower
  ControlTower* control_tower_;

  // My room number
  unsigned int room_number_;

  // Per-topic tailer
  TopicTailer* topic_tailer_;

  // The message loop base.
  // This is used to receive subscribe/unsubscribe/data messages
  // from the ControlTower.
  WorkerLoop<RoomCommand> room_loop_;

  // Map of host numbers to worker loop IDs.
  std::unordered_map<HostNumber, int> hostnum_to_worker_id_;

  // callbacks to process incoming messages
  void ProcessMetadata(std::unique_ptr<Message> msg,
                       int worker_id,
                       StreamID origin);
  void ProcessDeliver(std::unique_ptr<Message> msg,
                      const std::vector<HostNumber>& hosts);
  void ProcessGap(std::unique_ptr<Message> msg,
                  const std::vector<HostNumber>& hosts);
};

}  // namespace rocketspeed
