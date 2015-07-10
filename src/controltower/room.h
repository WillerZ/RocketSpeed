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
#include "src/controltower/options.h"

namespace rocketspeed {

class CommandQueue;
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

  // The Room Number [0...n)
  unsigned int GetRoomNumber() const { return room_number_; }

  // Forwards a message to this Room
  std::unique_ptr<Command> MsgCommand(std::unique_ptr<Message> msg,
                                      int worker_id,
                                      StreamID origin);

  // Processes a message from the tailer.
  void OnTailerMessage(std::unique_ptr<Message> msg,
                       std::vector<HostNumber> hosts);

 private:
  // I am part of this control tower
  ControlTower* control_tower_;

  // My room number
  unsigned int room_number_;

  // Per-topic tailer
  TopicTailer* topic_tailer_;

  // Map of host numbers to worker loop IDs.
  std::unordered_map<HostNumber, int> hostnum_to_worker_id_;

  // Queues for communicating back to client threads.
  std::vector<std::shared_ptr<CommandQueue>> room_to_client_queues_;

  // callbacks to process incoming messages
  void ProcessMetadata(std::unique_ptr<Message> msg,
                       int worker_id,
                       StreamID origin);
  void ProcessDeliver(std::unique_ptr<Message> msg,
                      const std::vector<HostNumber>& hosts);
  void ProcessGap(std::unique_ptr<Message> msg,
                  const std::vector<HostNumber>& hosts);
  void ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin);
};

}  // namespace rocketspeed
