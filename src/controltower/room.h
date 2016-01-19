// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <unordered_map>
#include "include/Env.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/controltower/options.h"
#include "src/controltower/tower.h"
#include "src/util/subscription_map.h"

namespace rocketspeed {

class CommandQueue;
class ControlTower;
class Flow;
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

  /**
   * Processes a message from the tailer.
   * @param flow Flow context from the source of the message.
   * @param msg The message from the tailer.
   * @param recipients Down-stream recipients of the message.
   */
  void OnTailerMessage(Flow* flow,
                       const Message& msg,
                       std::vector<CopilotSub> recipients);

  /** Find worker for CopilotSub (from sub_worker_) or -1 if not found. */
  int CopilotWorker(const CopilotSub& id);

 private:
  // I am part of this control tower
  ControlTower* control_tower_;

  // My room number
  unsigned int room_number_;

  // Per-topic tailer
  TopicTailer* topic_tailer_;

  // Queues for communicating back to client threads.
  std::vector<std::shared_ptr<CommandQueue>> room_to_client_queues_;

  SubscriptionMap<int> sub_worker_;

  // callbacks to process incoming messages
  void ProcessSubscribe(std::unique_ptr<Message> msg,
                        int worker_id,
                        StreamID origin);
  void ProcessUnsubscribe(std::unique_ptr<Message> msg,
                          int worker_id,
                          StreamID origin);
  void ProcessDeliver(Flow* flow,
                      const Message& msg,
                      const std::vector<CopilotSub>& recipients);
  void ProcessGap(Flow* flow,
                  const Message& msg,
                  const std::vector<CopilotSub>& recipients);
  void ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin);

};

}  // namespace rocketspeed
