// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <atomic>
#include <memory>
#include <map>
#include <vector>
#include "src/messages/msg_loop.h"
#include "src/controltower/options.h"
#include "src/util/subscription_map.h"

namespace rocketspeed {

class ControlRoom;
class LogTailer;
class TopicTailer;
class Statistics;

template <typename> class ThreadLocalQueues;

struct CopilotSub {
  CopilotSub()
  : stream_id(0)
  , sub_id(0) {
  }

  CopilotSub(StreamID _stream_id, SubscriptionID _sub_id)
  : stream_id(_stream_id)
  , sub_id(_sub_id) {
  }

  bool operator==(const CopilotSub& rhs) const {
    return rhs.stream_id == stream_id && rhs.sub_id == sub_id;
  }

  std::string ToString() const;

  StreamID stream_id;
  SubscriptionID sub_id;
};

class ControlTower {
 public:
  static const int DEFAULT_PORT = 58500;

  // A new instance of a Control Tower
  static Status CreateNewInstance(const ControlTowerOptions& options,
                                  ControlTower** ct);
  virtual ~ControlTower();

  /**
   * Synchronously stop the tower from sending further communication.
   */
  void Stop();

  // Returns the sanitized options used by the control tower
  ControlTowerOptions& GetOptions() {return options_;}

  // The Storage Reader
  LogTailer* GetLogTailer() {
    return log_tailer_.get();
  }

  TopicTailer* GetTopicTailer(int room_number) {
    assert(room_number < static_cast<int>(topic_tailer_.size()));
    return topic_tailer_[room_number].get();
  }

  // Get HostID
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  MsgLoop* GetMsgLoop() {
    return options_.msg_loop;
  }

  Statistics GetStatisticsSync();

  // Gets information about the running service.
  std::string GetInfoSync(std::vector<std::string> args);

  // Sets information about the running service
  std::string SetInfoSync(std::vector<std::string> args);

 private:
  // The options used by the Control Tower
  ControlTowerOptions options_;

  // A control tower has multiple ControlRooms.
  // Each Room handles its own set of topics. Each room has its own
  // room number. Each room also has its own MsgLoop.
  std::vector<std::unique_ptr<ControlRoom>> rooms_;

  // The Tailer to feed in data from LogStorage to Rooms
  std::unique_ptr<LogTailer> log_tailer_;
  std::vector<std::unique_ptr<TopicTailer>> topic_tailer_;

  // Queues for communicating from Tower processor to Rooms.
  std::vector<std::vector<std::shared_ptr<CommandQueue>>>
    tower_to_room_queues_;

  // Queues used to communicate from FindLatestSeqno response thread back to
  // client that issued the request.
  std::vector<std::unique_ptr<ThreadLocalQueues<std::unique_ptr<Command>>>>
    find_latest_seqno_response_queues_;

  std::vector<SubscriptionMap<int>> sub_to_room_;

  // private Constructor
  explicit ControlTower(const ControlTowerOptions& options);

  // Sanitize input options if necessary
  ControlTowerOptions SanitizeOptions(const ControlTowerOptions& src);

  // callbacks to process incoming messages
  void ProcessSubscribe(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessUnsubscribe(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessFindTailSeqno(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin);
  std::map<MessageType, MsgCallbackType> InitializeCallbacks();

  Status Initialize();

  int LogIDToRoom(LogID log_id) const;
};

}  // namespace rocketspeed
