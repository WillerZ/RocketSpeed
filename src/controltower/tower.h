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
#include "src/util/hostmap.h"

namespace rocketspeed {

class ControlRoom;
class LogTailer;
class TopicTailer;
class Statistics;

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

  // Sends a command to the msgloop
  Status SendCommand(std::unique_ptr<Command> command, int worker_id) {
    return options_.msg_loop->SendCommand(std::move(command), worker_id);
  }

  // Returns the HostId to HostNumber mapping
  HostMap& GetHostMap() { return hostmap_; }

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

  // Get the client id of a worker thread on this control tower
  const ClientID& GetClientId(int worker_id) const {
    return options_.msg_loop->GetClientId(worker_id);
  }

  // Find a host number and worker ID by client ID
  HostNumber LookupHost(StreamID origin, int* out_worker_id) const;

  // Find a client ID and worker ID by host number
  StreamID LookupHost(HostNumber hostnum, int* out_worker_id) const;

  // Add a new client ID and worker ID then return its host number
  HostNumber InsertHost(StreamID origin, int worker_id);

  MsgLoop* GetMsgLoop() {
    return options_.msg_loop;
  }

  Statistics GetStatisticsSync();

  // Gets information about the running service.
  std::string GetInfoSync(std::vector<std::string> args);

 private:
  // The options used by the Control Tower
  ControlTowerOptions options_;

  // Maps a HostId to a HostNumber.
  HostMap hostmap_;

  // Maps a HostNumber to a worker_id
  std::unique_ptr<std::atomic<int>[]> hostworker_;

  // A control tower has multiple ControlRooms.
  // Each Room handles its own set of topics. Each room has its own
  // room number. Each room also has its own MsgLoop.
  std::vector<std::unique_ptr<ControlRoom>> rooms_;

  // The Tailer to feed in data from LogStorage to Rooms
  std::unique_ptr<LogTailer> log_tailer_;
  std::vector<std::unique_ptr<TopicTailer>> topic_tailer_;

  // The id of this control tower
  const ClientID tower_id_;

  // private Constructor
  explicit ControlTower(const ControlTowerOptions& options);

  // Sanitize input options if necessary
  ControlTowerOptions SanitizeOptions(const ControlTowerOptions& src);

  // callbacks to process incoming messages
  void ProcessMetadata(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessFindTailSeqno(std::unique_ptr<Message> msg, StreamID origin);
  std::map<MessageType, MsgCallbackType> InitializeCallbacks();

  Status Initialize();

  int LogIDToRoom(LogID log_id) const;
};

}  // namespace rocketspeed
