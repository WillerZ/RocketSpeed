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
#include "src/controltower/room.h"
#include "src/controltower/tailer.h"

namespace rocketspeed {

class ControlTower {
 public:
  static const int DEFAULT_PORT = 58500;

  // A new instance of a Control Tower
  static Status CreateNewInstance(const ControlTowerOptions& options,
                                  ControlTower** ct);
  virtual ~ControlTower();

  // Returns the sanitized options used by the control tower
  ControlTowerOptions& GetOptions() {return options_;}

  // Sends a command to the msgloop
  Status SendCommand(std::unique_ptr<Command> command, int worker_id) {
    return options_.msg_loop->SendCommand(std::move(command), worker_id);
  }

  // Returns the HostId to HostNumber mapping
  HostMap& GetHostMap() { return hostmap_; }

  // The Storage Reader
  const Tailer* GetTailer() const { return tailer_.get(); }

  // Get HostID
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  // Get the client id of a worker thread on this control tower
  const ClientID& GetClientId(int worker_id) const {
    return options_.msg_loop->GetClientId(worker_id);
  }

  // Find a host number and worker ID by client ID
  HostNumber LookupHost(const ClientID& client_id, int* out_worker_id) const;

  // Find a client ID and worker ID by host number
  const ClientID* LookupHost(HostNumber hostnum, int* out_worker_id) const;

  // Add a new client ID and worker ID then return its host number
  HostNumber InsertHost(const ClientID& client_id, int worker_id);

  MsgLoop* GetMsgLoop() {
    return options_.msg_loop;
  }

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
  std::vector<unique_ptr<ControlRoom>> rooms_;

  // The Room worker threads
  std::vector<BaseEnv::ThreadId> room_thread_id_;

  // The Tailer to feed in data from LogStorage to Rooms
  unique_ptr<Tailer> tailer_;

  // The id of this control tower
  const ClientID tower_id_;

  // private Constructor
  explicit ControlTower(const ControlTowerOptions& options);

  // Sanitize input options if necessary
  ControlTowerOptions SanitizeOptions(const ControlTowerOptions& src);

  // callbacks to process incoming messages
  void ProcessMetadata(std::unique_ptr<Message> msg);
  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
