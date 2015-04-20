// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <thread>
#include <vector>
#include "src/copilot/options.h"
#include "src/copilot/worker.h"
#include "src/messages/serializer.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/rollcall/rollcall_impl.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/storage.h"
#include "src/util/control_tower_router.h"

namespace rocketspeed {

class Stats;

class Copilot {
 public:
  static const int DEFAULT_PORT = 58600;

  // A new instance of a Copilot
  static Status CreateNewInstance(CopilotOptions options,
                                  Copilot** copilot);
  ~Copilot();

  void Stop();

  // Returns the sanitized options used by the copilot
  CopilotOptions& GetOptions() { return options_; }

  // Get HostID
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  // Get the client id of a worker thread on this copilot
  const ClientID& GetClientId(int worker_id) const {
    return options_.msg_loop->GetClientId(worker_id);
  }

  // Sends a command to a msgloop worker.
  Status SendCommand(std::unique_ptr<Command> command, int worker_id) {
    return options_.msg_loop->SendCommand(std::move(command), worker_id);
  }

  // Get the worker loop associated with a log.
  int GetLogWorker(LogID logid) const;

  const ControlTowerRouter& GetControlTowerRouter() const {
    return control_tower_router_;
  }

  MsgLoop* GetMsgLoop() {
    return options_.msg_loop;
  }

  // Get the client used for logging into rollcall topic
  RollcallImpl* GetRollcallLogger() {
    return rollcall_.get();
  }

  // Returns an aggregated statistics object from the copilot
  Statistics GetStatistics() const;

  // Get a reference to an individual worker's data
  Stats* GetStats(int id) {
    return &stats_[id];
  }

 private:

  // The options used by the Copilot
  CopilotOptions options_;

  // Worker objects and threads, these have their own message loops.
  std::vector<std::unique_ptr<CopilotWorker>> workers_;
  std::vector<BaseEnv::ThreadId> worker_threads_;

  // Control tower router. Workers will access this, but don't own it.
  ControlTowerRouter control_tower_router_;

  // A client to write rollcall topic
  std::unique_ptr<RollcallImpl> rollcall_;

  // Per-thread statistics data.
  std::vector<Stats> stats_;


  // private Constructor
  Copilot(CopilotOptions options, std::unique_ptr<ClientImpl> client);

  // Sanitize input options if necessary
  CopilotOptions SanitizeOptions(CopilotOptions options);

  // Start the background workers.
  void StartWorkers();

  // callbacks to process incoming messages
  void ProcessDeliver(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessMetadata(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessGap(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin);

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

// Statistics collected by copilot
class Stats {
 public:
  Stats() {
    numwrites_rollcall_total = all.AddCounter(
                             "rocketspeed.copilot.numwrites_rollcall_total");
    numwrites_rollcall_failed = all.AddCounter(
                             "rocketspeed.copilot.numwrites_rollcall_failed");
  }
  // all statistics about a copilot
  Statistics all;

  // Number of writes attempted to rollcall topic
  Counter* numwrites_rollcall_total;

  // Number of subscription writes to rollcall topic that failed and
  // resulted in an automatic forced unsubscription request.
  Counter* numwrites_rollcall_failed;
};

}  // namespace rocketspeed
