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
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/storage.h"

namespace rocketspeed {

class Statistics;

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
  const ClientID& GetClientId(int worker_id = 0) const {
    return options_.msg_loop->GetClientId(worker_id);
  }

  // Sends a command to a msgloop worker.
  Status SendCommand(std::unique_ptr<Command> command, int worker_id) {
    return options_.msg_loop->SendCommand(std::move(command), worker_id);
  }

  MsgLoop* GetMsgLoop() {
    return options_.msg_loop;
  }

  // Returns an aggregated statistics object from the copilot
  Statistics GetStatisticsSync() const;

  // Gets information about the running service.
  std::string GetInfoSync(std::vector<std::string> args);

  /**
   * Updates the control tower routing information.
   *
   * @param nodes Map of NodeIds to control tower hosts. The node IDs
   *              determine what logs are routed to each host. This is a
   *              a completely new set of nodes, not a delta.
   * @return ok() if successfully propagated to all workers, error otherwise.
   */
  Status UpdateControlTowers(std::unordered_map<uint64_t, HostId> nodes);

  // Get the worker loop associated with a log.
  int GetLogWorker(LogID log_id) const;

  // EventLoop worker responsible for a control tower.
  int GetTowerWorker(LogID log_id, const HostId& tower) const;

 private:

  // The options used by the Copilot
  CopilotOptions options_;

  // Worker objects and threads, these have their own message loops.
  std::vector<std::unique_ptr<CopilotWorker>> workers_;

  // For each thread, maps (StreamID, SubscriptionID) pairs to copilot workers.
  using SubscriptionIDToWorkerMap =
    std::unordered_map<StreamID, std::unordered_map<SubscriptionID, int>>;
  std::vector<SubscriptionIDToWorkerMap> sub_id_map_;

  // Full-mesh network of queues for the messages from clients to workers.
  std::vector<std::vector<std::shared_ptr<CommandQueue>>>
    client_to_worker_queues_;

  // Full-mesh network on queues for responses from control towers to workers.
  std::vector<std::vector<std::shared_ptr<CommandQueue>>>
    tower_to_worker_queues_;

  // Thread-local queue mesh for updating workers with new tower routing info.
  std::vector<std::unique_ptr<ThreadLocalCommandQueues>> router_update_queues_;

  // private Constructor
  Copilot(CopilotOptions options, std::unique_ptr<ClientImpl> client);

  // Sanitize input options if necessary
  CopilotOptions SanitizeOptions(CopilotOptions options);

  // callbacks to process incoming messages
  void ProcessDeliver(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessMetadata(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessGap(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessSubscribe(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessUnsubscribe(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin);
  void ProcessTimerTick();

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
