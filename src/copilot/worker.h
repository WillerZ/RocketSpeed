// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <map>
#include <memory>
#include <unordered_map>
#include <vector>
#include "include/Types.h"
#include "src/copilot/options.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/control_tower_router.h"
#include "src/util/worker_loop.h"

namespace rocketspeed {

class Copilot;

// These are sent from the Copilot to the worker.
class CopilotWorkerCommand {
 public:
  CopilotWorkerCommand() = default;

  CopilotWorkerCommand(LogID logid, std::unique_ptr<Message> msg, int worker_id)
  : logid_(logid)
  , msg_(std::move(msg))
  , worker_id_(worker_id) {
  }

  // Get log ID where topic lives to subscribe to
  LogID GetLogID() const {
    return logid_;
  }

  // Get the message.
  std::unique_ptr<Message> GetMessage() {
    return std::move(msg_);
  }

  int GetWorkerId() const {
    return worker_id_;
  }

 private:
  LogID logid_;
  std::unique_ptr<Message> msg_;
  int worker_id_;
};

// These Commands sent from the Worker to the Copilot
class CopilotCommand : public SendCommand {
 public:
  CopilotCommand() = default;

  CopilotCommand(std::string message,
                 const ClientID& host,
                 uint64_t issued_time):
    SendCommand(issued_time),
    message_(std::move(message)) {
    recipient_.push_back(host);
    assert(message_.size() > 0);
  }
  CopilotCommand(std::string message,
                 Recipients hosts,
                 uint64_t issued_time):
    SendCommand(issued_time),
    recipient_(std::move(hosts)),
    message_(std::move(message)) {
    assert(message_.size() > 0);
  }
  void GetMessage(std::string* out) {
    out->assign(std::move(message_));
  }
  // return the Destination ClientID, otherwise returns null.
  const Recipients& GetDestination() const {
    return recipient_;
  }

 private:
  Recipients recipient_;
  std::string message_;
};

/**
 * Copilot worker. The copilot will allocate several of these, ideally one
 * per hardware thread. The workers take load off of the main thread.
 */
class CopilotWorker {
 public:
  // Constructs a new CopilotWorker (does not start a thread).
  CopilotWorker(const CopilotOptions& options,
                const ControlTowerRouter* control_tower_router,
                Copilot* copilot);

  // Forward a message to this worker for processing.
  bool Forward(LogID logid, std::unique_ptr<Message> msg, int worker_id);

  // Start the worker loop on this thread.
  // Blocks until the worker loop ends.
  void Run();

  // Stop the worker loop.
  void Stop() {
    worker_loop_.Stop();
  }

  // Check if the worker loop is running.
  bool IsRunning() const {
    return worker_loop_.IsRunning();
  }

  // Get the host id of this worker's worker loop.
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

 private:
  // Callback for worker loop commands.
  void CommandCallback(CopilotWorkerCommand command);

  // Send an ack message to the host for the msgid.
  void SendAck(const ClientID& host,
               const MsgId& msgid,
               MessageDataAck::AckStatus status);

  // Add a subscriber to a topic.
  void ProcessSubscribe(std::unique_ptr<Message> msg,
                        const TopicPair& request,
                        LogID logid,
                        int worker_id);

  // Remove a subscriber from a topic.
  void ProcessUnsubscribe(std::unique_ptr<Message> msg,
                          const TopicPair& request,
                          LogID logid,
                          int worker_id);

  // Process a metadata response from control tower.
  void ProcessMetadataResponse(std::unique_ptr<Message> msg,
                               const TopicPair& request);

  // Forward data to subscribers.
  void ProcessDeliver(std::unique_ptr<Message> msg);

  // Duplicates and distributes a command to different copilot event loops.
  void DistributeCommand(std::string msg,
                         std::vector<std::pair<ClientID, int>> destinations);

  // Main worker loop for this worker.
  WorkerLoop<CopilotWorkerCommand> worker_loop_;

  // Copilot specific options.
  const CopilotOptions& options_;

  // Shared router for control towers.
  const ControlTowerRouter* control_tower_router_;

  // Reference to the copilot
  Copilot* copilot_;

  // Subscription metadata
  struct Subscription {
    Subscription(ClientID const& id,
                 SequenceNumber seq_no,
                 bool await_ack,
                 int _worker_id)
    : client_id(id)
    , seqno(seq_no)
    , awaiting_ack(await_ack)
    , worker_id(_worker_id) {}

    ClientID client_id;    // The subscriber
    SequenceNumber seqno;  // Lowest seqno to accept
    bool awaiting_ack;     // Is the subscriber awaiting an subscribe response?
    int worker_id;         // The event loop worker for client.
  };

  // Map of topics to active subscriptions.
  std::unordered_map<Topic, std::vector<Subscription>> subscriptions_;
};

}  // namespace rocketspeed
