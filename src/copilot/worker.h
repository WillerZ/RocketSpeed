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
#include "src/messages/messages.h"
#include "src/messages/msg_client.h"
#include "src/util/control_tower_router.h"
#include "src/util/worker_loop.h"

namespace rocketspeed {

class CopilotWorkerCommand {
 public:
  CopilotWorkerCommand() = default;

  CopilotWorkerCommand(LogID logid, std::unique_ptr<Message> msg)
  : logid_(logid)
  , msg_(std::move(msg)) {
  }

  // Get log ID where topic lives to subscribe to
  LogID GetLogID() const {
    return logid_;
  }

  // Get the message.
  Message* GetMessage() {
    return msg_.get();
  }

 private:
  LogID logid_;
  std::unique_ptr<Message> msg_;
};

/**
 * Copilot worker. The copilot will allocate several of these, ideally one
 * per hardware thread. The workers take load off of the main thread by handling
 * the log appends and ack sending, and allows us to scale to multiple cores.
 */
class CopilotWorker {
 public:
  // Constructs a new CopilotWorker (does not start a thread).
  CopilotWorker(const CopilotOptions& options,
                const ControlTowerRouter* control_tower_router,
                MsgClient* msg_client);

  // Forward a message to this worker for processing.
  // This will asynchronously append the message into the log storage,
  // and then send an ack back to the to the message origin.
  bool Forward(LogID logid, std::unique_ptr<Message> msg);

  // Start the worker loop on this thread.
  // Blocks until the worker loop ends.
  void Run() {
    worker_loop_.Run([this] (CopilotWorkerCommand command) {
      CommandCallback(std::move(command));
    });
  }

  // Stop the worker loop.
  void Stop() {
    worker_loop_.Stop();
  }

  // Check if the worker loop is running.
  bool IsRunning() const {
    return worker_loop_.IsRunning();
  }

  // Get the host id of this worker's worker loop.
  HostId GetHostId() const {
    return HostId(options_.copilotname, options_.port_number);
  }

 private:
  // Callback for worker loop commands.
  void CommandCallback(CopilotWorkerCommand command);

  // Send an ack message to the host for the msgid.
  void SendAck(const HostId& host,
               const MsgId& msgid,
               MessageDataAck::AckStatus status);

  // Add a subscriber to a topic.
  void ProcessSubscribe(MessageMetadata* msg,
                        const TopicPair& request,
                        LogID logid);

  // Remove a subscriber from a topic.
  void ProcessUnsubscribe(MessageMetadata* msg,
                          const TopicPair& request,
                          LogID logid);

  // Process a metadata response from control tower.
  void ProcessMetadataResponse(MessageMetadata* msg,
                               const TopicPair& request);

  // Forward data to subscribers.
  void ProcessData(MessageData* msg);

  // Main worker loop for this worker.
  WorkerLoop<CopilotWorkerCommand> worker_loop_;

  // Copilot specific options.
  const CopilotOptions& options_;

  // Shared router for control towers.
  const ControlTowerRouter* control_tower_router_;

  // MsgClient from CoPilot, this is shared by all workers.
  MsgClient* msg_client_;

  // Subscription metadata
  struct Subscription {
    Subscription(HostId const& host_id, SequenceNumber seqno, bool awaiting_ack)
    : host_id(host_id)
    , seqno(seqno)
    , awaiting_ack(awaiting_ack) {}

    HostId host_id;        // The subscriber
    SequenceNumber seqno;  // Lowest seqno to accept
    bool awaiting_ack;     // Is the subscriber awaiting an subscribe response?
  };

  // Map of topics to active subscriptions.
  std::unordered_map<Topic, std::vector<Subscription>> subscriptions_;
};

}  // namespace rocketspeed
