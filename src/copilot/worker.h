// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "include/Types.h"
#include "src/copilot/options.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream_socket.h"
#include "src/util/control_tower_router.h"
#include "src/util/worker_loop.h"
#include "src/util/common/hash.h"
#include "src/util/topic_uuid.h"

namespace rocketspeed {

class Copilot;

// These are sent from the Copilot to the worker.
class CopilotWorkerCommand {
 public:
  CopilotWorkerCommand() = default;

  CopilotWorkerCommand(LogID logid,
                       std::unique_ptr<Message> msg,
                       int worker_id,
                       StreamID origin)
  : logid_(logid)
  , msg_(std::move(msg))
  , worker_id_(worker_id)
  , origin_(origin) {
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

  StreamID GetOrigin() const {
    return origin_;
  }

 private:
  LogID logid_;
  std::unique_ptr<Message> msg_;
  int worker_id_;
  StreamID origin_;
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
                const int myid,
                Copilot* copilot);

  // Forward a message to this worker for processing.
  bool Forward(LogID logid,
               std::unique_ptr<Message> msg,
               int worker_id,
               StreamID origin);

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
                        int worker_id,
                        StreamID subscriber);

  // Remove a subscriber from a topic.
  void ProcessUnsubscribe(std::unique_ptr<Message> msg,
                          const TopicPair& request,
                          LogID logid,
                          int worker_id,
                          StreamID subscriber);

  // Process a metadata response from control tower.
  void ProcessMetadataResponse(std::unique_ptr<Message> msg,
                               const TopicPair& request,
                               LogID logid,
                               int worker_id);

  // Forward data to subscribers.
  void ProcessDeliver(std::unique_ptr<Message> msg);

  // Fprward gap to subscribers.
  void ProcessGap(std::unique_ptr<Message> msg);

  // Remove all subscriptions for a client.
  void ProcessGoodbye(std::unique_ptr<Message> msg,
                      StreamID origin);

  // Removes a single subscription.
  // May update subscription to control tower.
  // Does not send response to subscriber.
  void RemoveSubscription(TenantID tenant_id,
                          const StreamID subscriber,
                          const NamespaceID& namespace_id,
                          const Topic& topic_name,
                          LogID logid,
                          int worker_id);

  // Write to Rollcall topic
  void RollcallWrite(std::unique_ptr<Message> msg,
                     const Topic& topic_name,
                     const NamespaceID& namespace_id,
                     const MetadataType type,
                     const LogID logid,
                     int worker_id,
                     StreamID origin);

  /** Gets or (re)open socket to control tower. */
  StreamSocket* GetControlTowerSocket(const ClientID& tower,
                                      MsgLoop* msg_loop,
                                      int outgoing_worker_id);

  // Main worker loop for this worker.
  WorkerLoop<CopilotWorkerCommand> worker_loop_;

  // Copilot specific options.
  const CopilotOptions& options_;

  // Shared router for control towers.
  const ControlTowerRouter* control_tower_router_;

  // Reference to the copilot
  Copilot* copilot_;

  // My worker id
  int myid_;

  // Subscription metadata
  struct Subscription {
    Subscription(StreamID id,
                 SequenceNumber seq_no,
                 bool await_ack,
                 int _worker_id)
    : stream_id(id)
    , seqno(seq_no)
    , awaiting_ack(await_ack)
    , worker_id(_worker_id) {}

    StreamID stream_id;    // The subscriber
    SequenceNumber seqno;  // Lowest seqno to accept
    bool awaiting_ack;     // Is the subscriber awaiting an subscribe response?
    int worker_id;         // The event loop worker for client.
  };

  // Map of topics to active subscriptions.
  std::unordered_map<TopicUUID, std::vector<Subscription>> subscriptions_;

  // Map of client to topics subscribed to.
  struct TopicInfo {
    struct Hash {
      size_t operator()(const TopicInfo& t) const {
        // Don't need to include logid, because it is a function of topic_name.
        return MurmurHash2<Topic, NamespaceID>()(t.topic_name, t.namespace_id);
      }
    };

    bool operator==(const TopicInfo& rhs) const {
      // Don't need to include logid, because it is a function of topic_name.
      return topic_name == rhs.topic_name && namespace_id == rhs.namespace_id;
    }

    Topic topic_name;
    NamespaceID namespace_id;
    LogID logid;
  };

  typedef std::unordered_set<TopicInfo, TopicInfo::Hash> TopicInfoSet;
  std::unordered_map<StreamID, TopicInfoSet> client_topics_;

  /**
   * Keeps track of all opened stream sockets to control towers, the index in
   * this array corresponds to message loop worker id.
   */
  std::unordered_map<ClientID, std::unordered_map<int, StreamSocket>>
      control_tower_sockets_;
};

}  // namespace rocketspeed
