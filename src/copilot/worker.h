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
#include "src/util/worker_loop.h"
#include "src/util/common/hash.h"
#include "src/util/topic_uuid.h"

namespace rocketspeed {

class Copilot;
class ControlTowerRouter;

// These are sent from the Copilot to the worker.
class CopilotWorkerCommand {
 public:
  CopilotWorkerCommand() {}

  CopilotWorkerCommand(LogID logid,
                       std::unique_ptr<Message> msg,
                       int worker_id,
                       StreamID origin)
  : logid_(logid)
  , msg_(std::move(msg))
  , worker_id_(worker_id)
  , origin_(origin) {
  }

  explicit CopilotWorkerCommand(std::shared_ptr<ControlTowerRouter> new_router)
  : router_update_(std::move(new_router)) {
  }

  bool IsRouterUpdate() const {
    return !msg_;
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

  std::shared_ptr<ControlTowerRouter> GetRouterUpdate() {
    assert(router_update_);
    assert(!msg_);
    return router_update_;
  }

 private:
  LogID logid_;
  std::unique_ptr<Message> msg_;
  int worker_id_;
  StreamID origin_;
  std::shared_ptr<ControlTowerRouter> router_update_;
};

/**
 * Copilot worker. The copilot will allocate several of these, ideally one
 * per hardware thread. The workers take load off of the main thread.
 */
class CopilotWorker {
 public:
  // Constructs a new CopilotWorker (does not start a thread).
  CopilotWorker(const CopilotOptions& options,
                std::shared_ptr<ControlTowerRouter> control_tower_router,
                const int myid,
                Copilot* copilot);

  // Forward a message to this worker for processing.
  bool Forward(LogID logid,
               std::unique_ptr<Message> msg,
               int worker_id,
               StreamID origin);

  bool Forward(std::shared_ptr<ControlTowerRouter> new_router);

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
  struct Subscription;

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
  void ProcessMetadataResponse(const TopicPair& request,
                               LogID logid,
                               int worker_id);

  // Forward data to subscribers.
  void ProcessDeliver(std::unique_ptr<Message> msg);

  // Forward gap to subscribers.
  void ProcessGap(std::unique_ptr<Message> msg);

  // Remove all subscriptions for a client.
  void ProcessGoodbye(std::unique_ptr<Message> msg,
                      StreamID origin);

  void ProcessRouterUpdate(std::shared_ptr<ControlTowerRouter> router);

  // Closes stream to a control tower, and updates all affected subscriptions.
  void CloseControlTowerStream(StreamID stream);

  // Attempts to re-establish subscriptions on behalf of a client.
  void ResendSubscriptions(LogID log_id,
                           const TopicUUID& uuid,
                           Subscription* sub);

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
  void RollcallWrite(const TenantID tenant_id,
                     const Topic& topic_name,
                     const NamespaceID& namespace_id,
                     const MetadataType type,
                     const LogID logid,
                     int worker_id,
                     StreamID origin);

  /** Gets or (re)open socket to control tower. */
  StreamSocket* GetControlTowerSocket(const HostId& tower,
                                      MsgLoop* msg_loop,
                                      int outgoing_worker_id);

  // Main worker loop for this worker.
  WorkerLoop<CopilotWorkerCommand> worker_loop_;

  // Copilot specific options.
  const CopilotOptions& options_;

  // Router for control towers.
  std::shared_ptr<ControlTowerRouter> control_tower_router_;

  // Reference to the copilot
  Copilot* copilot_;

  // My worker id
  int myid_;

  // Subscription metadata per client.
  struct Subscription {
    Subscription(StreamID id,
                 SequenceNumber seq_no,
                 int _worker_id,
                 TenantID _tenant_id)
    : stream_id(id)
    , seqno(seq_no)
    , worker_id(_worker_id)
    , tenant_id(_tenant_id) {}

    struct Tower {
      explicit Tower(StreamID _stream_id)
      : stream_id(_stream_id) {}

      StreamID stream_id;
    };

    Tower* FindTower(StreamID tower_stream) {
      for (Tower& tower : towers) {
        if (tower.stream_id == tower_stream) {
          return &tower;
        }
      }
      return nullptr;
    }

    StreamID stream_id;           // The subscriber
    SequenceNumber seqno;         // Lowest seqno to accept
    int worker_id;                // The event loop worker for client.
    TenantID tenant_id;           // Tenant ID of the subscriber.
    autovector<Tower, 1> towers;  // Tower subscriptions.
  };

  struct TopicState {
    explicit TopicState(LogID _log_id) : log_id(_log_id) {}
    LogID log_id;
    std::vector<std::unique_ptr<Subscription>> subscriptions;
  };

  // State of subscriptions for a single topic.
  std::unordered_map<TopicUUID, TopicState> topics_;

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
  std::unordered_map<HostId, std::unordered_map<int, StreamSocket>>
      control_tower_sockets_;
};

}  // namespace rocketspeed
