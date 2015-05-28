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
#include "src/util/common/hash.h"
#include "src/util/topic_uuid.h"

namespace rocketspeed {

class Copilot;
class ControlTowerRouter;

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

  // Get the host id of this worker's worker loop.
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  const Statistics& GetStatistics();

  /**
   * Returns human-readable info on the towers serving a particular log.
   */
  std::string GetTowersForLog(LogID log_id) const;

 private:
  struct Subscription;
  struct TopicState;

  struct Stats {
    Stats() {
      rollcall_writes_total =
        all.AddCounter("copilot.numwrites_rollcall_total");
      rollcall_writes_failed =
        all.AddCounter("copilot.numwrites_rollcall_failed");
      incoming_subscriptions =
        all.AddCounter("copilot.incoming_subscriptions");
      subscribed_topics =
        all.AddCounter("copilot.subscribed_topics");
      data_on_unsubscribed_topic =
        all.AddCounter("copilot.data_on_unsubscribed_topic");
      gap_on_unsubscribed_topic =
        all.AddCounter("copilot.gap_on_unsubscribed_topic");
      out_of_order_seqno_from_tower =
        all.AddCounter("copilot.out_of_order_seqno_from_tower");
      control_tower_socket_creations =
        all.AddCounter("copilot.control_tower_socket_creations");
      control_tower_sockets =
        all.AddCounter("copilot.control_tower_sockets");
      data_dropped_out_of_order =
        all.AddCounter("copilot.data_dropped_out_of_order");
      gap_dropped_out_of_order =
        all.AddCounter("copilot.gap_dropped_out_of_order");
      message_from_unexpected_tower =
        all.AddCounter("copilot.message_from_unexpected_tower");
    }

    Statistics all;

    Counter* rollcall_writes_total;
    Counter* rollcall_writes_failed;
    Counter* incoming_subscriptions;
    Counter* subscribed_topics;
    Counter* data_on_unsubscribed_topic;
    Counter* gap_on_unsubscribed_topic;
    Counter* out_of_order_seqno_from_tower;
    Counter* control_tower_socket_creations;
    Counter* control_tower_sockets;
    Counter* data_dropped_out_of_order;
    Counter* gap_dropped_out_of_order;
    Counter* message_from_unexpected_tower;
  } stats_;

  // Send an ack message to the host for the msgid.
  void SendAck(const ClientID& host,
               const MsgId& msgid,
               MessageDataAck::AckStatus status);

  // Add a subscriber to a topic.
  void ProcessSubscribe(TenantID tenant_id,
                        const NamespaceID& namespace_id,
                        const Topic& topic_name,
                        SequenceNumber start_seqno,
                        SubscriptionID sub_id,
                        LogID logid,
                        int worker_id,
                        StreamID subscriber);

  // Remove a subscriber from a topic.
  void ProcessUnsubscribe(TenantID tenant_id,
                          SubscriptionID sub_id,
                          MessageUnsubscribe::Reason reason,
                          int worker_id,
                          StreamID subscriber);

  // Process a metadata response from control tower.
  void ProcessMetadataResponse(const TopicPair& request,
                               LogID logid,
                               int worker_id);

  // Forward data to subscribers.
  void ProcessDeliver(std::unique_ptr<Message> msg,
                      StreamID origin);

  // Forward gap to subscribers.
  void ProcessGap(std::unique_ptr<Message> msg,
                  StreamID origin);

  // Remove all subscriptions for a client.
  void ProcessGoodbye(std::unique_ptr<Message> msg,
                      StreamID origin);

  void ProcessRouterUpdate(std::shared_ptr<ControlTowerRouter> router);

  // Closes stream to a control tower, and updates all affected subscriptions.
  void CloseControlTowerStream(StreamID stream);

  // Sends a metadata request.
  // Returns true if successful.
  bool SendMetadata(TenantID tenant_id,
                    MetadataType type,
                    const TopicUUID& uuid,
                    SequenceNumber seqno,
                    StreamSocket* stream,
                    int worker_id);

  // Refreshes ustream subscriptions for a topic.
  void UpdateTowerSubscriptions(const TopicUUID& uuid, TopicState& topic);

  // Removes a single subscription.
  // May update subscription to control tower.
  // Does not send response to subscriber.
  void RemoveSubscription(TenantID tenant_id,
                          SubscriptionID sub_id,
                          StreamID subscriber,
                          int worker_id);

  // Write to Rollcall topic
  void RollcallWrite(const SubscriptionID sub_id,
                     const TenantID tenant_id,
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

  // Advance the sequence number state of tower subscriptions.
  void AdvanceTowers(TopicState* topic,
                     SequenceNumber prev,
                     SequenceNumber next,
                     StreamID origin);

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
                 TenantID _tenant_id,
                 SubscriptionID _sub_id)
    : stream_id(id)
    , seqno(seq_no)
    , worker_id(_worker_id)
    , tenant_id(_tenant_id)
    , sub_id(_sub_id) {}

    StreamID stream_id;           // The subscriber
    SequenceNumber seqno;         // Lowest seqno to accept
    int worker_id;                // The event loop worker for client.
    TenantID tenant_id;           // Tenant ID of the subscriber.
    const SubscriptionID sub_id;  // Stream-local ID of this subscription.
  };

  enum : size_t { kMaxTowerConnections = 2 };

  struct TopicState {
    explicit TopicState(LogID _log_id) : log_id(_log_id) {}

    struct Tower {
      explicit Tower(StreamSocket* _stream,
                     SequenceNumber _next_seqno,
                     int _worker_id)
      : stream(_stream)
      , next_seqno(_next_seqno)
      , worker_id(_worker_id) {}

      StreamSocket* stream;       // Tower connection stream socket.
      SequenceNumber next_seqno;  // Next expected seqno (i.e. where subscribed)
      int worker_id;              // Worker ID for Tower.
    };

    Tower* FindTower(StreamSocket* tower_stream) {
      for (Tower& tower : towers) {
        if (tower.stream == tower_stream) {
          return &tower;
        }
      }
      return nullptr;
    }

    using Towers = autovector<Tower, kMaxTowerConnections>;

    LogID log_id;
    std::vector<std::unique_ptr<Subscription>> subscriptions;
    Towers towers; // Tower subscriptions.
  };

  // State of subscriptions for a single topic.
  std::unordered_map<TopicUUID, TopicState> topics_;

  // Map of client to topics subscribed to.
  struct TopicInfo {
    Topic topic_name;
    NamespaceID namespace_id;
    LogID logid;
  };

  using ClientSubscriptions =
      std::unordered_map<SubscriptionID, TopicInfo, MurmurHash2<size_t>>;
  std::unordered_map<StreamID, ClientSubscriptions> client_subscriptions_;

  /**
   * Keeps track of all opened stream sockets to control towers, the index in
   * this array corresponds to message loop worker id.
   */
  std::unordered_map<HostId, std::unordered_map<int, StreamSocket>>
      control_tower_sockets_;
};

}  // namespace rocketspeed
