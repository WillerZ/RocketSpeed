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
#include <queue>

#include "include/Types.h"
#include "src/copilot/options.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/hash.h"
#include "src/util/common/linked_map.h"
#include "src/util/timeout_list.h"
#include "src/util/topic_uuid.h"

namespace rocketspeed {

class ClientImpl;
class Copilot;
class ControlTowerRouter;
class RollcallImpl;

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
                Copilot* copilot,
                std::shared_ptr<ClientImpl> client);

  ~CopilotWorker();

  // Creates a worker command for processing.
  std::unique_ptr<Command> WorkerCommand(LogID logid,
                                         std::unique_ptr<Message> msg,
                                         int worker_id,
                                         StreamID origin);

  std::unique_ptr<Command> WorkerCommand(
    std::shared_ptr<ControlTowerRouter> new_router);

  // Invoked on a regularly clock interval.
  void ProcessTimerTick();

  // Get the host id of this worker's worker loop.
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  Statistics GetStatistics();

  /**
   * Returns human-readable info on the towers serving a particular log.
   */
  std::string GetTowersForLog(LogID log_id) const;

  /**
   * Returns human-readable info about all subscriptions.
   *
   * @param filter Filter for topic names (uses strstr).
   * @param max Maximum number of subscriptions to return.
   */
  std::string GetSubscriptionInfo(std::string filter, int max) const;

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
      orphaned_topics =
        all.AddCounter("copilot.orphaned_topics");
      orphaned_resubscribes =
        all.AddCounter("copilot.orphaned_resubscribes");
      tower_rebalances_checked =
        all.AddCounter("copilot.tower_rebalances_checked");
      tower_rebalances_performed =
        all.AddCounter("copilot.tower_rebalances_performed");
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
    Counter* orphaned_topics;
    Counter* orphaned_resubscribes;
    Counter* tower_rebalances_checked;
    Counter* tower_rebalances_performed;
  } stats_;

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
                     const TopicUUID& topic,
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



  /**
   * Gets control towers for a log. Potentially cached for performance.
   *
   * @param log_id The log to lookup.
   * @param out Output vector for found hosts.
   * @return ok() if successful, otherwise error.
   */
  Status GetControlTowers(LogID log_id, std::vector<HostId const*>* out) const;

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
    uint32_t records_sent = 0;
    uint32_t gaps_sent = 0;
  };

  bool CorrectTopicTowers(TopicState& topic);

  SequenceNumber FindLowestSequenceNumber(
      const TopicState& topic, bool* have_zero_sub_res);

  void UnsubscribeControlTowers(const TopicUUID& topic_uuid, TopicState& topic);

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



  // Maximum number of resubscriptions per ProcessTimerTick.
  uint64_t resubscriptions_per_tick_;

  // Maximum number of rebalances per ProcessTimerTick;
  uint64_t rebalances_per_tick_;

  // Queue for each client worker.
  std::vector<std::shared_ptr<CommandQueue>> client_queues_;

  // Queue for each control tower worker.
  std::vector<std::shared_ptr<CommandQueue>> tower_queues_;

  // Queues for reporting back to worker from RollCall error callback.
  std::unique_ptr<ThreadLocalCommandQueues> rollcall_error_queues_;

  // A client to write rollcall topic
  std::unique_ptr<RollcallImpl> rollcall_;

  // A list of subscriptions to check topic health periodically.
  // For long-living subscriptions, we need to ensure that the subscription
  // is on the correct control tower.
  TimeoutList<TopicUUID> topic_checkup_list_;

  // Cache of control tower mapping per log.
  mutable std::unordered_map<LogID, std::vector<const HostId*>>
    control_tower_cache_;

  /***
   * Re-subscription data structures
   */

  struct ResubscribeRequest {
    ResubscribeRequest(
        TopicUUID _topic_uuid,
        SequenceNumber _sequence_number,
        bool _have_zero_sub,
        bool _cancelled)
    : topic_uuid(std::move(_topic_uuid))
    , sequence_number(_sequence_number)
    , have_zero_sub(_have_zero_sub)
    , cancelled(_cancelled) {}

    const TopicUUID topic_uuid;
    const SequenceNumber sequence_number;
    const bool have_zero_sub;
    bool cancelled; // quicker than removing item from middle of priority_queue
  };

  using SafeResubscribeRequest = std::unique_ptr<ResubscribeRequest>;

  // Comparator for priority_queue. Request with lowest seq number is top().
  // Zero (i.e. tail) is higher than any other seq number.
  struct GreaterResubscribeRequest {
    bool operator()(
    const SafeResubscribeRequest& x, const SafeResubscribeRequest& y) {
      return  (x->sequence_number == 0 && y->sequence_number != 0)
          || (y->sequence_number != 0
              && x->sequence_number > y->sequence_number);
    }
  };

  using ResubscribeRequestQueue
      = std::priority_queue<
        SafeResubscribeRequest,
        std::vector<SafeResubscribeRequest>,
        GreaterResubscribeRequest>;

  ResubscribeRequestQueue current_resubscribe_request_queue_;
  ResubscribeRequestQueue pending_resubscribe_request_queue_;

  std::unordered_map<TopicUUID,ResubscribeRequest*>
    active_resubscribe_requests_by_topic_;

  /***
   * Re-subscribe helper methods
   */

  void CleanRequestQueues();

  void CleanRequestQueue(ResubscribeRequestQueue& request_queue);

  bool HasActiveResubscribeRequests();

  bool HasActiveResubscribeRequest(const TopicUUID& topic_uuid);

  void CancelResubscribeRequest(const TopicUUID& topic_uuid);

  SafeResubscribeRequest PopNextResubscribeRequest();

  // Called when a re-subscribe request has failed and needs to be re-tried.
  // (At a later time, not immediately).
  void ReScheduleResubscribeRequest(
      const TopicUUID& topic_uuid,
      const TopicState& topic_state,
      const SequenceNumber new_seqno,
      const bool have_zero_sub);

  // Called when a topic needs to be re-subscribed.
  // Calculates the sequence number to re-subscribe at.
  void ScheduleResubscribeRequest(
      const TopicUUID& topic_uuid, const TopicState& topic_state);

  // Called when a topic needs to be re-subscribed.
  void ScheduleResubscribeRequest(
      const TopicUUID& topic_uuid,
      const TopicState& topic_state,
      const SequenceNumber new_seqno,
      const bool have_zero_sub,
      ResubscribeRequestQueue& resubscribe_request_queue);

  /***
   * Update stuff
   */

  // Refreshes ustream subscriptions for a topic.
  void UpdateTowerSubscriptions(const TopicUUID& uuid,
                                TopicState& topic,
                                bool force_resub = false);

  // Refreshes ustream subscriptions for a topic.
  void UpdateTowerSubscriptions(
      const TopicUUID& uuid,
      TopicState& topic,
      const SequenceNumber new_seqno,
      const bool have_zero_sub,
      bool force_resub = false);

};


}  // namespace rocketspeed
