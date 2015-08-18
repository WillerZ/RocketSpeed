// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "include/Status.h"
#include "include/Types.h"
#include "include/RocketSpeed.h"
#include "include/SubscriptionStorage.h"
#include "src/messages/messages.h"
#include "src/messages/stream_socket.h"
#include "src/port/port.h"
#include "src/util/common/random.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

class ClientOptions;
class MessageDeliver;
class MessageUnsubscribe;
class MessageGoodbye;
class MsgLoopBase;
class SubscriptionState;

typedef uint64_t SubscriptionID;

/** Represents a state of a single subscription. */
class SubscriptionState {
 public:
  // Noncopyable
  SubscriptionState(const SubscriptionState&) = delete;
  SubscriptionState& operator=(const SubscriptionState&) = delete;
  // Movable
  SubscriptionState(SubscriptionState&&) = default;
  SubscriptionState& operator=(SubscriptionState&&) = default;

  SubscriptionState(SubscriptionParameters parameters,
                    SubscribeCallback subscription_callback,
                    MessageReceivedCallback deliver_callback,
                    DataLossCallback data_loss_callback)
  : tenant_id_(parameters.tenant_id)
  , namespace_id_(std::move(parameters.namespace_id))
  , topic_name_(std::move(parameters.topic_name))
  , subscription_callback_(std::move(subscription_callback))
  , deliver_callback_(std::move(deliver_callback))
  , data_loss_callback_(std::move(data_loss_callback))
  , expected_seqno_(parameters.start_seqno)
  // If we were to restore state from subscription storage before the
  // subscription advances, we would restore from the next sequence number,
  // that is why we persist the previous one.
  , last_acked_seqno_(parameters.start_seqno == 0 ? 0 : parameters.start_seqno -
                                                            1) {}

  TenantID GetTenant() const { return tenant_id_; }

  const NamespaceID& GetNamespace() const { return namespace_id_; }

  const Topic& GetTopicName() const { return topic_name_; }

  /** Terminates subscription and notifies the application. */
  void Terminate(const std::shared_ptr<Logger>& info_log,
                 SubscriptionID sub_id,
                 MessageUnsubscribe::Reason reason);

  /** Processes gap or data message. */
  void ReceiveMessage(const std::shared_ptr<Logger>& info_log,
                      std::unique_ptr<MessageDeliver> deliver);

  /** Returns a lower bound on the seqno of the next expected message. */
  SequenceNumber GetExpected() const {
    thread_check_.Check();
    return expected_seqno_;
  }

  /** Marks provided sequence number as acknowledged. */
  void Acknowledge(SequenceNumber seqno);

  /** Returns sequnce number of last acknowledged message. */
  SequenceNumber GetLastAcknowledged() const {
    thread_check_.Check();
    return last_acked_seqno_;
  }

 private:
  ThreadCheck thread_check_;

  const TenantID tenant_id_;
  const NamespaceID namespace_id_;
  const Topic topic_name_;
  const SubscribeCallback subscription_callback_;
  const MessageReceivedCallback deliver_callback_;
  const DataLossCallback data_loss_callback_;

  /** Next expected sequence number on this subscription. */
  SequenceNumber expected_seqno_;
  /** Seqence number of the last acknowledged message. */
  SequenceNumber last_acked_seqno_;

  /** Returns true iff message arrived in order and not duplicated. */
  bool ProcessMessage(const std::shared_ptr<Logger>& info_log,
                      const MessageDeliver& deliver);

  /** Announces status of a subscription via user defined callback. */
  void AnnounceStatus(bool subscribed, Status status);
};

/** State of a single subscriber worker, aligned to avoid false sharing. */
class alignas(CACHE_LINE_SIZE) Subscriber {
 public:
  // Noncopyable
  Subscriber(const Subscriber&) = delete;
  Subscriber& operator=(const Subscriber&) = delete;
  // Nonmovable
  Subscriber(Subscriber&&) = delete;
  Subscriber& operator=(Subscriber&&) = delete;

  Subscriber(const ClientOptions& options, MsgLoopBase* msg_loop);

  ~Subscriber();

  Status Start();

  const Statistics& GetStatistics();

  /** Handles creation of a subscription on provided worker thread. */
  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         MessageReceivedCallback deliver_callback,
                         SubscribeCallback subscription_callback,
                         DataLossCallback data_loss_callback);

  /** Marks message of given seqno on given subscription as acknowledged. */
  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno);

  /** Handles termination of a subscription on provided worker thread. */
  void TerminateSubscription(SubscriptionID sub_id);

  /** Saves state of the subscriber using provided storage strategy. */
  Status SaveState(SubscriptionStorage::Snapshot* snapshot, size_t worker_id);

 private:
  friend class ClientImpl;

  /** Options, whose lifetime must be managed by the owning client. */
  const ClientOptions& options_;
  /** A message loop object owned by the client. */
  MsgLoopBase* const msg_loop_;
  ThreadCheck thread_check_;

  /** Time point (in us) until which client should not attemt to reconnect. */
  uint64_t backoff_until_time_;
  /** Time point (in us) of last message sending event. */
  uint64_t last_send_time_;
  /** Number of consecutive goodbye messages. */
  size_t consecutive_goodbyes_count_;
  /** Random engine used by this client. */
  std::mt19937_64& rng_;
  /** Stream socket used by this worker to talk to the copilot. */
  StreamSocket copilot_socket;
  /** Determines whether copilot socket is valid. */
  bool copilot_socket_valid_;
  /** Version of configuration when we last fetched hosts. */
  uint64_t last_config_version_;
  /** All subscriptions served by this worker. */
  std::unordered_map<SubscriptionID, SubscriptionState> subscriptions_;
  /**
   * A timeout list with recently sent unsubscribe requests, used to dedup
   * unsubscribes if we receive a burst of messages on terminated subscription.
   */
  TimeoutList<SubscriptionID> recent_terminations_;
  /** A set of subscriptions pending subscribe message being sent out. */
  std::unordered_set<SubscriptionID> pending_subscribes_;
  /** A set of subscriptions pending unsubscribe message being sent out. */
  std::unordered_map<SubscriptionID, TenantID> pending_terminations_;

  struct Stats {
    Stats() {
      const std::string prefix = "client.";

      active_subscriptions = all.AddCounter(prefix + "active_subscriptions");
      unsubscribes_invalid_handle =
          all.AddCounter(prefix + "unsubscribes_invalid_handle");
    }

    Counter* active_subscriptions;
    Counter* unsubscribes_invalid_handle;
    Statistics all;
  } stats_;

  bool ExpectsMessage(const std::shared_ptr<Logger>& info_log, StreamID origin);

  /**
   * Synchronises a portion of pending subscribe and unsubscribe requests with
   * the Copilot. Takes into an account rate limits.
   */
  void SendPendingRequests();

  /** Handler for data and gap messages */
  void Receive(std::unique_ptr<MessageDeliver> msg, StreamID origin);

  /** Handler for unsubscribe messages. */
  void Receive(std::unique_ptr<MessageUnsubscribe> msg, StreamID origin);

  /** Handler for goodbye messages. */
  void Receive(std::unique_ptr<MessageGoodbye> msg, StreamID origin);
};

}  // namespace rocketspeed
