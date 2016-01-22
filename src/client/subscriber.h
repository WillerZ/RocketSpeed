// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "include/Status.h"
#include "include/Types.h"
#include "include/RocketSpeed.h"
#include "include/SubscriptionStorage.h"
#include "src/messages/messages.h"
#include "src/messages/types.h"
#include "src/port/port.h"
#include "include/HostId.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/random.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"

/**
 * This header defines a few Subscriber implementations. At the very high level,
 * each Subscriber serves the same purpose, it allows to receive data on
 * subscriptions.
 *
 * Each Subscriber exposes two important methods:
 * - Subscribe, which establishes a subscription with provided
 *   SubscriptionParameters; once the subscription is established, the
 *   application will be notified about new data messages, gaps and termination
 *   of the subscription via provided callbacks,
 * - Acknowledge, which marks provided message as acknowledged; if
 *   SubscriptionStorage is used, the Subscriber will resume subscripions from
 *   storage starting from next unacknowledged message,
 * - Unsubscribe, which termnates previously established subscription.
 *
 * Different subscribers are stacked one on another according to shared
 * functionality.
 */
namespace rocketspeed {

class ClientOptions;
class Command;
class Flow;
class MessageDeliver;
class MessageUnsubscribe;
class MessageGoodbye;
class MsgLoop;
class MultiShardSubscriber;
class EventLoop;
class Stream;
class SubscriptionState;
class SubscriberStats;
typedef uint64_t SubscriptionID;
typedef uint64_t SubscriptionHandle;
template <typename>
class ThreadLocalQueues;

/**
 * Represents a state of a single subscription.
 */
class SubscriptionState : public NonCopyable {
 public:
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
  void ReceiveMessage(Flow* flow,
                      const std::shared_ptr<Logger>& info_log,
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

/**
 * State of a subscriber per one shard.
 */
class Subscriber : public StreamReceiver {
 public:
  Subscriber(const ClientOptions& options,
             EventLoop* event_loop,
             std::shared_ptr<SubscriberStats> stats,
             std::unique_ptr<SubscriptionRouter> router);

  ~Subscriber();

  Status Start();

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         MessageReceivedCallback deliver_callback,
                         SubscribeCallback subscription_callback,
                         DataLossCallback data_loss_callback);

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno);

  void TerminateSubscription(SubscriptionID sub_id);

  /** Saves state of the subscriber using provided storage strategy. */
  Status SaveState(SubscriptionStorage::Snapshot* snapshot, size_t worker_id);

 private:
  friend class MultiShardSubscriber;

  /** Options, whose lifetime must be managed by the owning client. */
  const ClientOptions& options_;
  /** An event loop object this subscriber runs on. */
  EventLoop* const event_loop_;
  /** A shared statistics. */
  std::shared_ptr<SubscriberStats> stats_;
  ThreadCheck thread_check_;

  /** Time point (in us) until which client should not attempt to reconnect. */
  uint64_t backoff_until_time_;
  /** Time point (in us) of last message sending event. */
  uint64_t last_send_time_;
  /** Number of consecutive goodbye messages. */
  size_t consecutive_goodbyes_count_;
  /** Random engine used by this client. */
  std::mt19937_64& rng_;

  /** Stream socket used by this worker to talk to the Rocketeer. */
  std::unique_ptr<Stream> server_stream_;
  /** The current server host. */
  HostId server_host_;

  /** Version of the router when we last fetched hosts. */
  size_t last_router_version_;
  /** The router for this subscriber. */
  std::unique_ptr<SubscriptionRouter> router_;

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

  /**
   * Synchronises a portion of pending subscribe and unsubscribe requests with
   * the Copilot. Takes into an account rate limits.
   */
  void SendPendingRequests();

  void ReceiveDeliver(StreamReceiveArg<MessageDeliver> arg) final override;

  void ReceiveUnsubscribe(
      StreamReceiveArg<MessageUnsubscribe> arg) final override;

  void ReceiveGoodbye(StreamReceiveArg<MessageGoodbye> arg) final override;
};

/**
 * A single threaded thread-unsafe subscriber, that lazily brings up subscribers
 * per shard.
 */
class alignas(CACHE_LINE_SIZE) MultiShardSubscriber {
 public:
  MultiShardSubscriber(const ClientOptions& options, EventLoop* event_loop);

  ~MultiShardSubscriber();

  Status Start();

  const Statistics& GetStatistics();

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         MessageReceivedCallback deliver_callback,
                         SubscribeCallback subscription_callback,
                         DataLossCallback data_loss_callback);

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno);

  void TerminateSubscription(SubscriptionID sub_id);

  /** Saves state of the subscriber using provided storage strategy. */
  Status SaveState(SubscriptionStorage::Snapshot* snapshot, size_t worker_id);

 private:
  /** Options, whose lifetime must be managed by the owning client. */
  const ClientOptions& options_;
  /** An event loop object this subscriber runs on. */
  EventLoop* const event_loop_;

  /**
   * A map of subscribers one per each shard.
   * The map can be modified while some subscribers are running, therefore we
   * need them to be allocated separately.
   */
  std::unordered_map<size_t, std::unique_ptr<Subscriber>> subscribers_;

  /** A statistics object shared between subscribers. */
  std::shared_ptr<SubscriberStats> stats_;

  // TODO(t9432312)
  std::unordered_map<SubscriptionID, size_t> subscription_to_shard_;

  /**
   * Returns a subscriber for provided subscription ID or null if cannot
   * recognise the ID.
   */
  Subscriber* GetSubscriberForSubscription(SubscriptionID sub_id);

  /**
   * Synchronises a portion of pending subscribe and unsubscribe requests with
   * the Copilot. Takes into an account rate limits.
   */
  void SendPendingRequests();
};

/** A multi-threaded subscriber. */
class MultiThreadedSubscriber {
 public:
  MultiThreadedSubscriber(const ClientOptions& options,
                          std::shared_ptr<MsgLoop> msg_loop);

  Status Start();

  /**
   * If flow is non-null, the overflow is communicated via flow object.
   * Returns invalid SubscriptionHandle if and only if call attempt should be
   * retried due to queue overflow.
   */
  SubscriptionHandle Subscribe(Flow* flow,
                               SubscriptionParameters parameters,
                               MessageReceivedCallback deliver_callback,
                               SubscribeCallback subscription_callback,
                               DataLossCallback data_loss_callback);

  /**
   * If flow is non-null, the overflow is communicated via flow object.
   * Returns false if and only if call attempt should be retried due to queue
   * overflow.
   */
  bool Unsubscribe(Flow* flow, SubscriptionHandle sub_handle);

  /**
   * If flow is non-null, the overflow is communicated via flow object.
   * Returns false if and only if call attempt should be retried due to queue
   * overflow.
   */
  bool Acknowledge(Flow* flow, const MessageReceived& message);

  // TODO(t9457879)
  void SaveSubscriptions(SaveSubscriptionsCallback save_callback);

  Statistics GetStatisticsSync();

 private:
  /** Options provided when creating the Client. */
  const ClientOptions& options_;
  /** A set of loops to use. */
  const std::shared_ptr<MsgLoop> msg_loop_;

  /** One multi-threaded subscriber per thread. */
  std::vector<std::unique_ptr<MultiShardSubscriber>> subscribers_;
  /** Queues to communicate with each subscriber. */
  std::vector<std::unique_ptr<ThreadLocalQueues<std::unique_ptr<Command>>>>
      subscriber_queues_;

  /** Next subscription ID seed to be used for new subscription ID. */
  std::atomic<uint64_t> next_sub_id_;

  /**
   * Returns a new subscription handle. This method is thread-safe.
   *
   * @param worker_id A worker this subscription will be bound to.
   * @return A handle, if fails to allocate returns a null-handle.
   */
  SubscriptionHandle CreateNewHandle(size_t worker_id);

  /**
   * Extracts worker ID from provided subscription handle.
   * In case of error, returned worker ID is negative.
   */
  ssize_t GetWorkerID(SubscriptionHandle sub_handle) const;
};

}  // namespace rocketspeed
