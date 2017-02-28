// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <random>
#include <vector>

#include "BackPressure.h"
#include "EnvOptions.h"
#include "Slice.h"
#include "Status.h"
#include "SubscriptionStorage.h"
#include "Types.h"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif
namespace rocketspeed {

class BaseEnv;
class ClientHooks;
class DataLossInfo;
class Flow;
class Logger;
class Statistics;
class WakeLock;

/** Notifies about the status of a message published to the RocketSpeed. */
using PublishCallback = std::function<void(std::unique_ptr<ResultStatus>)>;

/**
 * Notifies the application about status of a subscription, see subscribe call
 * for details.
 */
using SubscribeCallback = std::function<void(const SubscriptionStatus&)>;

/** Notifies about messages delivered on a topic. */
using DeliverCallback =
    std::function<BackPressure(std::unique_ptr<MessageReceived>&)>;

/** Notifies about data loss. */
using DataLossCallback =
    std::function<BackPressure(const DataLossInfo&)>;

/** Notifies about status of a finished subscription snapshot. */
using SaveSubscriptionsCallback = std::function<void(Status)>;

using ClientRNG = std::mt19937_64;

/// A strategy that computes for how long the client should wait before
/// attempting to reconnect.
using BackOffStrategy =
    std::function<std::chrono::milliseconds(ClientRNG*, size_t retry)>;

/// Various strategies.
namespace backoff {

BackOffStrategy RandomizedTruncatedExponential(
    std::chrono::milliseconds backoff_value,
    std::chrono::milliseconds backoff_limit,
    double backoff_base,
    double jitter = 1.0);
}

/** Describes the Client object to be created. */
class ClientOptions {
 public:
  // Callback to invoke when subscription metadata changes.
  // Will be called even if Observer is provided.
  SubscribeCallback subscription_callback;

  // Callback to invoke when a message is delivered on a topic.
  // Will be called even if Observer is provided.
  DeliverCallback deliver_callback;

  // Callback to invoke when data loss occurs.
  // Will be called even if Observer is provided.
  DataLossCallback data_loss_callback;

  // Configuration of this service provider.
  std::shared_ptr<PublisherRouter> publisher;

  // Sharding and routing configuration for subscriptions.
  std::shared_ptr<ShardingStrategy> sharding;

  // Logger that is used for info messages.
  // Default: nullptr.
  std::shared_ptr<Logger> info_log;

  // BaseEnv to be used by all client side components.
  // Default: a default env of a client.
  BaseEnv* env;

  // Environment options that will be used by client components.
  // Default: default construction of EnvOptions.
  EnvOptions env_options;

  // WakeLock acquired by the client when it processes messages.
  // Default: nullptr
  std::shared_ptr<WakeLock> wake_lock;

  // Number of threads used by the client.
  // Default: 1
  int num_workers;

  // Maps subscriptions to workers based on subscription parameters.
  // Default: selects the least loaded worker.
  ThreadSelectionStrategy thread_selector;

  // Strategy for storing subscriptions.
  // Default: null-strategy.
  std::unique_ptr<SubscriptionStorage> storage;

  // Period of internal client clock. Determines resolution of backoff, rate
  // limiting time measurements, and publish timeouts.
  // Default: 200 ms
  std::chrono::milliseconds timer_period;

  // Calculates backoff.
  // Default: randomised, truncated exponential backoff.
  BackOffStrategy backoff_strategy;

  // If client receives a burst of messages on non-existent subscription ID, it
  // will respond with unsubscribe message only once every specified duration.
  // Default: 10s
  std::chrono::milliseconds unsubscribe_deduplication_timeout;

  // Minimum time to wait before declaring a publish as failed due to timeout.
  // Default: 5s
  std::chrono::milliseconds publish_timeout;

  // Max number of open subscriptions a client can have.
  // The client returns SubscriptionHandle(0) if the limit is exceeded.
  // Default: std::numeric_limits<size_t>::max()
  size_t max_subscriptions;

  // Time to wait after terminating the last subscription on a connection before
  // disconnecting.
  // Default: 0s
  std::chrono::milliseconds connection_without_streams_keepalive;

  // Maximum number of subscribe/unsubscribe events per second.
  // Note: Events are throttled on timer_period intervals.
  // That is if the limit is set to 1000 per second and timer_period is 200ms
  // the actually applied rate limit will be 200 per 200 ms.
  // Default: 1,000,000,000 (effectively unlimited)
  size_t subscription_rate_limit;

  // Size of per-thread queues.
  // Queues are pre-allocated, so larger queues consume more memory, but
  // can be used to smooth out larger spikes in subscriptions.
  // Default: 50000
  size_t queue_size;

  // Size of the allocator for SubscriptionIDs.
  // Larger allocator occupies more memory, but reduces contention and increases
  // lifetime of a client.
  // Default: 1024
  size_t allocator_size;

  // If provided, the statistics visitor will be invoked every minute with
  // client statistics, including rolling averages over the past minute, hour,
  // and day. The visitor will be invoked from the same thread each time, and
  // then flushed.
  // Default: nullptr
  std::shared_ptr<StatisticsVisitor> statistics_visitor;

  // Prefix to use for statistics.
  // Default: "rocketspeed"
  std::string stats_prefix;

  // Should a new health notification propagate to observers? This
  // doesn't disable the heartbeat timeout mechanism. It will stop all
  // observers from being notified when a shard's health status
  // changes.
  // Default: true
  bool should_notify_health;

  // Notify observers that a shard is unhealthy if it is still trying
  // to connect after this many attempts.
  // Default: 3
  size_t max_silent_reconnects;

  // Notify observers that a shard is unhealthy if we haven't received
  // a heartbeat within this period. Set to zero to disable the
  // timeout mechanism. Heartbeats will still be received but will
  // have no effect.
  // Default: 2 minutes
  std::chrono::milliseconds heartbeat_timeout;

  // When the last subscription on a shard is unsubscribed, the client closes
  // the stream to free some resources. Freeing the stream has some overhead
  // both on the client and server. If this happens often, and we expect a new
  // subscription later, it may be less expensive to keep the stream alive.
  // This option sets the time to stay alive while inactive.
  // Default: 10 seconds
  std::chrono::milliseconds inactive_stream_linger;

  // A map of properties (key:value) provided by the client to introduce itself
  // to the server.
  // The properties are known by both client and server defined by the
  // application.

  // Properties specific to the stream
  // A message sent on a stream with these properties will be received on a
  // stream with the same properties, therefore it can be used to encode
  // contractual protocol information.
  // The same is not true for client properties, since there may be
  // proxies in between.
  IntroProperties stream_properties;

  // Properties specific to the client
  IntroProperties client_properties;

  // TenantID of the client
  TenantID tenant_id;

  /** Creates options with default values. */
  ClientOptions();
};

/** Callback interface to be implemented by the subscribers. */
class Observer {
 public:
  /**
   * Notifies about change of the status of the subscription.
   */
  virtual void OnSubscriptionStatusChange(const SubscriptionStatus&) {}

  /**
   * New API for applying backpressure. These are temporary until we use
   * global callbacks everywhere.
   */
  virtual BackPressure OnData(std::unique_ptr<MessageReceived>& msg) {
    OnMessageReceived(nullptr, msg);
    return BackPressure::None();
  }

  virtual BackPressure OnLoss(const DataLossInfo& dl) {
    OnDataLoss(nullptr, dl);
    return BackPressure::None();
  }

  virtual ~Observer() = default;

 private:
  /**
   * Notifies about message received on a subscription.
   *
   * The application can steal message from the reference argument, but if it
   * doesn't, using the message object after the callback yields undefined
   * behaviour.
   * If the application does not need message payload buffer outside of the
   * callback, it is advised not to steal the message object.
   */
  // DEPRECATED
  virtual void OnMessageReceived(Flow*, std::unique_ptr<MessageReceived>&) {}

  /**
   * Notifies about data loss.
   */
  // DEPRECATED
  virtual void OnDataLoss(Flow*, const DataLossInfo&) {}
};

/** The Client is used to produce and consume messages on arbitrary topics. */
class Client {
 public:
  /**
   * Creates the client object.
   *
   * @param options The description of client object to be created.
   * @return Status::OK() iff client was created successfully.
   */
  static Status Create(ClientOptions client_options,
                       std::unique_ptr<Client>* client);

  /** Closes the client, connections and frees all allocated resources. */
  virtual ~Client() = default;

  /**
   * Connect hooks instance to specified subscription.
   * Subscription may exists or not. Only one instance of hooks
   * is allowed to be installed at the same time.
   *
   * @params params subscription description
   * @params hooks hooks callback
   */
  virtual void InstallHooks(const HooksParameters& params,
                            std::shared_ptr<ClientHooks> hooks) = 0;
  /**
   * Disconnect hooks instance from subscription (no matter its state).
   * This call should match previous InstallHooks() call.
   *
   * @params params subscription description
   */
  virtual void UnInstallHooks(const HooksParameters& params) = 0;

  /**
   * Asynchronously publishes a new message to the Topic. The return parameter
   * indicates whether the publish was successfully enqueued.
   *
   * @param tenant_id ID of tenant responsible for the publish.
   * @param topic_name Name of this topic to be opened
   * @param topic_namespace Namespace of this topic name
   * @param options Quality of service for this Topic
   * @param data Payload of message
   * @param callback Callback to call with response from RocketSpeed. This will
   *                 only be called when Publish was successful. The result will
   *                 be ok if the message was successfully committed into
   *                 RocketSpeed, otherwise an error will be provided.
   * @params message_id The provided message_id, optional
   * @return the status and message ID of the published message.
   */
  virtual PublishStatus Publish(const TenantID tenant_id,
                                const Topic& topic_name,
                                const NamespaceID& topic_namespace,
                                const TopicOptions& options,
                                const Slice& data,
                                PublishCallback callback = nullptr,
                                const MsgId message_id = MsgId()) = 0;

  /**
   * Subscribes to a topic with provided parameters.
   *
   * @param parameters Parameters of the subscription.
   * @param observer Observer interface with one or more methods implemented
   *                 by the application. Must be not-nullptr. Will only be
   *                 moved away if a valid subscription handle is returned.
   * @return A handle that identifies this subscription. The handle is unengaged
   *         iff the Client failed to create the subscription
   *         or the subscription limit is reached.
   */
  virtual SubscriptionHandle Subscribe(SubscriptionParameters parameters,
                                       std::unique_ptr<Observer>& observer) = 0;

  /**
   * As above, except for rvalue observers.
   *
   * @param parameters Parameters of the subscription.
   * @param observer Typed observer rvalue, same as for the Subscribe method
   *                 above. The observer is moved away only if a valid
   *                 subscription handle is returned.
   * @return A handle that identifies the subscription (as above).
   */
  template <typename ObserverType>
  SubscriptionHandle Subscribe(SubscriptionParameters parameters,
                               std::unique_ptr<ObserverType>&& observer) {
    std::unique_ptr<Observer> tmp = std::move(observer);
    auto r = Subscribe(std::move(parameters), tmp);
    if (!r) {
      observer.reset(static_cast<ObserverType*>(tmp.release()));
    }
    return r;
  }

  SubscriptionHandle Subscribe(SubscriptionParameters parameters) {
    std::unique_ptr<Observer> tmp;
    return Subscribe(std::move(parameters), tmp);
  }

  /**
   * Convenience method, see the other overload for details.
   *
   * Note: usage of std::function-for-callback overloads of Subscribe may
   *       result in increased memory usage when compared to
   *       Observer-for-callback overloads.
   *
   * @param deliver_callback Invoked with every message received on the
   *                         subscription. See Observer::OnMessageReceived
   *                         for extra information. If not specified (nullptr)
   *                         the default one is used.
   * @param subscription_callback Invoked to notify status change of the
   *                              subscription.
   *                              See Observer::OnSubscriptionStatusChange
   *                              for extra information. If not specified
   *                              (nullptr) the default one is used.
   * @param data_loss_callback Invoked to notify there's been data loss.
   *                           See Observer::OnDataLoss for extra information.
   *                           If not specified (nullptr) the default one
   *                           is used.
   */
  virtual SubscriptionHandle Subscribe(
      SubscriptionParameters parameters,
      DeliverCallback deliver_callback,
      SubscribeCallback subscription_callback = nullptr,
      DataLossCallback
          data_loss_callback = nullptr) = 0;

  /** Convenience method, see the other overload for details. */
  // DEPRECATED
  virtual SubscriptionHandle Subscribe(
      TenantID tenant_id,
      NamespaceID namespace_id,
      Topic topic_name,
      SequenceNumber start_seqno,
      DeliverCallback deliver_callback,
      SubscribeCallback subscription_callback = nullptr,
      DataLossCallback
          data_loss_callback = nullptr) = 0;

  SubscriptionHandle Subscribe(
      TenantID tenant_id,
      NamespaceID namespace_id,
      Topic topic_name,
      SequenceNumber start_seqno) {
    return Subscribe({tenant_id,
                      std::move(namespace_id),
                      std::move(topic_name),
                      start_seqno});
  }

  /**
   * Unsubscribes from a topic identified by provided handle.
   *
   * Subscribe callback is invoked when the client is successfully unsubscribed.
   * Messages arriving on this subscription after client unsubscribed, are
   * silently dropped.
   *
   * The method should not be called more than once for a given handle.
   * Doing so will result in inconsistent state of number of open subscriptions.
   * The number of subscriptions allowed by the client might be greater
   * than the max_subscription limit specified in ClientOptions.
   * Risk of std::abort() if the number of subscriptions go below 0.
   *
   * @param namespace_id Namespace of subscription to unsubscribe.
   * @param topic Topic to unsubscribe.
   * @param A handle that identifies the subscription.
   * @return Status::OK() iff unsubscription request was successfully enqueued.
   */
  virtual Status Unsubscribe(NamespaceID namespace_id, Topic topic) = 0;

  // DEPRECATED
  virtual Status Unsubscribe(NamespaceID namespace_id,
                             Topic topic,
                             SubscriptionHandle sub_handle) = 0;

  /**
   * Acknowledges that this message was processed by the application.
   * All sequence numbers no later than the sequence number of given message
   * are as well considered to be processed by the application. When application
   * saves, and then restores subscription state, the subscription will continue
   * from the next sequence number.
   *
   * @param message The message to be acknowledged.
   * @return Status::OK() iff acknowledgement was successful.
   */
  virtual Status Acknowledge(const MessageReceived& message) = 0;

  /**
   * Check if a subscription has had an update in the recent past. This can
   * be useful when trying to synchronise two subscribers on the same topic,
   * which may be at different sequence numbers.
   *
   * The callback will be called with 'true' if there has been a message on the
   * topic between (source, seqno) and the current position of the subscription.
   * The callback will be invoked in the same thread as the subscription
   * callbacks, and in the same order so that the result is correct for the
   * subscription's position as the application sees it.
   *
   * The second parameter to the callback is an opaque binary blob provided
   * by the data source. RocketSpeed ignores this value so interpretation
   * is up to the implementation.
   *
   * The lower bound is non-inclusive. A message at (source, seqno) will not
   * be considered in the result.
   *
   * @param sub_handle The subscription to test against.
   * @param namespace_id The namespace of the subscription.
   * @param topic The topic of the subscription.
   * @param source The source of the lower bound on the query.
   * @param seqno The sequence number of the lower bound on the query.
   * @param callback The callback to invoke when the query completes. This is
   *                 guaranteed to be called, unless the client is destroyed.
   *                 The argument gives the result (see HasMessageSinceResult).
   * @return Status::OK() if the message was sent.
   *         Status::NoBuffer() if the message could not be queued. In this
   *         case the application should try again later.
   */
  virtual Status HasMessageSince(
      SubscriptionHandle sub_handle,
      NamespaceID namespace_id,
      Topic topic,
      DataSource source,
      SequenceNumber seqno,
      std::function<void(HasMessageSinceResult, std::string)> callback) = 0;

  /**
   * Saves state of subscriptions according to strategy selected when opening
   * the client.
   * All messages acknowledged before this call will be included in the saved
   * state, which means that the application will be able to restore them using
   * the following call.
   *
   * @param save_callback A callback to inform whether saving succeeded.
   */
  virtual void SaveSubscriptions(SaveSubscriptionsCallback save_callback) = 0;

  /**
   * Reads all subscriptions saved by strategy selected when opening the client.
   * Returns a list with subscription prototypes, so that an application can
   * decide which subscriptions should be restored and provide appropriate
   * callbacks.
   *
   * @subscriptions An out parameter with a list of restored subscriptions.
   * @return Status::OK iff subscriptions were restored successfully.
   */
  virtual Status RestoreSubscriptions(
      std::vector<SubscriptionParameters>* subscriptions) = 0;

  /**
   * Walks over all statistics using the provided StatisticsVisitor.
   *
   * @param visitor Used to visit all statistics maintained by the client.
   */
  virtual void ExportStatistics(StatisticsVisitor* visitor) const = 0;

  /** Returns client statistics. */
  virtual Statistics GetStatisticsSync() const = 0;

  /**
   * Calls the function on the thread dedicated for the subscription.
   * For single threaded clients it should be called synchronously.
   *
   * @params params A subscription specific to the job.
   * @params job A function to be called.
   * @return true iff the job was successfully scheduled to be executed
   */
  virtual bool CallInSubscriptionThread(SubscriptionParameters params,
                                        std::function<void()> job) {
    RS_ASSERT(false) << "Not implemented";
    return false;
  }
};

}  // namespace rocketspeed
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
