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
class DataLossInfo;
class Flow;
class Logger;
class WakeLock;
class ClientHooks;

/** Notifies about the status of a message published to the RocketSpeed. */
typedef std::function<void(std::unique_ptr<ResultStatus>)> PublishCallback;

/**
 * Notifies the application about status of a subscription, see subscribe call
 * for details.
 */
typedef std::function<void(const SubscriptionStatus&)> SubscribeCallback;

typedef std::function<void(const DataLossInfo&)> DataLossCallback;

/** Notifies about status of a finished subscription snapshot. */
typedef std::function<void(Status)> SaveSubscriptionsCallback;

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

  // If true all subscriptions are silently forwarded to the tail and all
  // subscriptions on a single topic are merged into one upstream subscription.
  // Default: false
  bool collapse_subscriptions_to_tail;

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

  /** Creates options with default values. */
  ClientOptions();
};

/** Callback interface to be implemented by the subscribers. */
class Observer {
 public:
  /**
   * Notifies about message received on a subscription.
   *
   * The application can steal message from the reference argument, but if it
   * doesn't, using the message object after the callback yields undefined
   * behaviour.
   * If the application does not need message payload buffer outside of the
   * callback, it is advised not to steal the message object.
   */
  virtual void OnMessageReceived(Flow*, std::unique_ptr<MessageReceived>&) {}

  /**
   * Notifies about change of the status of the subscription.
   */
  virtual void OnSubscriptionStatusChange(const SubscriptionStatus&) {}

  /**
   * Notifies about data loss.
   */
  virtual void OnDataLoss(Flow*, const DataLossInfo&) {}

  virtual ~Observer() = default;
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
   * Sets default callback for announcing subscription status and message
   * delivery.
   *
   * This method is NOT thread-safe, if ever called, it must be right after
   * creating the client from the same thread. Caller must ensure that this call
   * "happened-before" any call which creates a new subscription.
   *
   * Note: the default callbacks have effect only if Subscribe's overload
   *       with separately specified callbacks is used. The default callbacks
   *       are applied instead of not specified (nullptr) ones. The default
   *       callbacks have no effect if Subscribe with an Observer is used.
   *
   * @param subscription_callback See docs for corresponding callback in
   *                              subscribe method.
   * @param deliver_callback See docs for corresponding callback in
   *                         subscribe method.
   * @param data_loss_callback See docs for corresponding callback in
   *                           subscribe method.
   */
  virtual void SetDefaultCallbacks(
      SubscribeCallback subscription_callback = nullptr,
      std::function<void(std::unique_ptr<MessageReceived>&)>
          deliver_callback = nullptr,
      DataLossCallback
          data_loss_callback = nullptr) = 0;

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
   *                         the default one is used (see SetDefaultCallbacks).
   * @param subscription_callback Invoked to notify status change of the
   *                              subscription.
   *                              See Observer::OnSubscriptionStatusChange
   *                              for extra information. If not specified
   *                              (nullptr) the default one is used
   *                              (see SetDefaultCallbacks).
   * @param data_loss_callback Invoked to notify there's been data loss.
   *                           See Observer::OnDataLoss for extra information.
   *                           If not specified (nullptr) the default one
   *                           is used (see SetDefaultCallbacks).
   */
  virtual SubscriptionHandle Subscribe(
      SubscriptionParameters parameters,
      std::function<void(std::unique_ptr<MessageReceived>&)>
          deliver_callback = nullptr,
      SubscribeCallback subscription_callback = nullptr,
      DataLossCallback
          data_loss_callback = nullptr) = 0;

  /** Convenience method, see the other overload for details. */
  virtual SubscriptionHandle Subscribe(
      TenantID tenant_id,
      NamespaceID namespace_id,
      Topic topic_name,
      SequenceNumber start_seqno,
      std::function<void(std::unique_ptr<MessageReceived>&)>
          deliver_callback = nullptr,
      SubscribeCallback subscription_callback = nullptr,
      DataLossCallback
          data_loss_callback = nullptr) = 0;

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
   * @param A handle that identifies the subscription.
   * @return Status::OK() iff unsubscription request was successfully enqueued.
   */
  virtual Status Unsubscribe(SubscriptionHandle sub_handle) = 0;

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
