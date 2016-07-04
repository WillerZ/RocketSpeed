// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "include/BaseEnv.h"
#include "include/HostId.h"
#include "include/RocketSpeed.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/util/common/subscription_id.h"

namespace rocketspeed {


class ClientOptions;
class Command;
class ExecuteCommand;
class MsgLoop;
class Statistics;
class SubscriberStats;
typedef uint64_t SubscriptionHandle;
class SubscriberIf;
template <typename>
class UnboundedMPSCQueue;

/** A multi-threaded subscriber. */
class MultiThreadedSubscriber {
 public:
  MultiThreadedSubscriber(const ClientOptions& options,
                          std::shared_ptr<MsgLoop> msg_loop);

  /**
   * Unsubscribes all subscriptions and prepares the subscriber for destruction.
   * Must be called while the MsgLoop this subscriber uses is still running.
   */
  void Stop();

  /**
   * Must be called after the MsgLoop this subscriber runs on is stopped.
   */
  ~MultiThreadedSubscriber();

  /**
   * Returns invalid SubscriptionHandle if and only if call attempt should be
   * retried due to queue overflow. In that case, observer will not be consumed.
   */
  SubscriptionHandle Subscribe(SubscriptionParameters parameters,
                               std::unique_ptr<Observer>& observer);

  /**
   * Unsubscribes from a subscription. Never fails.
   */
  void Unsubscribe(SubscriptionHandle sub_handle);

  /**
   * Returns false if and only if call attempt should be retried due to queue
   * overflow.
   */
  bool Acknowledge(const MessageReceived& message);

  // TODO(t9457879)
  void SaveSubscriptions(SaveSubscriptionsCallback save_callback);

  Statistics GetStatisticsSync();

 private:
  friend class SubscribeCommand;

  /** Options provided when creating the Client. */
  const ClientOptions& options_;
  /** A set of loops to use. */
  const std::shared_ptr<MsgLoop> msg_loop_;

  /** One multi-threaded subscriber per thread. */
  std::vector<std::unique_ptr<SubscriberIf>> subscribers_;
  /** Statistics per subscriber. */
  std::vector<std::shared_ptr<SubscriberStats>> statistics_;
  /** Queues to communicate with each subscriber. */
  std::vector<std::unique_ptr<
      UnboundedMPSCQueue<std::unique_ptr<ExecuteCommand>>>> subscriber_queues_;

  /** Next subscription ID seed to be used for new subscription ID. */
  std::atomic<uint64_t> next_sub_id_;

  /**
   * Returns a new subscription handle. This method is thread-safe.
   *
   * @param worker_id A worker this subscription will be bound to.
   * @return A handle, if fails to allocate returns a null-handle.
   */
  SubscriptionID CreateNewHandle(size_t shard_id, size_t worker_id);

  /**
   * Extracts worker ID from provided subscription ID.
   * In case of error, returned worker ID is negative.
   */
  ssize_t GetWorkerID(SubscriptionID sub_id) const;
};

}  // namespace rocketspeed
