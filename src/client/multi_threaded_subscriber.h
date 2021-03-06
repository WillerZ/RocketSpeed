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
class HasMessageSinceParams;
class MsgLoop;
class Statistics;
class SubscriberStats;
typedef uint64_t SubscriptionHandle;
class SubscriberIf;
template <typename>
class UnboundedMPSCQueue;
class SubscriberHooks;

/** A multi-threaded subscriber. */
class MultiThreadedSubscriber {
 public:
  MultiThreadedSubscriber(const ClientOptions& options,
                          std::shared_ptr<MsgLoop> msg_loop);

  void InstallHooks(const HooksParameters& params,
                    std::shared_ptr<SubscriberHooks> hooks);
  void UnInstallHooks(const HooksParameters& params);

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
  void Unsubscribe(NamespaceID namespace_id,
                   Topic topic,
                   SubscriptionHandle sub_handle);

  /**
   * Returns false if and only if call attempt should be retried due to queue
   * overflow.
   */
  bool Acknowledge(const MessageReceived& message);

  Status HasMessageSince(HasMessageSinceParams params);

  // TODO(t9457879)
  void SaveSubscriptions(SaveSubscriptionsCallback save_callback);

  /**
   * Returns false if and only if call attempt should be retried due to queue
   * overflow.
   */
  bool CallInSubscriptionThread(SubscriptionParameters parameters,
                                std::function<void()> job);

  Statistics GetStatisticsSync();

 private:
  friend class SubscribeCommand;

  /** Options provided when creating the Client. */
  const ClientOptions& options_;
  /** A set of loops to use. */
  const std::shared_ptr<MsgLoop> msg_loop_;

  /** shared_ptr to introduction parameters */
  const std::shared_ptr<const IntroParameters> intro_parameters_;

  /** One multi-shard subscriber per thread. */
  std::vector<std::unique_ptr<SubscriberIf>> subscribers_;
  /** Statistics per subscriber. */
  std::vector<std::shared_ptr<SubscriberStats>> statistics_;
  /** Queues to communicate with each subscriber. */
  std::vector<std::unique_ptr<
      UnboundedMPSCQueue<std::unique_ptr<ExecuteCommand>>>> subscriber_queues_;

  SubscriptionIDAllocator allocator_;

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
