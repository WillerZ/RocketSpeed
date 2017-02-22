// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <unordered_map>

#include "include/BaseEnv.h"
#include "include/HostId.h"
#include "include/RocketSpeed.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/subscriber_hooks_container.h"
#include "src/client/subscriber_if.h"
#include "src/port/port.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/statistics.h"
#include "src/util/common/subscription_id.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

class ClientOptions;
class Flow;
class EventLoop;
class Stream;
class SubscriberStats;
class SubscriptionID;
typedef uint64_t SubscriptionHandle;

/**
 * A single threaded thread-unsafe subscriber, that lazily brings up subscribers
 * per shard.
 */
class alignas(CACHE_LINE_SIZE) MultiShardSubscriber : public SubscriberIf {
 public:
  MultiShardSubscriber(const ClientOptions& options,
                       EventLoop* event_loop,
                       std::shared_ptr<SubscriberStats> stats,
                       size_t max_active_subscriptions,
                       std::shared_ptr<const IntroParameters> intro_parameters);

  ~MultiShardSubscriber() override;

  void InstallHooks(const HooksParameters& params,
                    std::shared_ptr<SubscriberHooks> hooks) override;
  void UnInstallHooks(const HooksParameters& params) override;

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         std::unique_ptr<Observer> observer) override;

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno) override;

  void HasMessageSince(HasMessageSinceParams params) override;

  void TerminateSubscription(NamespaceID namespace_id,
                             Topic topic,
                             SubscriptionID sub_id) override;

  bool Empty() const override { return subscribers_.empty(); }

  Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                   size_t worker_id) override;

  void RefreshRouting() override;

  void NotifyHealthy(bool isHealthy) override;

  // I should forward the call to per shard subscriber but one might not exists
  // yet.
  // We're single threaded so there's a little reason to create it only to call
  // the function.
  bool CallInSubscriptionThread(SubscriptionParameters,
                                std::function<void()> job) override {
    job();
    return true;
  }

 private:
  void GarbageCollectInactiveSubscribers();

  /** Options, whose lifetime must be managed by the owning client. */
  const ClientOptions& options_;
  /** An event loop object this subscriber runs on. */
  EventLoop* const event_loop_;

  /**
   * A map of subscribers one per each shard.
   * The map can be modified while some subscribers are running, therefore we
   * need them to be allocated separately.
   */
  std::unordered_map<size_t, std::unique_ptr<SubscriberIf>> subscribers_;

  /** A statistics object shared between subscribers. */
  std::shared_ptr<SubscriberStats> stats_;

  /// Version of the router when we last fetched hosts.
  size_t last_router_version_;

  /// A timer to periodically check for router updates
  std::unique_ptr<EventCallback> maintenance_timer_;

  /**
   * Returns a subscriber for provided shard ID or null if cannot
   * recognise the ID.
   */
  SubscriberIf* GetSubscriberForShard(ShardID shard_id);

  /** Maximum number of active subscription across the shards in this thread. */
  const size_t max_active_subscriptions_;

  /** Number of active subscriptions in this thread across shards. */
  std::shared_ptr<size_t> num_active_subscriptions_;

  /** stash of hooks for not (yet) existing subscribers */
  using HooksMap =
      std::unordered_map<HooksParameters, std::shared_ptr<SubscriberHooks>>;
  std::unordered_map<ShardID, HooksMap> pending_hooks_;

  /**
   * List of empty shards. This adds hysteresis to the cleanup process to avoid
   * rapid oscillations in the number of active streams.
   */
  TimeoutList<ShardID> inactive_shards_;

  /** shared_ptr to introduction parameters */
  std::shared_ptr<const IntroParameters> intro_parameters_;
};

}  // namespace rocketspeed
