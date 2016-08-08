/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "include/BaseEnv.h"
#include "include/HostId.h"
#include "include/RocketSpeed.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/single_shard_subscriber.h"
#include "src/client/topic_subscription_map.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

class ClientOptions;
class Flow;
class Stream;
class SubscriberStats;
class SubscriptionID;

namespace detail {
class TailCollapsingObserver;
}  // namespace detail

/**
 * A subscriber adaptor that collapses subscriptions, so that all downstream
 * subscriptions on a one particular topic are served from a single, tail
 * upstream subscription.
 */
class TailCollapsingSubscriber : public SubscriberIf {
 public:
  explicit TailCollapsingSubscriber(std::unique_ptr<SubscriberIf> subscriber);

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         std::unique_ptr<Observer> observer) override;

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno) override;

  void TerminateSubscription(SubscriptionID sub_id) override;

  bool Empty() const override { return subscriber_->Empty(); }

  Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                   size_t worker_id) override;

  void RefreshRouting() override {
    subscriber_->RefreshRouting();
  }

  void NotifyHealthy(bool isHealthy) override {
    subscriber_->NotifyHealthy(isHealthy);
  }

 private:
  friend class detail::TailCollapsingObserver;

  ThreadCheck thread_check_;
  /** The underlying subscriber. */
  std::unique_ptr<SubscriberIf> subscriber_;

  /**
   * Maps ID of downstream subscription to the ID of the upstream one that
   * serves the former, only for collapsed subscriptions.
   */
  std::unordered_map<SubscriptionID, SubscriptionID> downstream_to_upstream_;
  /**
   * Stores all existing instances of TailCollapsingObserver created by this
   * subscriber. Useful when determining whether given upstream subscription
   * serves multiple downstream ones.
   */
  std::unordered_set<Observer*> special_observers_;
  /**
   * The map that we use for finding an upstream subscription for given topic
   * and namespace.
   */
  TopicToSubscriptionMap upstream_subscriptions_;
};

}  // namespace rocketspeed
