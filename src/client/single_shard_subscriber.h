/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <unordered_map>

#include "include/BaseEnv.h"
#include "include/HostId.h"
#include "include/RocketSpeed.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/subscriber_if.h"
#include "src/util/common/subscription_id.h"
#include "src/client/subscriptions_map.h"
#include "src/messages/messages.h"
#include "src/messages/types.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

class Status;
class SubscriberStats;

class SubscriptionStatusImpl : public SubscriptionStatus {
 public:
  const SubscriptionState& state_;
  Status status_;
  NamespaceID namespace_id_;
  Topic topic_name_;

  explicit SubscriptionStatusImpl(const SubscriptionState& state)
  : state_(state)
  , namespace_id_(state.GetNamespace().ToString())
  , topic_name_(state.GetTopicName().ToString()) {}

  SubscriptionHandle GetSubscriptionHandle() const override {
    // Note that we never rewind a subscriptions, hence the ID here matches the
    // handle given out to the user.
    return state_.GetIDWhichMayChange();
  }

  TenantID GetTenant() const override { return state_.GetTenant(); }

  const NamespaceID& GetNamespace() const override {
    // TODO: may return Slice just as well
    return namespace_id_;
  }

  const Topic& GetTopicName() const override {
    // TODO: may return Slice just as well
    return topic_name_;
  }

  SequenceNumber GetSequenceNumber() const override {
    return state_.GetExpectedSeqno();
  }

  const Status& GetStatus() const override { return status_; }
};

/// A subscriber that manages subscription on a single shard.
class Subscriber : public SubscriberIf {
 public:
  Subscriber(const ClientOptions& options,
             EventLoop* event_loop,
             std::shared_ptr<SubscriberStats> stats,
             std::unique_ptr<SubscriptionRouter> router);

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         std::unique_ptr<Observer> observer) override;

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno) override;

  void TerminateSubscription(SubscriptionID sub_id) override;

  bool Empty() const override { return subscriptions_map_.Empty(); }

  Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                   size_t worker_id) override;

  SubscriptionState* GetState(SubscriptionID sub_id) override {
    return subscriptions_map_.Find(sub_id);
  }

 private:
  ThreadCheck thread_check_;

  /// Options, whose lifetime must be managed by the owning client.
  const ClientOptions& options_;
  /// An event loop object this subscriber runs on.
  EventLoop* const event_loop_;
  /// A shared statistics.
  std::shared_ptr<SubscriberStats> stats_;

  SubscriptionsMap<SubscriptionState> subscriptions_map_;

  std::unordered_map<SubscriptionID, SequenceNumber> last_acks_map_;

  /// Version of the router when we last fetched hosts.
  size_t last_router_version_;
  /// The router for this subscriber.
  std::unique_ptr<SubscriptionRouter> router_;

  /// A timer to periodically check for router updates.
  std::unique_ptr<EventCallback> router_timer_;

  /// Returns sequence number of last acknowledged message about
  /// the given subscription id.
  SequenceNumber GetLastAcknowledged(SubscriptionID sub_id) const;

  /// Checkt if destination host has been updated.
  void CheckRouterVersion();

  void ReceiveDeliver(Flow* flow,
                      SubscriptionState*,
                      std::unique_ptr<MessageDeliver>);

  void ReceiveTerminate(Flow* flow,
                        SubscriptionState*,
                        std::unique_ptr<MessageUnsubscribe>);
};

}  // namespace rocketspeed
