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
#include "src/client/subscriber_if.h"
#include "src/util/common/subscription_id.h"
#include "src/port/port.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

class ClientOptions;
class Flow;
class EventLoop;
class Stream;
class SubscriberStats;
class SubscriptionState;
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
                       std::shared_ptr<SubscriberStats> stats);

  ~MultiShardSubscriber() override;

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         std::unique_ptr<Observer> observer) override;

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno) override;

  void TerminateSubscription(SubscriptionID sub_id) override;

  bool Empty() const override { return subscribers_.empty(); }

  Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                   size_t worker_id) override;

  SubscriptionState *GetState(SubscriptionID) override {
    RS_ASSERT(false);
    return nullptr;
  }

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
  std::unordered_map<size_t, std::unique_ptr<SubscriberIf>> subscribers_;

  /** A statistics object shared between subscribers. */
  std::shared_ptr<SubscriberStats> stats_;

  /**
   * Returns a subscriber for provided subscription ID or null if cannot
   * recognise the ID.
   */
  SubscriberIf* GetSubscriberForSubscription(SubscriptionID sub_id);
};

}  // namespace rocketspeed
