// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "multi_shard_subscriber.h"

#include <chrono>
#include <cmath>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "external/folly/Memory.h"
#include "external/folly/move_wrapper.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/single_shard_subscriber.h"
#include "src/util/common/subscription_id.h"
#include "src/client/tail_collapsing_subscriber.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
#include "src/port/port.h"
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/common/rate_limiter_sink.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"
#include "src/util/xxhash.h"

namespace rocketspeed {

///////////////////////////////////////////////////////////////////////////////
MultiShardSubscriber::MultiShardSubscriber(
    const ClientOptions& options,
    EventLoop* event_loop,
    std::shared_ptr<SubscriberStats> stats)
: options_(options), event_loop_(event_loop), stats_(std::move(stats)) {}

MultiShardSubscriber::~MultiShardSubscriber() {
  subscribers_.clear();
}

void MultiShardSubscriber::StartSubscription(
    SubscriptionID sub_id,
    SubscriptionParameters parameters,
    std::unique_ptr<Observer> observer) {
  // Determine the shard ID.
  size_t shard_id = options_.sharding->GetShard(parameters.namespace_id,
                                                parameters.topic_name);
  subscription_to_shard_.emplace(sub_id, shard_id);

  // Find or create a subscriber for this shard.
  auto it = subscribers_.find(shard_id);
  if (it == subscribers_.end()) {
    // Subscriber is missing, create and start one.
    std::unique_ptr<SubscriberIf> subscriber(new Subscriber(
        options_, event_loop_, stats_, options_.sharding->GetRouter(shard_id)));
    if (options_.collapse_subscriptions_to_tail) {
      // TODO(t10132320)
      RS_ASSERT(parameters.start_seqno == 0);
      auto sub = static_cast<Subscriber*>(subscriber.release());
      subscriber.reset(
          new TailCollapsingSubscriber(std::unique_ptr<Subscriber>(sub)));
    }

    // Put it back in the map so it can be resued.
    auto result = subscribers_.emplace(shard_id, std::move(subscriber));
    RS_ASSERT(result.second);
    it = result.first;
  }

  it->second->StartSubscription(
      sub_id, std::move(parameters), std::move(observer));
}

void MultiShardSubscriber::Acknowledge(SubscriptionID sub_id,
                                       SequenceNumber seqno) {
  if (auto subscriber = GetSubscriberForSubscription(sub_id)) {
    subscriber->Acknowledge(sub_id, seqno);
  }
}

void MultiShardSubscriber::TerminateSubscription(SubscriptionID sub_id) {
  if (auto subscriber = GetSubscriberForSubscription(sub_id)) {
    subscriber->TerminateSubscription(sub_id);
    if (subscriber->Empty()) {
      // Subscriber no longer serves any subscriptions, destroy it
      auto it = subscription_to_shard_.find(sub_id);
      RS_ASSERT(it != subscription_to_shard_.end());
      subscribers_.erase(it->second);
    }
  }
  // Remove the mapping from subscription ID to a shard.
  subscription_to_shard_.erase(sub_id);
}

Status MultiShardSubscriber::SaveState(SubscriptionStorage::Snapshot* snapshot,
                                       size_t worker_id) {
  for (const auto& subscriber : subscribers_) {
    auto st = subscriber.second->SaveState(snapshot, worker_id);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

SubscriberIf* MultiShardSubscriber::GetSubscriberForSubscription(
    SubscriptionID sub_id) {
  auto it = subscription_to_shard_.find(sub_id);
  if (it == subscription_to_shard_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot find subscriber for SubscriptionID(%llu)",
             sub_id.ForLogging());
    return nullptr;
  }

  auto it1 = subscribers_.find(it->second);
  if (it1 == subscribers_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot find subscriber for ShardID(%zu)",
             it->second);
    return nullptr;
  }
  return it1->second.get();
}

}  // namespace rocketspeed
