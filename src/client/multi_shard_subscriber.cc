// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "multi_shard_subscriber.h"

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/single_shard_subscriber.h"
#include "src/client/subscriber_stats.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
#include "src/port/port.h"
#include "src/util/common/processor.h"
#include "src/util/common/rate_limiter_sink.h"
#include "src/util/common/statistics.h"
#include "src/util/common/subscription_id.h"
#include "src/util/timeout_list.h"
#include <xxhash.h>

namespace rocketspeed {

///////////////////////////////////////////////////////////////////////////////
MultiShardSubscriber::MultiShardSubscriber(
    const ClientOptions& options,
    EventLoop* event_loop,
    std::shared_ptr<SubscriberStats> stats,
    size_t max_active_subscriptions,
    std::shared_ptr<const IntroParameters> intro_parameters)
: options_(options)
, event_loop_(event_loop)
, stats_(std::move(stats))
, last_router_version_(options_.sharding->GetVersion())
, max_active_subscriptions_(max_active_subscriptions)
, num_active_subscriptions_(std::make_shared<size_t>(0))
, intro_parameters_(intro_parameters) {
  RS_ASSERT(intro_parameters_);

  // Periodically check for new router versions.
  maintenance_timer_ = event_loop_->CreateTimedEventCallback(
    [this]() {
      RefreshRouting();
      GarbageCollectInactiveSubscribers();
    },
    options_.timer_period);
  maintenance_timer_->Enable();
}

MultiShardSubscriber::~MultiShardSubscriber() {
  subscribers_.clear();
}

void MultiShardSubscriber::InstallHooks(
    const HooksParameters& params, std::shared_ptr<SubscriberHooks> hooks) {
  size_t shard_id = options_.sharding->GetShard(
      params.namespace_id, params.topic_name, *intro_parameters_);
  auto it = subscribers_.find(shard_id);
  if (it != subscribers_.end()) {
    it->second->InstallHooks(params, hooks);
  } else {
    pending_hooks_[shard_id].emplace(params, hooks);
  }
}

void MultiShardSubscriber::UnInstallHooks(const HooksParameters& params) {
  size_t shard_id = options_.sharding->GetShard(
      params.namespace_id, params.topic_name, *intro_parameters_);
  auto it = subscribers_.find(shard_id);
  if (it == subscribers_.end()) {
    auto& shard_hooks = pending_hooks_[shard_id];
    shard_hooks.erase(params);
    if (shard_hooks.empty()) {
      pending_hooks_.erase(shard_id);
    }
  } else {
    it->second->UnInstallHooks(params);
  }
}

void MultiShardSubscriber::RefreshRouting() {
  stats_->router_version_checks->Add(1);
  const auto version = options_.sharding->GetVersion();
  if (last_router_version_ != version) {
    stats_->router_version_changes->Add(1);
    last_router_version_ = version;

    // Routing has changed; inform all shards.
    for (auto& entry : subscribers_) {
      entry.second->RefreshRouting();
    }
  }
}

void MultiShardSubscriber::GarbageCollectInactiveSubscribers() {
  // Once shards have been inactive for some time (i.e. they have no
  // subscriptions), we remove them and close the stream.
  inactive_shards_.ProcessExpired(
      options_.inactive_stream_linger,
      [this] (ShardID shard_id) {
        auto it = subscribers_.find(shard_id);
        if (it != subscribers_.end()) {
          if (it->second->Empty()) {
            subscribers_.erase(it);
          }
        }
      },
      -1);
}

void MultiShardSubscriber::NotifyHealthy(bool isHealthy) {
  for (const auto& kv : subscribers_) {
    kv.second->NotifyHealthy(isHealthy);
  }
}

void MultiShardSubscriber::StartSubscription(
    SubscriptionID sub_id,
    SubscriptionParameters parameters,
    std::unique_ptr<Observer> observer) {
  // Determine the shard ID.
  ShardID shard_id = options_.sharding->GetShard(
        parameters.namespace_id, parameters.topic_name, *intro_parameters_);

  // Find or create a subscriber for this shard.
  auto it = subscribers_.find(shard_id);
  if (it == subscribers_.end()) {
    // Subscriber is missing, create and start one.
    std::unique_ptr<SubscriberIf> subscriber(
        new Subscriber(options_,
                       event_loop_,
                       stats_,
                       shard_id,
                       max_active_subscriptions_,
                       num_active_subscriptions_,
                       intro_parameters_));
    auto hooks = pending_hooks_.find(shard_id);
    if (hooks != pending_hooks_.end()) {
      for (auto& p : hooks->second) {
        subscriber->InstallHooks(p.first, p.second);
      }
      pending_hooks_.erase(hooks);
    }

    // Put it back in the map so it can be reused.
    auto result = subscribers_.emplace(shard_id, std::move(subscriber));
    RS_ASSERT(result.second);
    it = result.first;
  }

  it->second->StartSubscription(
      sub_id, std::move(parameters), std::move(observer));
}

void MultiShardSubscriber::Acknowledge(SubscriptionID sub_id,
                                       SequenceNumber seqno) {
  if (auto subscriber = GetSubscriberForShard(sub_id.GetShardID())) {
    subscriber->Acknowledge(sub_id, seqno);
  }
}

void MultiShardSubscriber::HasMessageSince(HasMessageSinceParams params) {
  ShardID shard_id = options_.sharding->GetShard(
      params.namespace_id, params.topic, *intro_parameters_);
  if (auto subscriber = GetSubscriberForShard(shard_id)) {
    subscriber->HasMessageSince(std::move(params));
  }
}

void MultiShardSubscriber::TerminateSubscription(NamespaceID namespace_id,
                                                 Topic topic,
                                                 SubscriptionID sub_id) {
  ShardID shard_id;
  if (sub_id) {
    shard_id = sub_id.GetShardID();
  } else {
    shard_id = options_.sharding->GetShard(
        namespace_id, topic, *intro_parameters_);
  }
  if (auto subscriber = GetSubscriberForShard(shard_id)) {
    subscriber->TerminateSubscription(std::move(namespace_id),
                                      std::move(topic),
                                      sub_id);
    if (subscriber->Empty()) {
      // Subscriber no longer serves any subscriptions, destroy it if this is
      // still true after some time.
      inactive_shards_.Add(shard_id);

      if (options_.inactive_stream_linger.count() == 0) {
        // Fast, deterministic path when linger is 0.
        GarbageCollectInactiveSubscribers();
      }
    }
  }
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

SubscriberIf* MultiShardSubscriber::GetSubscriberForShard(ShardID shard_id) {
  auto it1 = subscribers_.find(shard_id);
  if (it1 == subscribers_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot find subscriber for ShardID(%u)",
             shard_id);
    return nullptr;
  }
  return it1->second.get();
}

}  // namespace rocketspeed
