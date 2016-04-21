/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <functional>
#include <memory>

#include "Types.h"

namespace rocketspeed {

class HostId;
class HotnessDetector;
class Logger;
class ShardingStrategy;
class Slice;
class Status;
class SubscriptionResultState;
class UpdatesAccumulator;

/// A factory for UpdatesAccumulators.
using UpdatesAccumulatorFactory =
    std::function<std::unique_ptr<UpdatesAccumulator>(const Slice& namespace_id,
                                                      const Slice& topic_name)>;

class ProxyServerOptions {
 public:
  ~ProxyServerOptions();

  /// Logger for info messages.
  std::shared_ptr<Logger> info_log;

  /// Sharding and routing configuration for subscriptions.
  std::shared_ptr<ShardingStrategy> routing;

  /// A strategy that tells which topics are considered "hot".
  std::shared_ptr<HotnessDetector> hot_topics;

  /// Provides a strategy that bootstraps new subscribers to the current state
  /// of an upstream subscription.
  std::shared_ptr<UpdatesAccumulatorFactory> accumulator;

  /// Number of downstream threads.
  size_t num_downstream_threads{1};

  /// Number of upstream threads.
  size_t num_upstream_threads{1};

  /// A port to listen on.
  /// IF null, the port will be chosen automatically.
  uint16_t port{0};
};

/// A server that acts as a proxy between Subscribers and Rocketeers.
///
/// The proxy makes the following assumptions about Subscriber's and Rocketeer's
/// operation.
/// 1. Subscriber shall never mix multiple shards, as defined by the
///   ShardingStrategy, in a single protocol stream.
class ProxyServer {
 public:
  static Status Create(ProxyServerOptions options,
                       std::unique_ptr<ProxyServer>* out);

  virtual const HostId& GetListenerAddress() const = 0;

  virtual ~ProxyServer() = 0;
};

/// A strategy which tells whether subscriptions on given topic shall be
/// collapsed by the proxy.
///
/// Proxy does not need to store metadata for subscriptions which are not
/// collapsed. At the same time, all subscriptions which are not collapsed by
/// the proxy must be handled by the destination RocketeerServer. Therefore, it
/// makes sense to collapse only popular topics.
class HotnessDetector {
 public:
  virtual bool IsHotTopic(const Slice& namespace_id,
                          const Slice& topic_name) = 0;

  virtual ~HotnessDetector() = default;
};

/// Accumulates updates on a subscription and bootstraps new subscribers to the
/// current state of the subscription by sending any missing deltas or
/// snapshots.
///
/// Each upstream subscription on a hot topic has a unique UpdatesAccumulator
/// associated with it.
class UpdatesAccumulator {
 public:
  /// Creates a default accumulator.
  static std::unique_ptr<UpdatesAccumulator> CreateDefault();

  /// Invoked in-order for every update received from an upstream subscription.
  /// The Proxy adjusts the associated upstream subscription according to the
  /// Action.
  enum class Action {
    /// Do not adjust upstream subscription.
    kNoOp,
    /// Resubscribe the upstream subscription from SubscriptionNumber(0).
    kResubscribeUpstream,
  };
  virtual Action ConsumeUpdate(const Slice& contents,
                               SequenceNumber prev_seqno,
                               SequenceNumber current_seqno) = 0;

  /// Provides updates to be delivered on a subscription by calling provided
  /// consumer callback arbitrary number of times.
  ///
  /// All calls to the callback must be synchronous and the callback may not be
  /// invoked after this method returns. It is guaranteed that the Slice
  /// provided to the consumer will not be accessed after the consumer returns.
  virtual void BootstrapSubscription(
      SequenceNumber current_seqno,
      std::function<void(const Slice& contents,
                         SequenceNumber prev_seqno,
                         SequenceNumber current_seqno)> consumer) = 0;

  virtual ~UpdatesAccumulator() = default;
};

}  // namespace rocketspeed
