/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <functional>
#include <memory>

#include "RocketSpeed.h"
#include "Types.h"

namespace rocketspeed {

class HostId;
class HotnessDetector;
class Logger;
class ShardingStrategy;
class Slice;
class Status;
class StatisticsVisitor;
class SubscriptionResultState;
class UpdatesAccumulator;

/// A factory for UpdatesAccumulators.
using UpdatesAccumulatorFactory =
    std::function<std::unique_ptr<UpdatesAccumulator>(Slice namespace_id,
                                                      Slice topic_name)>;

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
  UpdatesAccumulatorFactory accumulator;

  /// Number of downstream threads.
  size_t num_downstream_threads{1};

  /// Number of upstream threads.
  size_t num_upstream_threads{1};

  /// A port to listen on.
  /// If null, the port will be chosen automatically.
  uint16_t port{0};

  /// A backoff strategy to follow when reattempting to synchronise multiplexed
  /// subscriptions after connection failure.
  /// Defaults to exponential backoff.
  BackOffStrategy backoff_strategy;

  /// Stats prefix.
  std::string stats_prefix;
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

  /// Walks over all statistics using the provided StatisticsVisitor.
  ///
  /// @param visitor Used to visit all statistics maintained by the client.
  virtual void ExportStatistics(StatisticsVisitor* visitor) const = 0;

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
  virtual bool IsHotTopic(Slice namespace_id, Slice topic_name) = 0;

  virtual ~HotnessDetector() = default;
};

/// Accumulates updates on an upstream subscription and bootstraps new
/// downstream subscribers to the current state of the subscription by sending
/// any missing deltas or snapshots.
///
/// Each upstream subscription on a hot topic has a unique UpdatesAccumulator
/// associated with it.
class UpdatesAccumulator {
 public:
  /// Creates a default accumulator.
  static std::unique_ptr<UpdatesAccumulator> CreateDefault(size_t count_limit);

  /// Invoked in-order for every update received from an upstream subscription.
  /// The Proxy adjusts the associated upstream subscription according to the
  /// Action.
  enum class Action {
    /// Do not adjust upstream subscription.
    kNoOp,
    /// Resubscribe the upstream subscription from SubscriptionNumber(0).
    kResubscribeUpstream,
  };
  virtual Action ConsumeUpdate(Slice contents,
                               SequenceNumber prev_seqno,
                               SequenceNumber current_seqno) = 0;

  /// Provides updates to be delivered on a downstream subscription by calling
  /// provided consumer callback arbitrary number of times.
  ///
  /// All calls to the callback must be synchronous and the callback may not be
  /// invoked after this method returns. It is guaranteed that the Slice
  /// provided to the consumer will not be accessed after the consumer returns.
  /// The callback returns false iff an update was scheduled for delivery, but
  /// no more updates shall be provided for now (flow control). If that is the
  /// case, the method shall promptly return the next sequence number expected
  /// on the subscription, assuming all updates provided thus far are delivered.
  ///
  /// Provided that no back-pressure has been exerted by the callback, this
  /// method must generate all updates necessary to bootstrap the downstream
  /// subscription to the same expected sequence number as the upstream
  /// subscription.
  using ConsumerCb = std::function<bool(
      Slice contents, SequenceNumber prev_seqno, SequenceNumber current_seqno)>;
  virtual SequenceNumber BootstrapSubscription(SequenceNumber initial_seqno,
                                               const ConsumerCb& consumer) = 0;

  virtual ~UpdatesAccumulator() = default;
};

}  // namespace rocketspeed
