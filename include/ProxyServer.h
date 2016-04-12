/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <memory>

namespace rocketspeed {

class HostId;
class Logger;
class ShardingStrategy;
class Slice;
class Status;
class SubscriptionResultState;
class TopicCollapsingStrategy;

class ProxyServerOptions {
 public:
  ~ProxyServerOptions();

  /// Logger for info messages.
  std::shared_ptr<Logger> info_log;

  /// Sharding and routing configuration for subscriptions.
  std::shared_ptr<ShardingStrategy> routing;

  /// A strategy that tells which topics are worth collapsing.
  std::shared_ptr<TopicCollapsingStrategy> collapsing;

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
class TopicCollapsingStrategy {
 public:
  virtual bool ShouldCollapse(const Slice& namespace_id,
                              const Slice& topic_name) = 0;

  virtual ~TopicCollapsingStrategy() = default;
};

}  // namespace rocketspeed
