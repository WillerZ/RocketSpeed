/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "include/HostId.h"
#include "src/client/subscriptions_map.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/queues.h"
#include "src/messages/stream_allocator.h"
#include "src/messages/types.h"
#include "src/proxy2/abstract_worker.h"
#include "src/proxy2/multiplexer.h"
#include "src/util/id_allocator.h"

namespace rocketspeed {

class Counter;
class EventCallback;
class EventLoop;
class Message;
class Multiplexer;
class PerShard;
class PerStream;
class ProxyServerOptions;
class Statistics;
class Stream;
class UpstreamWorker;

/// The layer of UpstreamWorkers is sharded by the shard, to which the
/// destination topic belongs. All important proxy-related logic happens here:
/// * statistics collection,
/// * stream remapping,
/// * subscription termination and deduplication.
///
/// The worker is composed of the following pieces:
/// * UpstreamWorker -- routes messages according to stream assignment.
/// * PerStream -- detects hot topics and performs stream-level routing for
///                subscriptions on cold topics.
/// * PerShard -- handles obtaining and distributing shard routing information.
/// * Multiplexer -- deduplicates subscriptions on hot topics across all streams
///                  on one shard.
///
/// The worker's structure can be described by the following DAG:
///   UpstreamWorker
///   |
///   +---> PerStream         --+
///   |     |                   |
///   |     |                   |
///   |     +---> Multiplexer   } streams on one shard share the Multiplexer
///   |     |                   |
///   |     |                   |
///   +---> PerStream         --+
///   |
///   [...]
///
/// Messages received from ProxyServer's subscribers flow as follows:
/// DownstreamWorker -> UpstreamWorker -> PerStream -> {Stream, Multiplexer},
/// those received from the server, the proxy connects to, flow in the opposite
/// direction.
///
/// Worker's own memory requirements must be at most linear in the total number
/// of active streams.
class UpstreamWorker : public AbstractWorker {
 public:
  UpstreamWorker(const ProxyServerOptions& options,
                 EventLoop* event_loop,
                 const StreamAllocator::DivisionMapping& stream_to_id);

  EventLoop* GetLoop() const { return event_loop_; }
  const ProxyServerOptions& GetOptions() const { return options_; }

  void Start();

  void ReceiveFromQueue(Flow* flow,
                        size_t inbound_id,
                        MessageAndStream message) override;

  void ReceiveFromStream(Flow* flow,
                         PerStream* per_stream,
                         MessageAndStream message);

  ~UpstreamWorker();

 private:
  struct Stats {
    Counter* num_streams;
    Counter* num_shards;
  } stats_;

  const StreamAllocator::DivisionMapping stream_to_id_;

  std::unordered_map<StreamID, std::unique_ptr<PerStream>> streams_;
  std::unordered_map<size_t, std::unique_ptr<PerShard>> shard_cache_;

  void CleanupState(PerStream* per_stream);
};

/// A stream- and subscription-level proxy (per stream of subscriptions from a
/// client). Messages related to subscriptions on hot topics are handled by the
/// Multiplexer.
///
/// PerStream's memory requirements must be at most linear in the total number
/// of active subscriptions on hot topics.
class PerStream {
 public:
  explicit PerStream(UpstreamWorker* worker,
                     PerShard* per_shard,
                     StreamID downstream_id);

  EventLoop* GetLoop() const { return worker_->GetLoop(); }
  const ProxyServerOptions& GetOptions() const { return worker_->GetOptions(); }
  Statistics* GetStatistics() const { return worker_->GetStatistics(); }
  PerShard* GetShard() const { return per_shard_; }
  StreamID GetStream() const { return downstream_id_; }

  void ReceiveFromWorker(Flow* flow, MessageAndStream message);

  void ReceiveFromStream(Flow* flow, MessageAndStream message);

  void ReceiveFromMultiplexer(Flow* flow, MessageAndStream message);

  void ChangeRoute();

  ~PerStream();

 private:
  struct Stats {
    Counter* num_downstream_subscriptions;
  } stats_;

  UpstreamWorker* const worker_;
  PerShard* const per_shard_;
  const StreamID downstream_id_;

  /// A sink for messages on subscriptions that were not picked for
  /// multiplexing.
  std::unique_ptr<Stream> upstream_;
  /// A mapping from SubscriptionID of the original subscription to the handle
  /// of the upstream subscription.
  // TODO(stupaq): intrusive
  std::unordered_map<SubscriptionID, UpstreamSubscription*>
      downstream_to_upstream_;

  void CleanupState();

  /// Closes the stream, ensuring that both client and server receive goodbye
  /// messages and all local state is cleaned up.
  void ForceCloseStream();
};

/// Encapsulates logic and resources that are common to all PerStream objects on
/// the same shard.
///
/// PerShard's memory requirements must be at most linear in the total number
/// of PerStream objects that use it.
class PerShard {
 public:
  explicit PerShard(UpstreamWorker* worker, size_t shard_id);

  void AddPerStream(PerStream* per_stream);
  void RemovePerStream(PerStream* per_stream);

  EventLoop* GetLoop() const { return worker_->GetLoop(); }
  const ProxyServerOptions& GetOptions() const { return worker_->GetOptions(); }
  Statistics* GetStatistics() const { return worker_->GetStatistics(); }
  size_t GetShardID() const { return shard_id_; }
  const HostId& GetHost() const { return host_; }
  bool IsEmpty() const { return streams_on_shard_.empty(); }
  Multiplexer* GetMultiplexer() { return &multiplexer_; }

  ~PerShard();

 private:
  UpstreamWorker* const worker_;
  const size_t shard_id_;
  const std::unique_ptr<EventCallback> timer_;
  const std::shared_ptr<ShardingStrategy> router_;

  size_t router_version_;
  HostId host_;

  /// A set of streams on this shard.
  std::unordered_set<PerStream*> streams_on_shard_;

  /// Handles topic multiplexing.
  Multiplexer multiplexer_;

  /// Checks if router version has changed and handles router changes.
  void CheckRoutes();
};

}  // namespace rocketspeed
