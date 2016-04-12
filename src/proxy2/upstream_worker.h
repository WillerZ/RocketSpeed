/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "include/RocketSpeed.h"
#include "src/messages/event_loop.h"
#include "src/messages/queues.h"
#include "src/messages/stream_allocator.h"
#include "src/messages/types.h"
#include "src/proxy2/abstract_worker.h"
#include "src/util/common/flow_control.h"

namespace rocketspeed {

class EventCallback;
class EventLoop;
class Message;
class ProxyServerOptions;
class Stream;
class SubscriberIf;
class SubscriberStats;

/// The layer of UpstreamWorkers is sharded by the shard the destination topic
/// belongs to. All important proxy-related logic happens here:
/// * statistics collection,
/// * stream remapping,
/// * subscription termination and deduplication.
///
/// The worker conceptually consists of two independent pieces that handle
/// disjoint subsets of inbound subscriptions:
/// * Forwarder, which acts as a stream-level proxy,
/// * Multiplexer, which acts as a subscription-level proxy.
///
/// The worker itself is stateless, although each piece, Forwarder and
/// Multiplexer, may maintain internal state.
class UpstreamWorker : public AbstractWorker {
 public:
  UpstreamWorker(const ProxyServerOptions& options,
                 EventLoop* event_loop,
                 const StreamAllocator::DivisionMapping& stream_to_id);

  void ReceiveFromQueue(Flow* flow,
                        size_t inbound_id,
                        MessageAndStream message) override;

  ~UpstreamWorker();

 private:
  /// A stream-level proxy.
  ///
  /// Forwarder::PerShard handles actual proxying logic. This class
  /// (de)multiplexes shards and encapsulates boilerplate code.
  ///
  /// Forwarder's memory requirements must be at most linear in the total number
  /// of streams (not subscriptions).
  class Forwarder {
    class PerShard;

   public:
    explicit Forwarder(UpstreamWorker* worker,
                       const StreamAllocator::DivisionMapping& stream_to_id);

    void Handle(Flow* flow, MessageAndStream message);

    void ReceiveFromShard(Flow* flow,
                          PerShard* per_shard,
                          MessageAndStream message);

   private:
    UpstreamWorker* const worker_;
    const StreamAllocator::DivisionMapping stream_to_id_;

    std::unordered_map<StreamID, PerShard*> stream_to_shard_;
    std::unordered_map<size_t, std::unique_ptr<PerShard>> shard_cache_;

    void CleanupState(PerShard* per_shard, StreamID stream_id);
  } forwarder_;

  /// A subscription-level proxy.
  ///
  /// Multiplexer's memory requirements may be linear in the total number of
  /// active subscriptions it learns about.
  class Multiplexer {
   public:
    explicit Multiplexer(UpstreamWorker* worker);

    /// Returns true if the message was handled by Multiplexer and doesn't need
    /// to be handled by the Forwarder.
    ///
    /// It is the responsibility of the Multiplexer to filter out all messages
    /// that refer to active subscriptions it handles.
    bool TryHandle(Flow* flow, const MessageAndStream& message);

    ~Multiplexer();

   private:
    UpstreamWorker* const worker_;
    const std::shared_ptr<SubscriberStats> subscriber_stats_;
    ClientOptions subscriber_opts_;
    std::unique_ptr<SubscriberIf> subscriber_;
  } multiplexer_;
};

/// A Forwarder that acts on a single shard.
///
/// PerShard is aware of two "kinds" of streams to/from subscribers or servers.
/// We call them "down(stream) streams" and "up(stream) streams" respectively.
class UpstreamWorker::Forwarder::PerShard : public StreamReceiver {
 public:
  explicit PerShard(Forwarder* forwarder, size_t shard_id);

  void Handle(Flow* flow, MessageAndStream message);

  void ReceiveFromStream(Flow* flow, MessageAndStream message);

  bool HasActiveStreams() const { return !downstream_to_upstream_.empty(); }

  size_t GetShardID() const { return shard_id_; }

  ~PerShard();

 private:
  Forwarder* const forwarder_;
  const size_t shard_id_;
  const std::unique_ptr<EventCallback> timer_;
  const std::unique_ptr<SubscriptionRouter> router_;

  std::unordered_map<StreamID, std::unique_ptr<Stream>> downstream_to_upstream_;
  size_t router_version_;

  /// Checks if router version has changed and handles router changes.
  void CheckRoutes();

  /// Closes the stream, ensuring that both client and server receive goodbye
  /// messages and all local state is cleaned up.
  void ForceCloseStream(StreamID downstream_id);

  void CleanupState(StreamID stream_id);
};

}  // namespace rocketspeed
