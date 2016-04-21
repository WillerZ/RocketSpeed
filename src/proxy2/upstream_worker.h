/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "include/HostId.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/queues.h"
#include "src/messages/stream_allocator.h"
#include "src/proxy2/abstract_worker.h"

namespace rocketspeed {

class EventCallback;
class EventLoop;
class Message;
class PerShard;
class ProxyServerOptions;
class Stream;

/// The layer of UpstreamWorkers is sharded by the shard, to which the
/// destination topic belongs. All important proxy-related logic happens here:
/// * statistics collection,
/// * stream remapping,
/// * subscription termination and deduplication.
///
/// The worker's structure can be described by the following hierarchy:
///   UpstreamWorker -- routes subscriptions according to shard assignment.
///   |
///   +---> PerShard -- triages hot and cold topics, handles shard failover.
///   |     +---> Forwarder -- performs stream-level routing.
///   |     +---> Multiplexer -- deduplicates subscriptions on hot topics.
///   |
///   +---> PerShard
//    |     [...]
///   [...]
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

  void ReceiveFromQueue(Flow* flow,
                        size_t inbound_id,
                        MessageAndStream message) override;

  void ReceiveFromShard(Flow* flow,
                        PerShard* per_shard,
                        MessageAndStream message);

  ~UpstreamWorker();

 private:
  const StreamAllocator::DivisionMapping stream_to_id_;

  std::unordered_map<StreamID, PerShard*> stream_to_shard_;
  std::unordered_map<size_t, std::unique_ptr<PerShard>> shard_cache_;

  void CleanupState(PerShard* per_shard, StreamID stream_id);
};

/// A stream-level proxy (per shard of subscriptions).
///
/// Forwarder's memory requirements must be at most linear in the total number
/// of active streams (not subscriptions).
///
/// Forwarder is aware of two "kinds" of streams to/from subscribers or servers.
/// We call them "down(stream) streams" and "up(stream) streams" respectively.
class Forwarder {
 public:
  explicit Forwarder(PerShard* per_shard);

  EventLoop* GetLoop() const;
  const ProxyServerOptions& GetOptions() const;

  void Handle(Flow* flow, MessageAndStream message);

  void ReceiveFromStream(Flow* flow, MessageAndStream message);

  void ChangeRoute(const HostId& new_host);

  bool IsEmpty() const { return downstream_to_upstream_.empty(); }

  ~Forwarder();

 private:
  PerShard* const per_shard_;

  HostId host_;
  std::unordered_map<StreamID, std::unique_ptr<Stream>> downstream_to_upstream_;

  /// Closes the stream, ensuring that both client and server receive goodbye
  /// messages and all local state is cleaned up.
  void ForceCloseStream(StreamID downstream_id);

  void CleanupState(StreamID stream_id);
};

/// A subscription-level proxy (per shard of subscriptions).
///
/// Multiplexer's memory requirements may be linear in the total number of
/// active subscriptions it learns about.
class Multiplexer {
 public:
  explicit Multiplexer(PerShard* per_shard);

  EventLoop* GetLoop() const;
  const ProxyServerOptions& GetOptions() const;

  /// Returns true if the message was handled by Multiplexer and doesn't need
  /// to be handled by the Forwarder.
  ///
  /// It is the responsibility of the Multiplexer to filter out all messages
  /// that refer to active subscriptions it handles.
  bool TryHandle(Flow* flow, const MessageAndStream& message);

  void ChangeRoute(const HostId& new_host);

  bool IsEmpty() const {
    // FIXME
    return true;
  }

  ~Multiplexer();

 private:
  PerShard* const per_shard_;
};

/// Handles both kinds of proxying logic for a single shard of subscriptions.
///
/// PerShard's memory requirements must be at most linear in the total number
/// of active streams (not subscriptions).
class PerShard {
 public:
  explicit PerShard(UpstreamWorker* worker, size_t shard_id);

  EventLoop* GetLoop() const { return worker_->GetLoop(); }
  const ProxyServerOptions& GetOptions() const { return worker_->GetOptions(); }

  size_t GetShardID() const { return shard_id_; }

  void Handle(Flow* flow, MessageAndStream message);

  void ReceiveFromForwarder(Flow* flow, MessageAndStream message);

  bool IsEmpty() const {
    return forwarder_.IsEmpty() && multiplexer_.IsEmpty();
  }

  ~PerShard();

 private:
  UpstreamWorker* const worker_;
  const size_t shard_id_;
  const std::unique_ptr<EventCallback> timer_;
  const std::unique_ptr<SubscriptionRouter> router_;

  size_t router_version_;

  Multiplexer multiplexer_;
  Forwarder forwarder_;

  /// Checks if router version has changed and handles router changes.
  void CheckRoutes();
};

}  // namespace rocketspeed
