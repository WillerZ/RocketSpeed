/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "include/Types.h"
#include "src/client/resilient_receiver.h"
#include "src/client/subscriptions_map.h"
#include "src/util/common/hash.h"
#include "src/util/id_allocator.h"

namespace rocketspeed {

class Counter;
class EventLoop;
class Flow;
class Multiplexer;
class PerShard;
class PerStream;
class ProxyServerOptions;
class Slice;
class Statistics;
class Stream;
class SubscriptionID;
class UpdatesAccumulator;

// TODO(stupaq):
/// As we already know the shard in the Multiplexer, we can just as well use <8
/// bytes for subscription IDs. Once we loop around the IDs we can:
/// * kill the Stream,
/// * reallocate SubscriptionIDs,
/// * resync all subscriptions using a new Stream.
class UpstreamAllocator : public IDAllocator<uint64_t, UpstreamAllocator> {
  using Base = IDAllocator<uint64_t, UpstreamAllocator>;

 public:
  using Base::Base;
};

class UpstreamSubscription {
 public:
  explicit UpstreamSubscription(TopicUUID uuid, SubscriptionID sub_id)
  : uuid_(std::move(uuid)), sub_id_(sub_id) {}

  UpstreamSubscription() = delete;

  using DownstreamSubscriptionsSet =
      std::unordered_set<std::pair<PerStream*, SubscriptionID>,
                         MurmurHash2<std::pair<PerStream*, SubscriptionID>>>;

  SubscriptionID GetSubID() const { return sub_id_; }

  const TopicUUID& GetTopicUUID() const { return uuid_; }

  void SetSubID(SubscriptionID sub_id) { sub_id_ = sub_id; }

  UpdatesAccumulator* GetAccumulator() const { return accumulator_.get(); }

  void SetAccumulator(std::unique_ptr<UpdatesAccumulator> accumulator) {
    RS_ASSERT(!accumulator_);
    accumulator_ = std::move(accumulator);
  }

  void AddDownstream(PerStream* per_stream,
                     SubscriptionID downstream_sub,
                     SequenceNumber initial_seqno,
                     TenantID tenant_id,
                     Slice namespace_id,
                     Slice topic_name,
                     SequenceNumber expected_seqno_check);

  size_t RemoveDownstream(PerStream* per_stream, SubscriptionID downstream_sub);

  void ReceiveDeliver(PerShard* per_shard,
                      Flow* flow,
                      std::unique_ptr<MessageDeliver> deliver);

  void ReceiveTerminate(PerShard* per_shard,
                        Flow* flow,
                        std::unique_ptr<MessageUnsubscribe> unsubscribe);

  SequenceNumber GetExpectedSeqno() const { return expected_seqno_; }

 private:
  TopicUUID uuid_;
  SubscriptionID sub_id_;
  std::unique_ptr<UpdatesAccumulator> accumulator_;
  SequenceNumber expected_seqno_{0};
  // TODO(stupaq): optimise for small cardinality
  DownstreamSubscriptionsSet downstream_subscriptions_;
};

/// A subscription-level proxy (per stream of subscriptions).
///
/// Multiplexer's memory requirements may be linear in the total number of
/// active subscriptions it learns about.
class Multiplexer : public ConnectionAwareReceiver {
 public:
  explicit Multiplexer(PerShard* per_shard, IntroProperties stream_properties);

  EventLoop* GetLoop() const;
  const ProxyServerOptions& GetOptions() const;
  Statistics* GetStatistics() const;

  const IntroProperties& GetProperties() const { return stream_properties_; }

  /// Handles a subscription that was chosen for multiplexing.
  ///
  /// Returned handle to the subscription state is valid until matching
  /// unsubscribe call.
  UpstreamSubscription* Subscribe(Flow* flow,
                                  TenantID tenant_id,
                                  const Slice& namespace_id,
                                  const Slice& topic_name,
                                  SequenceNumber initial_seqno,
                                  PerStream* per_stream,
                                  SubscriptionID downstream_sub);

  void Unsubscribe(Flow* flow,
                   UpstreamSubscription* upstream_sub,
                   PerStream* per_stream,
                   SubscriptionID downstream_sub);

  void ChangeRoute();

  ~Multiplexer();

 private:
  struct Stats {
    Counter* num_upstream_subscriptions;
  } stats_;

  PerShard* const per_shard_;

  UpstreamAllocator upstream_allocator_;
  SubscriptionsMap subscriptions_map_;
  ResilientStreamReceiver stream_supervisor_;

  const IntroProperties stream_properties_;

  // TODO(stupaq): intrusive
  std::unordered_map<std::pair<std::string, std::string>,
                     UpstreamSubscription*,
                     MurmurHash2<std::pair<std::string, std::string>>>
      topic_index_;

  UpstreamSubscription* GetUpstreamSubscription(const TopicUUID& uuid);

  UpstreamSubscription* FindInIndex(NamespaceID namespace_id, Topic topic_name);

  void InsertIntoIndex(NamespaceID namespace_id,
                       Topic topic_name,
                       UpstreamSubscription* upstream_sub);

  void RemoveFromIndex(NamespaceID namespace_id, Topic topic_name);

  void ReceiveConnectionStatus(bool isHealthy);

  void ConnectionChanged() final override;
  void ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe>) final override;
  void ReceiveDeliver(StreamReceiveArg<MessageDeliver>) final override;
  void ReceiveSubAck(StreamReceiveArg<MessageSubAck>) final override;

  void SendMessage(
      Flow* flow,
      SubscriptionsMap::ReplicaIndex replica,
      std::unique_ptr<Message> message);
};

}  // namespace rocketspeed
