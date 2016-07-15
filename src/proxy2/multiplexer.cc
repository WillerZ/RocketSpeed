/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "multiplexer.h"

#include "external/folly/Memory.h"

#include "src/client/subscriptions_map.tcc"
#include "src/proxy2/upstream_worker.h"

namespace rocketspeed {

using namespace std::placeholders;

void UpstreamSubscription::AddDownstream(PerStream* per_stream,
                                         SubscriptionID downstream_sub,
                                         SequenceNumber initial_seqno) {
  // Bootstrap the downstream subscription to the same sequence number as the
  // upstream subscription, so we can simply forward any updates received on
  // the upstream subscription.
  auto callback = [&](const Slice& contents,
                      SequenceNumber prev_seqno,
                      SequenceNumber current_seqno) -> bool {
    // Prepare a message.
    MessageDeliverData data(GetTenant(), downstream_sub, MsgId(), contents);
    data.SetSequenceNumbers(prev_seqno, current_seqno);
    // Send the message on downstream subscription.
    SourcelessFlow no_flow(per_stream->GetLoop()->GetFlowControl());
    per_stream->ReceiveFromMultiplexer(
        &no_flow, {per_stream->GetStream(), Message::Copy(data)});
    // TODO(stupaq): flow control
    return true;
  };
  auto bootstrapped_to =
      accumulator_->BootstrapSubscription(initial_seqno, callback);
  RS_ASSERT(bootstrapped_to == 0 || GetExpectedSeqno() == 0 ||
            bootstrapped_to == GetExpectedSeqno());
  (void)bootstrapped_to;
  // Register the subscription after it's been bootstrapped to the same
  // sequence number as the upstream subscription.
  auto result = downstream_subscriptions_.emplace(per_stream, downstream_sub);
  RS_ASSERT(result.second);
  (void)result;
}

size_t UpstreamSubscription::RemoveDownstream(PerStream* per_stream,
                                              SubscriptionID downstream_sub) {
  auto pair = std::make_pair(per_stream, downstream_sub);
  auto result = downstream_subscriptions_.erase(pair);
  RS_ASSERT(result == 1);
  (void)result;
  return downstream_subscriptions_.size();
}

void UpstreamSubscription::ReceiveDeliver(
    PerShard* per_shard, Flow* flow, std::unique_ptr<MessageDeliver> deliver) {
  SequenceNumber received_seqno = deliver->GetSequenceNumber();
  if (expected_seqno_ != 0 && expected_seqno_ > received_seqno) {
    // Drop the message as no downsteam subscriber really needs it. We likely
    // received as a result of rewinding a subscription.
    LOG_DEBUG(per_shard->GetOptions().info_log,
              "Multiplexer(%zu)::ReceiveDeliver(%llu) expecting %" PRIu64
              " > received %" PRIu64,
              per_shard->GetShardID(),
              GetIDWhichMayChange().ForLogging(),
              expected_seqno_,
              received_seqno);
    return;
  }
  expected_seqno_ = received_seqno + 1;

  for (auto& entry : downstream_subscriptions_) {
    PerStream* per_stream = entry.first;
    StreamID downstream_id = per_stream->GetStream();
    SubscriptionID downstream_sub = entry.second;
    LOG_DEBUG(per_shard->GetOptions().info_log,
              "Multiplexer(%zu)::ReceiveDeliver(%llu) -> "
              "ReceiveFromMultiplexer(%llu, %llu)",
              per_shard->GetShardID(),
              GetIDWhichMayChange().ForLogging(),
              downstream_id,
              downstream_sub.ForLogging());

    deliver->SetSubID(downstream_sub);
    per_stream->ReceiveFromMultiplexer(
        flow, {per_stream->GetStream(), Message::Copy(*deliver)});
  }
}

void UpstreamSubscription::ReceiveTerminate(
    PerShard* per_shard,
    Flow* flow,
    std::unique_ptr<MessageUnsubscribe> unsubscribe) {
  for (auto& entry : downstream_subscriptions_) {
    PerStream* per_stream = entry.first;
    StreamID downstream_id = per_stream->GetStream();
    SubscriptionID downstream_sub = entry.second;
    LOG_DEBUG(per_shard->GetOptions().info_log,
              "Multiplexer(%zu)::ReceiveTerminate(%llu) -> "
              "ReceiveFromMultiplexer(%llu, %llu)",
              per_shard->GetShardID(),
              GetIDWhichMayChange().ForLogging(),
              downstream_id,
              downstream_sub.ForLogging());

    unsubscribe->SetSubID(downstream_sub);
    per_stream->ReceiveFromMultiplexer(
        flow, {per_stream->GetStream(), Message::Copy(*unsubscribe)});
  }
}

////////////////////////////////////////////////////////////////////////////////
Multiplexer::Multiplexer(PerShard* per_shard)
: per_shard_(per_shard)
, subscriptions_map_(
      GetLoop(),
      std::bind(&Multiplexer::ReceiveDeliver, this, _1, _2, _3),
      std::bind(&Multiplexer::ReceiveTerminate, this, _1, _2, _3))
, stream_supervisor_(GetLoop(), &subscriptions_map_,
                     GetOptions().backoff_strategy) {
  // Create stats.
  auto prefix = per_shard->GetOptions().stats_prefix + "multiplexer.";
  auto stats = per_shard->GetStatistics();
  stats_.num_upstream_subscriptions =
      stats->AddCounter(prefix + "num_upstream_subscriptions");
  // Connect to the server.
  stream_supervisor_.ConnectTo(per_shard_->GetHost());
  // Don't use the null SubscriptionID.
  auto null_id = upstream_allocator_.Next();
  RS_ASSERT(null_id == 0);
  (void)null_id;
}

EventLoop* Multiplexer::GetLoop() const {
  return per_shard_->GetLoop();
}

const ProxyServerOptions& Multiplexer::GetOptions() const {
  return per_shard_->GetOptions();
}

Statistics* Multiplexer::GetStatistics() const {
  return per_shard_->GetStatistics();
}

UpstreamSubscription* Multiplexer::Subscribe(Flow* flow,
                                             TenantID tenant_id,
                                             const Slice& namespace_id,
                                             const Slice& topic_name,
                                             SequenceNumber initial_seqno,
                                             PerStream* per_stream,
                                             SubscriptionID downstream_sub) {
  LOG_DEBUG(GetOptions().info_log,
            "Multiplexer(%zu)::Subscribe(%.*s, %.*s, %" PRIu64 ", %llu, %llu)",
            per_shard_->GetShardID(),
            static_cast<int>(namespace_id.size()),
            namespace_id.data(),
            static_cast<int>(topic_name.size()),
            topic_name.data(),
            initial_seqno,
            per_stream->GetStream(),
            downstream_sub.ForLogging());

  // Find a subscription on the topic, if one exists.
  UpstreamSubscription* upstream_sub;
  if (!(upstream_sub = FindInIndex(namespace_id, topic_name))) {
    RS_ASSERT(per_shard_->GetShardID() <= std::numeric_limits<ShardID>::max());
    auto upstream_id = SubscriptionID::ForShard(
                          static_cast<ShardID>(per_shard_->GetShardID()),
                          upstream_allocator_.Next());
    // This could fire if shard and hierarchical ID cannot be encoded in 8
    // bytes.
    RS_ASSERT(upstream_id);
    // Create an upstream subscription if one doesn't exist.
    upstream_sub = subscriptions_map_.Subscribe(
        upstream_id,
        tenant_id,
        namespace_id,
        topic_name,
        // TODO(stupaq): consider subscribing from initial_seqno instead of "0"
        // and rewinding to "0" if/when we receive a subscription at that seqno
        /* initial_seqno */ 0);
    // Assign updates accumulator (subscription bootstrapping).
    upstream_sub->SetAccumulator(
        GetOptions().accumulator(namespace_id, topic_name));
    // Insert into an index.
    InsertIntoIndex(upstream_sub);
  }
  // Add a downstream subscription, this may result in messages being delivered
  // on the downstream subscription due to subscription bootstrapping.
  upstream_sub->AddDownstream(per_stream, downstream_sub, initial_seqno);
  return upstream_sub;
}

void Multiplexer::Unsubscribe(Flow* flow,
                              UpstreamSubscription* upstream_sub,
                              PerStream* per_stream,
                              SubscriptionID downstream_sub) {
  LOG_DEBUG(GetOptions().info_log,
            "Multiplexer(%zu)::Unsubscribe(%llu, %llu, %llu)",
            per_shard_->GetShardID(),
            upstream_sub->GetIDWhichMayChange().ForLogging(),
            per_stream->GetStream(),
            downstream_sub.ForLogging());

  // Remove a downstream subscriber.
  auto remaining_downstream =
      upstream_sub->RemoveDownstream(per_stream, downstream_sub);
  // If we have no downstream subscriptions, kill upstream one.
  if (remaining_downstream == 0) {
    // Remove from index first.
    RemoveFromIndex(upstream_sub);
    // Remove the subscription state, that'd invalidate the pointer.
    subscriptions_map_.Unsubscribe(upstream_sub);
    upstream_sub = nullptr;
  }
}

void Multiplexer::ChangeRoute() {
  stream_supervisor_.ConnectTo(per_shard_->GetHost());
  // Topic to subscription index is unaffected as UpstreamSubscription objects
  // have stable pointers.
}

Multiplexer::~Multiplexer() = default;

UpstreamSubscription* Multiplexer::FindInIndex(const Slice& namespace_id,
                                               const Slice& topic_name) {
  auto key = std::make_pair(namespace_id.ToString(), topic_name.ToString());
  auto it = topic_index_.find(key);
  return it == topic_index_.end() ? nullptr : it->second;
}

void Multiplexer::InsertIntoIndex(UpstreamSubscription* upstream_sub) {
  auto key = std::make_pair(upstream_sub->GetNamespace().ToString(),
                            upstream_sub->GetTopicName().ToString());
  auto result = topic_index_.emplace(std::move(key), upstream_sub);
  (void)result;
  RS_ASSERT(result.second);
  stats_.num_upstream_subscriptions->Add(1);
}

void Multiplexer::RemoveFromIndex(UpstreamSubscription* upstream_sub) {
  auto key = std::make_pair(upstream_sub->GetNamespace().ToString(),
                            upstream_sub->GetTopicName().ToString());
  auto it = topic_index_.find(key);
  RS_ASSERT(it != topic_index_.end());
  if (it != topic_index_.end()) {
    topic_index_.erase(it);
    stats_.num_upstream_subscriptions->Add(-1);
  }
}

void Multiplexer::ReceiveDeliver(Flow* flow,
                                 UpstreamSubscription* upstream_sub,
                                 std::unique_ptr<MessageDeliver> deliver) {
  const auto type = deliver->GetMessageType();
  LOG_DEBUG(GetOptions().info_log,
            "Multiplexer(%zu)::ReceiveDeliver(%llu, %s)",
            per_shard_->GetShardID(),
            upstream_sub->GetIDWhichMayChange().ForLogging(),
            MessageTypeName(type));

  RS_ASSERT(type == MessageType::mDeliverGap ||
            type == MessageType::mDeliverData);

  // Update state in the accumulator
  if (type == MessageType::mDeliverData) {
    auto data = static_cast<MessageDeliverData*>(deliver.get());
    // Update the accumulator.
    auto action = upstream_sub->GetAccumulator()->ConsumeUpdate(
        data->GetPayload(),
        data->GetPrevSequenceNumber(),
        data->GetSequenceNumber());
    // Adjust the subscription based on an action.
    if (action == UpdatesAccumulator::Action::kResubscribeUpstream) {
      LOG_DEBUG(GetOptions().info_log,
                "Multiplexer(%zu)::ReceiveDeliver(%llu, %" PRIu64 ", %" PRIu64
                ") : resubscribing",
                per_shard_->GetShardID(),
                upstream_sub->GetIDWhichMayChange().ForLogging(),
                data->GetPrevSequenceNumber(),
                data->GetSequenceNumber());

      RS_ASSERT(per_shard_->GetShardID() <=
                std::numeric_limits<ShardID>::max());
      auto new_upstream_id = SubscriptionID::ForShard(
          static_cast<ShardID>(per_shard_->GetShardID()),
          upstream_allocator_.Next());
      // This could fire if shard and hierarchical ID cannot be encoded in 8
      // bytes.
      RS_ASSERT(new_upstream_id);
      // Rewind a subscription to zero.
      subscriptions_map_.Rewind(
          upstream_sub, new_upstream_id, 0 /* new_seqno */);
    }
  }

  // Broadcast delivery.
  upstream_sub->ReceiveDeliver(per_shard_, flow, std::move(deliver));
}

void Multiplexer::ReceiveTerminate(
    Flow* flow,
    UpstreamSubscription* upstream_sub,
    std::unique_ptr<MessageUnsubscribe> unsubscribe) {
  LOG_DEBUG(GetOptions().info_log,
            "Multiplexer(%zu)::ReceiveTerminate(%llu)",
            per_shard_->GetShardID(),
            upstream_sub->GetIDWhichMayChange().ForLogging());

  // The subscription has been removed from the map, so update an index.
  RemoveFromIndex(upstream_sub);

  // Broadcast termination.
  upstream_sub->ReceiveTerminate(per_shard_, flow, std::move(unsubscribe));
}

}  // namespace rocketspeed
