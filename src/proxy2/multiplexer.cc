/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "multiplexer.h"

#include "src/client/subscriptions_map.h"
#include "src/proxy2/upstream_worker.h"

namespace rocketspeed {

using namespace std::placeholders;

void UpstreamSubscription::AddDownstream(PerStream* per_stream,
                                         SubscriptionID downstream_sub,
                                         SequenceNumber initial_seqno,
                                         TenantID tenant_id,
                                         Slice namespace_id,
                                         Slice topic_name,
                                         SequenceNumber expected_seqno_check) {
  // Bootstrap the downstream subscription to the same sequence number as the
  // upstream subscription, so we can simply forward any updates received on
  // the upstream subscription.
  auto callback = [&](const Slice& contents,
                      SequenceNumber prev_seqno,
                      SequenceNumber current_seqno) -> bool {
    // Prepare a message.
    MessageDeliverData data(
        tenant_id, namespace_id.ToString(), topic_name.ToString(),
        downstream_sub, MsgId(), contents.ToString());
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
  RS_ASSERT(bootstrapped_to == 0 || expected_seqno_check == 0 ||
            bootstrapped_to == expected_seqno_check);
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
  const auto sub_id = deliver->GetSubID();
  SequenceNumber received_seqno = deliver->GetSequenceNumber();
  if (expected_seqno_ != 0 && expected_seqno_ > received_seqno) {
    // Drop the message as no downsteam subscriber really needs it. We likely
    // received as a result of rewinding a subscription.
    LOG_DEBUG(per_shard->GetOptions().info_log,
              "Multiplexer(%zu)::ReceiveDeliver(%llu) expecting %" PRIu64
              " > received %" PRIu64,
              per_shard->GetShardID(),
              sub_id.ForLogging(),
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
              sub_id.ForLogging(),
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
  const auto sub_id = unsubscribe->GetSubID();
  for (auto& entry : downstream_subscriptions_) {
    PerStream* per_stream = entry.first;
    StreamID downstream_id = per_stream->GetStream();
    SubscriptionID downstream_sub = entry.second;
    LOG_DEBUG(per_shard->GetOptions().info_log,
              "Multiplexer(%zu)::ReceiveTerminate(%llu) -> "
              "ReceiveFromMultiplexer(%llu, %llu)",
              per_shard->GetShardID(),
              sub_id.ForLogging(),
              downstream_id,
              downstream_sub.ForLogging());

    unsubscribe->SetSubID(downstream_sub);
    per_stream->ReceiveFromMultiplexer(
        flow, {per_stream->GetStream(), Message::Copy(*unsubscribe)});
  }
}

////////////////////////////////////////////////////////////////////////////////
namespace {
void UserDataCleanup(void* user_data) {
  delete static_cast<UpstreamSubscription*>(user_data);
}
}

Multiplexer::Multiplexer(PerShard* per_shard)
: per_shard_(per_shard)
, subscriptions_map_(GetLoop(),
                     std::bind(&Multiplexer::SendMessage, this, _1, _2),
                     &UserDataCleanup)
, stream_supervisor_(GetLoop(),
                     this,
                     std::bind(&Multiplexer::ReceiveConnectionStatus, this, _1),
                     GetOptions().backoff_strategy,
                     GetOptions().max_silent_reconnects) {
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

void Multiplexer::SendMessage(Flow* flow, std::unique_ptr<Message> message) {
  flow->Write(GetConnection(), message);
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
  UpstreamSubscription* upstream_sub =
      FindInIndex(namespace_id.ToString(), topic_name.ToString());
  if (!upstream_sub) {
    RS_ASSERT(per_shard_->GetShardID() <= std::numeric_limits<ShardID>::max());
    SubscriptionID upstream_id = SubscriptionID::ForShard(
                          static_cast<ShardID>(per_shard_->GetShardID()),
                          upstream_allocator_.Next());
    // This could fire if shard and hierarchical ID cannot be encoded in 8
    // bytes.
    RS_ASSERT(upstream_id);

    upstream_sub = new UpstreamSubscription(upstream_id);
    upstream_sub->SetAccumulator(
        GetOptions().accumulator(namespace_id, topic_name));
    // Create an upstream subscription if one doesn't exist.
    subscriptions_map_.Subscribe(
        upstream_id,
        tenant_id,
        namespace_id,
        topic_name,
        // TODO(stupaq): consider subscribing from initial_seqno instead of "0"
        // and rewinding to "0" if/when we receive a subscription at that seqno
        /* initial_seqno */ 0,
        static_cast<void*>(upstream_sub));
    // Assign updates accumulator (subscription bootstrapping).

    // Insert into an index.
    InsertIntoIndex(
        namespace_id.ToString(), topic_name.ToString(), upstream_sub);
  }
  // Add a downstream subscription, this may result in messages being delivered
  // on the downstream subscription due to subscription bootstrapping.
  // expected_seqno_check is only used for assertion.
  auto expected_seqno_check = upstream_sub->GetExpectedSeqno();
  upstream_sub->AddDownstream(
      per_stream, downstream_sub, initial_seqno, tenant_id,
      namespace_id, topic_name, expected_seqno_check);
  return upstream_sub;
}

void Multiplexer::Unsubscribe(Flow* flow,
                              UpstreamSubscription* upstream_sub,
                              PerStream* per_stream,
                              SubscriptionID downstream_sub) {
  const auto sub_id = upstream_sub->GetSubID();
  LOG_DEBUG(GetOptions().info_log,
            "Multiplexer(%zu)::Unsubscribe(%llu, %llu, %llu)",
            per_shard_->GetShardID(),
            sub_id.ForLogging(),
            per_stream->GetStream(),
            downstream_sub.ForLogging());

  // Remove a downstream subscriber.
  auto remaining_downstream =
      upstream_sub->RemoveDownstream(per_stream, downstream_sub);
  // If we have no downstream subscriptions, kill upstream one.
  if (remaining_downstream == 0) {
    // Find the topic and namespace.
    using Info = decltype(subscriptions_map_)::Info;
    Info info;
    Info::Flags flags = Info::kTopic | Info::kNamespace;
    bool success = subscriptions_map_.Select(sub_id, flags, &info);
    RS_ASSERT(success);
    // Remove from index first.
    RemoveFromIndex(info.GetNamespace(), info.GetTopic());
    // Remove the subscription state, that'd invalidate the pointer.
    subscriptions_map_.Unsubscribe(sub_id);
    upstream_sub = nullptr;
  }
}

void Multiplexer::ChangeRoute() {
  stream_supervisor_.ConnectTo(per_shard_->GetHost());
  // Topic to subscription index is unaffected as UpstreamSubscription objects
  // have stable pointers.
}

Multiplexer::~Multiplexer() = default;

UpstreamSubscription* Multiplexer::GetUpstreamSubscription(
    SubscriptionID sub_id) {
  using Info = decltype(subscriptions_map_)::Info;
  Info info;
  bool success = subscriptions_map_.Select(sub_id, Info::kUserData, &info);
  RS_ASSERT(success);
  return static_cast<UpstreamSubscription*>(info.GetUserData());
}

UpstreamSubscription* Multiplexer::FindInIndex(NamespaceID namespace_id,
                                        Topic topic_name) {
  auto key = std::make_pair(std::move(namespace_id), std::move(topic_name));
  auto it = topic_index_.find(key);
  return it == topic_index_.end() ? nullptr : it->second;
}

void Multiplexer::InsertIntoIndex(NamespaceID namespace_id,
                                  Topic topic_name,
                                  UpstreamSubscription* upstream_sub) {
  auto key = std::make_pair(std::move(namespace_id), std::move(topic_name));
  auto result = topic_index_.emplace(std::move(key), upstream_sub);
  (void)result;
  RS_ASSERT(result.second);
  stats_.num_upstream_subscriptions->Add(1);
}

void Multiplexer::RemoveFromIndex(NamespaceID namespace_id, Topic topic_name) {
  auto key = std::make_pair(std::move(namespace_id), std::move(topic_name));
  auto it = topic_index_.find(key);
  RS_ASSERT(it != topic_index_.end());
  if (it != topic_index_.end()) {
    topic_index_.erase(it);
    stats_.num_upstream_subscriptions->Add(-1);
  }
}

void Multiplexer::ReceiveConnectionStatus(bool isHealthy) {
  // TODO(gds): what should happen here?
}

void Multiplexer::ConnectionChanged() {
  if (GetConnection()) {
    subscriptions_map_.StartSync();
  } else {
    subscriptions_map_.StopSync();
  }
}

void Multiplexer::ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe> arg) {
  auto upstream_sub = arg.message->GetSubID();

  LOG_DEBUG(GetOptions().info_log,
            "ReceiveUnsubscribe(%llu, %llu, %d)",
            arg.stream_id,
            upstream_sub.ForLogging(),
            static_cast<int>(arg.message->GetMessageType()));

  using Info = decltype(subscriptions_map_)::Info;
  Info info;
  Info::Flags flags = Info::kTopic | Info::kNamespace;
  if (!subscriptions_map_.ProcessUnsubscribe(
      arg.flow, *arg.message, flags, &info)) {
    // Message didn't match a subscription.
    return;
  }

  LOG_DEBUG(GetOptions().info_log,
            "Multiplexer(%zu)::ReceiveTerminate(%llu)",
            per_shard_->GetShardID(),
            upstream_sub.ForLogging());

  // The subscription has been removed from the map, so update an index.
  RemoveFromIndex(info.GetNamespace(), info.GetTopic());

  // Broadcast termination.
  GetUpstreamSubscription(upstream_sub)->ReceiveTerminate(
      per_shard_, arg.flow, std::move(arg.message));
}

void Multiplexer::ReceiveDeliver(StreamReceiveArg<MessageDeliver> arg) {
  auto flow = arg.flow;
  auto& deliver = arg.message;
  auto upstream_sub = deliver->GetSubID();
  const auto type = deliver->GetMessageType();

  LOG_DEBUG(GetOptions().info_log,
            "ReceiveDeliver(%llu, %llu, %s)",
            arg.stream_id,
            upstream_sub.ForLogging(),
            MessageTypeName(type));

  if (subscriptions_map_.ProcessDeliver(arg.flow, *deliver)) {
    // Message was processed by a subscription.
    LOG_DEBUG(GetOptions().info_log,
              "Multiplexer(%zu)::ReceiveDeliver(%llu, %s)",
              per_shard_->GetShardID(),
              upstream_sub.ForLogging(),
              MessageTypeName(type));

    RS_ASSERT(type == MessageType::mDeliverGap ||
              type == MessageType::mDeliverData);

    // Update state in the accumulator
    UpstreamSubscription* sub = GetUpstreamSubscription(upstream_sub);
    if (type == MessageType::mDeliverData) {
      auto data = static_cast<MessageDeliverData*>(deliver.get());
      // Update the accumulator.
      auto action = sub->GetAccumulator()->ConsumeUpdate(
          data->GetPayload(),
          data->GetPrevSequenceNumber(),
          data->GetSequenceNumber());
      // Adjust the subscription based on an action.
      if (action == UpdatesAccumulator::Action::kResubscribeUpstream) {
        LOG_DEBUG(GetOptions().info_log,
                  "Multiplexer(%zu)::ReceiveDeliver(%llu, %" PRIu64 ", %" PRIu64
                  ") : resubscribing",
                  per_shard_->GetShardID(),
                  upstream_sub.ForLogging(),
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
        // Update subscription.
        sub->SetSubID(new_upstream_id);
      }
    }

    // Broadcast delivery.
    sub->ReceiveDeliver(per_shard_, flow, std::move(deliver));
  }
}

}  // namespace rocketspeed
