/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "subscriptions_map.h"

#include <cstddef>
#include <memory>

#include "external/folly/Memory.h"

#include "include/Assert.h"
#include "include/Logger.h"
#include "include/Slice.h"
#include "include/Types.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/messages.h"
#include "src/messages/stream.h"
#include "src/util/common/flow.h"
#include "src/util/common/random.h"

namespace rocketspeed {

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////
template <typename SubscriptionState>
SubscriptionsMap<SubscriptionState>::SubscriptionsMap(
    EventLoop* event_loop,
    DeliverCb deliver_cb,
    TerminateCb terminate_cb,
    BackOffStrategy backoff_strategy)
: event_loop_(event_loop)
, deliver_cb_(std::move(deliver_cb))
, terminate_cb_(std::move(terminate_cb))
, backoff_strategy_(std::move(backoff_strategy))
, pending_subscriptions_(event_loop)
, pending_unsubscribes_(event_loop) {
  auto flow_control = event_loop_->GetFlowControl();
  // Wire the source of pending subscriptions.
  flow_control->Register<typename Subscriptions::value_type>(
      &pending_subscriptions_,
      [this](Flow* flow, typename Subscriptions::value_type entry) {
        HandlePendingSubscription(flow, std::move(entry.second));
      });

  // Wire the source of unsubscribe events.
  flow_control->Register<SubscriptionID>(
      &pending_unsubscribes_,
      std::bind(&SubscriptionsMap::HandlePendingUnsubscription, this, _1, _2));

  // Disable sources that point to non-existent sink.
  pending_subscriptions_.SetReadEnabled(event_loop_, false);
  pending_unsubscribes_.SetReadEnabled(event_loop_, false);
}

template <typename SubscriptionState>
SubscriptionState* SubscriptionsMap<SubscriptionState>::Subscribe(
    SubscriptionID sub_id,
    TenantID tenant_id,
    const Slice& namespace_id,
    const Slice& topic_name,
    SequenceNumber initial_seqno) {
  LOG_DEBUG(GetLogger(),
            "Subscribe(%llu, %u, %s, %s, %" PRIu64 ")",
            sub_id.ForLogging(),
            tenant_id,
            namespace_id.ToString().c_str(),
            topic_name.ToString().c_str(),
            initial_seqno);

  auto tenant_and_namespace = tenant_and_namespace_factory_.GetFlyweight(
      {tenant_id, namespace_id.ToString()});
  // Record the subscription, we check that the ID is unique among all active
  // subscriptions.
  SubscriptionState* state;
  pending_subscriptions_.Modify([&](Subscriptions& map) {
    auto result = map.emplace(
        sub_id,
        folly::make_unique<SubscriptionState>(
            tenant_and_namespace, topic_name, sub_id, initial_seqno));
    RS_ASSERT(result.second);
    state = result.first->second.get();
  });
  // Pending subscriptions will be synced opportunistically, as adding an
  // element renders the Source readable.
  return state;
}

template <typename SubscriptionState>
SubscriptionState* SubscriptionsMap<SubscriptionState>::Find(
    SubscriptionID sub_id) const {
  LOG_DEBUG(GetLogger(), "Find(%llu)", sub_id.ForLogging());

  auto it = synced_subscriptions_.find(sub_id);
  if (it != synced_subscriptions_.end()) {
    return it->second.get();
  }
  it = pending_subscriptions_->find(sub_id);
  if (it != pending_subscriptions_->end()) {
    return it->second.get();
  }
  return nullptr;
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::Rewind(SubscriptionState* ptr,
                                                 SubscriptionID new_sub_id,
                                                 SequenceNumber new_seqno) {
  LOG_DEBUG(GetLogger(),
            "Rewind(%llu, %llu, %" PRIu64 ")",
            ptr->GetIDWhichMayChange().ForLogging(),
            new_sub_id.ForLogging(),
            new_seqno);

  auto old_sub_id = ptr->GetSubscriptionID();
  RS_ASSERT(new_sub_id != old_sub_id);
  // Generate an unsubscibe message for the old ID.
  pending_unsubscribes_.Modify(
      [&](Unsubscribes& set) { set.emplace(old_sub_id); });
  // Reinsert the subscription, as we may not change the
  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
    std::unique_ptr<SubscriptionState> state;
    {  // We have to remove the state before modifying it.
      auto it = synced_subscriptions_.find(old_sub_id);
      if (it != synced_subscriptions_.end()) {
        state = std::move(it->second);
        synced_subscriptions_.erase(it);
      } else {
        it = pending_subscriptions.find(old_sub_id);
        if (it != pending_subscriptions.end()) {
          state = std::move(it->second);
          pending_subscriptions.erase(it);
        } else {
          RS_ASSERT(false);
          return;
        }
      }
    }
    // Rewind the state.
    state->Rewind(new_sub_id, new_seqno);
    // Reinsert the subscription as pending one.
    auto result = pending_subscriptions.emplace(new_sub_id, std::move(state));
    RS_ASSERT(result.second);
    (void)result;
  });
  // Terminate the subscription on old ID, the server sees rewound
  // subscription as a new one.
  pending_unsubscribes_.Modify(
      [&](Unsubscribes& set) { set.emplace(old_sub_id); });
  // Pending subscribe and unsubscribe events will be synced opportunistically,
  // as adding an element renders the Sources readable.
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::Unsubscribe(SubscriptionState* ptr) {
  LOG_DEBUG(GetLogger(),
            "Unsubscribe(%llu)",
            ptr->GetIDWhichMayChange().ForLogging());

  auto sub_id = ptr->GetSubscriptionID();
  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
    if (synced_subscriptions_.erase(sub_id) > 0) {
      // Schedule an unsubscribe message to be sent only if a subscription has
      // been sent out.
      pending_unsubscribes_.Modify(
          [&](Unsubscribes& set) { set.emplace(sub_id); });
    } else if (pending_subscriptions.erase(sub_id) == 0) {
      // We probably corrupted memory when dereferencing `state`
      // anyway...
      RS_ASSERT(false);
    }
  });
  // Pending unsubscribe events will be synced opportunistically, as adding an
  // element renders the Source readable.
}

template <typename SubscriptionState>
bool SubscriptionsMap<SubscriptionState>::Empty() const {
  return synced_subscriptions_.empty() && pending_subscriptions_->empty();
}

template <typename SubscriptionState>
template <typename Iter>
void SubscriptionsMap<SubscriptionState>::Iterate(Iter&& iter) {
  for (auto& entry : pending_subscriptions_.Read()) {
    iter(entry.second.get());
  }
  for (auto& entry : synced_subscriptions_) {
    iter(entry.second.get());
  }
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::ReconnectTo(const HostId& host) {
  if (current_host_ == host) {
    return;
  }
  LOG_INFO(GetLogger(), "ReconnectTo(%s)", host.ToString().c_str());

  connection_failues_ = 0;
  current_host_ = host;
  Reconnect();
}

template <typename SubscriptionState>
Logger* SubscriptionsMap<SubscriptionState>::GetLogger() const {
  return event_loop_->GetLog().get();
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::Reconnect() {
  // Think about this method as something that can be called in almost arbitrary
  // state and should leave the SubscriptionsMap in a perfectly valid state.
  // Code over here should be rather defensive. Corner cases should be severely
  // underrepresented.

  // Kill any scheduled reconnection.
  backoff_timer_.reset();

  if (sink_) {
    auto flow_control = event_loop_->GetFlowControl();
    // Unwire and close the stream sink.
    flow_control->UnregisterSink(sink_.get());
    sink_.reset();
  }

  // Disable sources that point to the destroyed sink.
  pending_subscriptions_.SetReadEnabled(event_loop_, false);
  pending_unsubscribes_.SetReadEnabled(event_loop_, false);

  // If no remote host is known, we're done. The procedure will be repeated
  // after router update.
  if (!current_host_) {
    LOG_INFO(GetLogger(), "Reconnect(): unknown host");
    return;
  }

  auto reconnect = [this]() {
    RS_ASSERT(!sink_);
    RS_ASSERT(current_host_);

    // Open the stream.
    auto stream = event_loop_->OpenStream(current_host_);
    stream->SetReceiver(this);
    LOG_INFO(GetLogger(),
             "Reconnect()::reconnect(%s, %lld)",
             current_host_.ToString().c_str(),
             stream->GetLocalID());

    sink_ = std::move(stream);

    // Make all subscriptions pending.
    pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
      // Most of the times, the set of pending subscriptions is orders of
      // magnitude smaller, swapping sets and moving elements from the one with
      // pending subscriptions to the former one would trigger less
      // reallocations and reduce peak memory usage.
      if (pending_subscriptions.size() < synced_subscriptions_.size()) {
        std::swap(pending_subscriptions, synced_subscriptions_);
      }
      std::move(
          synced_subscriptions_.begin(),
          synced_subscriptions_.end(),
          std::inserter(pending_subscriptions, pending_subscriptions.begin()));
      RS_ASSERT(synced_subscriptions_.empty());
    });

    // All subscriptions have been implicitly unsubscribed when the stream
    // was closed.
    pending_unsubscribes_.Modify([&](Unsubscribes& set) { set.clear(); });

    // Enable sources as the sink is there.
    pending_subscriptions_.SetReadEnabled(event_loop_, true);
    pending_unsubscribes_.SetReadEnabled(event_loop_, true);

    // Kill the backoff timer, if set.
    backoff_timer_.reset();
  };

  // We do not apply backoff if there have been no recorded connection failure
  // to this host.
  if (connection_failues_ == 0) {
    reconnect();
  } else {
    RS_ASSERT(connection_failues_ > 0);
    // Figure out the backoff.
    auto backoff_duration =
        backoff_strategy_(&ThreadLocalPRNG(), connection_failues_ - 1);

    LOG_INFO(GetLogger(),
             "Reconnect()::backoff(%zu, %zd)",
             connection_failues_,
             backoff_duration.count());

    // Schedule asynchronous creation of a new stream.
    RS_ASSERT(!backoff_timer_);
    backoff_timer_ =
        event_loop_->CreateTimedEventCallback(reconnect, backoff_duration);
    backoff_timer_->Enable();
  }
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::HandlePendingSubscription(
    Flow* flow, std::unique_ptr<SubscriptionState> state) {
  LOG_DEBUG(GetLogger(),
            "HandlePendingSubscription(%llu)",
            state->GetIDWhichMayChange().ForLogging());

  RS_ASSERT(sink_);
  // Send a message.
  MessageSubscribe subscribe(state->GetTenant(),
                             state->GetNamespace(),
                             state->GetTopicName(),
                             state->GetExpectedSeqno(),
                             state->GetSubscriptionID());
  auto ts = Stream::ToTimestampedString(subscribe);
  flow->Write(sink_.get(), ts);

  // Mark the subscription as synced.
  auto result = synced_subscriptions_.emplace(state->GetSubscriptionID(),
                                              std::move(state));
  (void)result;
  RS_ASSERT(result.second);
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::HandlePendingUnsubscription(
    Flow* flow, SubscriptionID sub_id) {
  LOG_DEBUG(
      GetLogger(), "HandlePendingUnsubscription(%llu)", sub_id.ForLogging());

  RS_ASSERT(sink_);
  // Send the message.
  MessageUnsubscribe unsubscribe(
      GuestTenant, sub_id, MessageUnsubscribe::Reason::kRequested);
  auto ts = Stream::ToTimestampedString(unsubscribe);
  flow->Write(sink_.get(), ts);
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::ReceiveGoodbye(
    StreamReceiveArg<MessageGoodbye> arg) {
  LOG_DEBUG(GetLogger(), "ReceiveGoodbye(%llu)", arg.stream_id);

  ++connection_failues_;
  Reconnect();
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::ReceiveUnsubscribe(
    StreamReceiveArg<MessageUnsubscribe> arg) {
  auto sub_id = arg.message->GetSubID();
  auto reason = arg.message->GetReason();
  LOG_DEBUG(GetLogger(),
            "ReceiveUnsubscribe(%llu, %llu, %d)",
            arg.stream_id,
            sub_id.ForLogging(),
            static_cast<int>(arg.message->GetMessageType()));

  switch (reason) {
    case MessageUnsubscribe::Reason::kInvalid:
    case MessageUnsubscribe::Reason::kRequested: {
      // Sanity check that the message did not refer to a subscription that has
      // not been synced to the server.
      RS_ASSERT(pending_subscriptions_->count(sub_id) == 0);
      // Terminate the subscription.
      std::unique_ptr<SubscriptionState> state;
      auto it = synced_subscriptions_.find(sub_id);
      if (it != synced_subscriptions_.end()) {
        state = std::move(it->second);
        synced_subscriptions_.erase(it);
        // No need to send unsubscribe request, as we've just received one.
      } else {
        // A natural race between the server and the client terminating a
        // subscription.
      }
      // Notify via callback.
      terminate_cb_(arg.flow, state.get(), std::move(arg.message));
    } break;
  }
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::ReceiveDeliver(
    StreamReceiveArg<MessageDeliver> arg) {
  auto sub_id = arg.message->GetSubID();
  LOG_DEBUG(GetLogger(),
            "ReceiveDeliver(%llu, %llu, %s)",
            arg.stream_id,
            sub_id.ForLogging(),
            MessageTypeName(arg.message->GetMessageType()));

  // Sanity check that the message did not refer to a subscription that has
  // not been synced to the server.
  RS_ASSERT(pending_subscriptions_->count(sub_id) == 0);
  // Find the subscription.
  SubscriptionState* state;
  auto it = synced_subscriptions_.find(sub_id);
  if (it != synced_subscriptions_.end()) {
    state = it->second.get();
  } else {
    // A natural race between the server delivering a message and the client
    // terminating a subscription.
    return;
  }
  // Update the state.
  if (!state->ProcessUpdate(event_loop_->GetLog().get(),
                            arg.message->GetPrevSequenceNumber(),
                            arg.message->GetSequenceNumber())) {
    // Drop the update.
    return;
  }
  // Deliver.
  deliver_cb_(arg.flow, state, std::move(arg.message));
}

}  // namespace rocketspeed
