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

namespace rocketspeed {

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////
template <typename SubscriptionState>
SubscriptionsMap<SubscriptionState>::SubscriptionsMap(
    EventLoop* event_loop,
    DeliverCb deliver_cb,
    TerminateCb terminate_cb,
    UserDataCleanupCb user_data_cleanup_cb)
: event_loop_(event_loop)
, deliver_cb_(std::move(deliver_cb))
, terminate_cb_(std::move(terminate_cb))
, user_data_cleanup_cb_(std::move(user_data_cleanup_cb))
, pending_subscriptions_(event_loop)
, pending_unsubscribes_(event_loop) {
  auto flow_control = event_loop_->GetFlowControl();
  // Wire the source of pending subscriptions.
  flow_control->Register<typename Subscriptions::value_type>(
      &pending_subscriptions_,
      [this](Flow* flow, typename Subscriptions::value_type ptr) {
        using T
          = typename std::remove_pointer<decltype(ptr)>::type;
        // we own the pointer now
        HandlePendingSubscription(flow, std::unique_ptr<T>(ptr));
      });

  // Required by sparse_hash_set to mark deleted elements.
  // SubscriptionID() cannot be inserted from now on.
  pending_unsubscribes_.Modify(
    [this](Unsubscribes& set) {
      set.set_deleted_key(SubscriptionID());
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
SubscriptionsMap<SubscriptionState>::~SubscriptionsMap() {
  pending_subscriptions_.Modify([&](Subscriptions& map) {
    for (auto it = map.Begin(); it != map.End(); ++it) {
      CleanupSubscription(*it);
    }
  });

  for (auto it = synced_subscriptions_.Begin();
      it != synced_subscriptions_.End(); ++it) {
    CleanupSubscription(*it);
  }
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::Subscribe(
    SubscriptionID sub_id,
    TenantID tenant_id,
    const Slice& namespace_id,
    const Slice& topic_name,
    SequenceNumber initial_seqno,
    void* user_data) {
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
  SubscriptionState* state = new SubscriptionState(
            tenant_and_namespace, topic_name, sub_id, initial_seqno, user_data);
  pending_subscriptions_.Modify([&](Subscriptions& map) {
    auto inserted = map.emplace(sub_id, state);
    RS_ASSERT(inserted);
  });
  // Pending subscriptions will be synced opportunistically, as adding an
  // element renders the Source readable.
}

template <typename SubscriptionState>
SubscriptionState* SubscriptionsMap<SubscriptionState>::Find(
    SubscriptionID sub_id) const {
  LOG_DEBUG(GetLogger(), "Find(%llu)", sub_id.ForLogging());

  auto sync_it = synced_subscriptions_.Find(sub_id);
  if (sync_it != synced_subscriptions_.End()) {
    return *sync_it;
  }
  auto pend_it = pending_subscriptions_->Find(sub_id);
  if (pend_it != pending_subscriptions_->End()) {
    return *pend_it;
  }
  return nullptr;
}

template <typename SubscriptionState>
bool SubscriptionsMap<SubscriptionState>::Select(
    SubscriptionID sub_id, typename Info::Flags flags, Info* info) const {
  if (auto sub = Find(sub_id)) {
    if (flags & Info::kTenant) {
      info->SetTenant(sub->GetTenant());
    }
    if (flags & Info::kNamespace) {
      info->SetNamespace(sub->GetNamespace().ToString());
    }
    if (flags & Info::kTopic) {
      info->SetTopic(sub->GetTopicName().ToString());
    }
    if (flags & Info::kSequenceNumber) {
      info->SetSequenceNumber(sub->GetExpectedSeqno());
    }
    if (flags & Info::kUserData) {
      info->SetUserData(sub->GetUserData());
    }
    return true;
  }
  return false;
}


template <typename SubscriptionState>
bool SubscriptionsMap<SubscriptionState>::Exists(SubscriptionID sub_id) const {
  LOG_DEBUG(GetLogger(), "Exists(%llu)", sub_id.ForLogging());

  auto sync_it = synced_subscriptions_.Find(sub_id);
  if (sync_it != synced_subscriptions_.End()) {
    return true;
  }
  auto pend_it = pending_subscriptions_->Find(sub_id);
  if (pend_it != pending_subscriptions_->End()) {
    return true;
  }
  return false;
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::Rewind(SubscriptionID old_sub_id,
                                                 SubscriptionID new_sub_id,
                                                 SequenceNumber new_seqno) {
  auto ptr = Find(old_sub_id);
  RS_ASSERT(ptr);

  LOG_DEBUG(GetLogger(),
            "Rewind(%llu, %llu, %" PRIu64 ")",
            ptr->GetIDWhichMayChange().ForLogging(),
            new_sub_id.ForLogging(),
            new_seqno);

  RS_ASSERT(new_sub_id != old_sub_id);
  // Generate an unsubscibe message for the old ID.
  pending_unsubscribes_.Modify(
      [&](Unsubscribes& set) { set.insert(old_sub_id); });
  // Reinsert the subscription, as we may not change the
  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
    SubscriptionState* state = nullptr;
    {  // We have to remove the state before modifying it.
      auto sync_it = synced_subscriptions_.Find(old_sub_id);
      if (sync_it != synced_subscriptions_.End()) {
        state = *sync_it; // don't delete, it will be inserted to map
        synced_subscriptions_.erase(sync_it);
      } else {
        auto pend_it = pending_subscriptions.Find(old_sub_id);
        RS_ASSERT(pend_it != pending_subscriptions.End());
        state = *pend_it;
        pending_subscriptions.erase(pend_it);
      }
    }
    // Rewind the state.
    state->Rewind(new_sub_id, new_seqno);
    // Reinsert the subscription as pending one.
    auto inserted = pending_subscriptions.emplace(new_sub_id, state);
    RS_ASSERT(inserted);
  });
  // Terminate the subscription on old ID, the server sees rewound
  // subscription as a new one.
  pending_unsubscribes_.Modify(
      [&](Unsubscribes& set) { set.insert(old_sub_id); });
  // Pending subscribe and unsubscribe events will be synced opportunistically,
  // as adding an element renders the Sources readable.
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::Unsubscribe(SubscriptionID sub_id) {
  LOG_DEBUG(GetLogger(),
            "Unsubscribe(%llu)",
            sub_id.ForLogging());

  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
    auto sync_it = synced_subscriptions_.Find(sub_id);
    if (sync_it != synced_subscriptions_.End()) {
      CleanupSubscription(*sync_it);
      synced_subscriptions_.erase(sync_it);
      // Schedule an unsubscribe message to be sent only if a subscription has
      // been sent out.
      pending_unsubscribes_.Modify(
          [&](Unsubscribes& set) { set.insert(sub_id); });
    } else {
      auto pend_it = pending_subscriptions.Find(sub_id);
      RS_ASSERT(pend_it != pending_subscriptions.End());
      CleanupSubscription(*pend_it);
      pending_subscriptions.erase(pend_it);
    }
  });
  // Pending unsubscribe events will be synced opportunistically, as adding an
  // element renders the Source readable.
}

template <typename SubscriptionState>
bool SubscriptionsMap<SubscriptionState>::Empty() const {
  return synced_subscriptions_.Empty() && pending_subscriptions_->Empty();
}

template <typename SubscriptionState>
template <typename Iter>
void SubscriptionsMap<SubscriptionState>::Iterate(Iter&& iter) {
  class SubscriptionStateData : public SubscriptionData {
   public:
    explicit SubscriptionStateData(SubscriptionState* state) : state_(state) {}

    TenantID GetTenant() const override {
      return state_->GetTenant();
    }

    Slice GetNamespace() const override {
      return state_->GetNamespace();
    }

    Slice GetTopicName() const override {
      return state_->GetTopicName();
    }

    SequenceNumber GetExpectedSeqno() const override {
      return state_->GetExpectedSeqno();
    }

    SubscriptionID GetID() const override {
      return state_->GetIDWhichMayChange();
    }

   private:
    SubscriptionState* state_;
  };

  for (auto ptr : pending_subscriptions_.Read()) {
    const SubscriptionStateData data(ptr);
    iter(data);
  }
  for (auto ptr : synced_subscriptions_) {
    const SubscriptionStateData data(ptr);
    iter(data);
  }
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::SetUserData(SubscriptionID sub_id,
                                                      void* user_data) {
  auto sub = Find(sub_id);
  RS_ASSERT(sub);
  sub->SetUserData(user_data);
}

template <typename SubscriptionState>
Logger* SubscriptionsMap<SubscriptionState>::GetLogger() const {
  return event_loop_->GetLog().get();
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
  // We own the state pointer now.
  auto inserted = synced_subscriptions_.Insert(state.release());
  RS_ASSERT(inserted);
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
void SubscriptionsMap<SubscriptionState>::ConnectionCreated(
    std::unique_ptr<Sink<SharedTimestampedString>> sink) {

  sink_ = std::move(sink);

  // Make all subscriptions pending.
  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {

      // Most of the times, the set of pending subscriptions is orders of
      // magnitude smaller, swapping sets and moving elements from the one with
      // pending subscriptions to the former one would trigger less
      // reallocations and reduce peak memory usage.
      if (pending_subscriptions.Size() < synced_subscriptions_.Size()) {
        pending_subscriptions.Swap(synced_subscriptions_);
      }

      for (auto sub: synced_subscriptions_) {
        pending_subscriptions.emplace(sub->GetSubscriptionID(), sub);
      }
      synced_subscriptions_.Clear();
    });

  // All subscriptions have been implicitly unsubscribed when the stream
  // was closed.
  pending_unsubscribes_.Modify([&](Unsubscribes& set) { set.clear(); });

  // Enable sources as the sink is there.
  pending_subscriptions_.SetReadEnabled(event_loop_, true);
  pending_unsubscribes_.SetReadEnabled(event_loop_, true);
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::ConnectionDropped() {
  if (sink_) {
    auto flow_control = event_loop_->GetFlowControl();
    // Unwire and close the stream sink.
    flow_control->UnregisterSink(sink_.get());
    sink_.reset();
  }

  // Disable sources that point to the destroyed sink.
  pending_subscriptions_.SetReadEnabled(event_loop_, false);
  pending_unsubscribes_.SetReadEnabled(event_loop_, false);
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
      RS_ASSERT(pending_subscriptions_->Find(sub_id)
        == pending_subscriptions_->End());
      // Terminate the subscription.
      auto it = synced_subscriptions_.Find(sub_id);
      if (it != synced_subscriptions_.End()) {
        // No need to send unsubscribe request, as we've just received one.
        // Notify via callback.
        terminate_cb_(arg.flow, sub_id, std::move(arg.message));
        // Remove after callback so callback has chance to query final state.
        synced_subscriptions_.erase(it);
      } else {
        // A natural race between the server and the client terminating a
        // subscription.
        // State is null at this point, No need to call the terminate callback,
        // as the client has already Unsubscribed.
      }
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
  RS_ASSERT(pending_subscriptions_->Find(sub_id)
    == pending_subscriptions_->End());
  // Find the subscription.
  SubscriptionState* state = nullptr;
  auto it = synced_subscriptions_.Find(sub_id);
  if (it != synced_subscriptions_.End()) {
    state = *it; // don't delete since it stays in the map
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
  deliver_cb_(arg.flow, sub_id, std::move(arg.message));
}

template <typename SubscriptionState>
void SubscriptionsMap<SubscriptionState>::CleanupSubscription(
    SubscriptionState* sub) {
  if (user_data_cleanup_cb_) {
    user_data_cleanup_cb_(sub->GetUserData());
  }
  sub->SetUserData(nullptr);
  delete sub;
}

}  // namespace rocketspeed
