/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "subscriptions_map.h"

#include <cinttypes>
#include <cstddef>
#include <memory>

#include "include/Assert.h"
#include "include/Logger.h"
#include "include/Slice.h"
#include "include/Types.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/messages.h"
#include "src/messages/stream.h"
#include "src/util/common/flow.h"
#include <xxhash.h>

namespace rocketspeed {

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////
SubscriptionBase::~SubscriptionBase() {
  // We set user_data_ to null on deletion. This is a sanity check to ensure
  // that we are always calling the user data cleanup callback, and not leaking
  // user data.
  RS_ASSERT(user_data_ == nullptr);
}

bool SubscriptionBase::ProcessUpdate(Logger* info_log,
                                     const SequenceNumber previous,
                                     Cursor current) {
  RS_ASSERT_DBG(current.seqno >= previous);
  if (current.seqno < previous) {
    LOG_ERROR(info_log,
              "Message has sequence numbers out of order "
              "(%" PRIu64 ", %s)",
              previous,
              current.ToString().c_str());
    return false;
  }

  // TODO(pja): Do state checks per source.
  if ((current.seqno < previous) /* this should never happen */ ||
      (expected_.seqno == 0 &&
       previous != 0) /* must receive a snapshot if expected */ ||
      (expected_.seqno > current.seqno) /* must not go back in time */ ||
      (expected_.seqno < previous) /* must not skip an update */) {
    LOG_WARN(info_log,
             "SubscriptionBase(%llu, %s, %s)::ProcessUpdate(%" PRIu64
             ", %s) expected %s, dropped",
             GetIDWhichMayChange().ForLogging(),
             tenant_and_namespace_.Get().namespace_id.c_str(),
             topic_name_.c_str(),
             previous,
             current.ToString().c_str(),
             expected_.ToString().c_str());
    return false;
  } else {
    LOG_DEBUG(info_log,
              "SubscriptionBase(%llu, %s, %s)::ProcessUpdate(%" PRIu64
              ", %s) expected %s, accepted",
              GetIDWhichMayChange().ForLogging(),
              tenant_and_namespace_.Get().namespace_id.c_str(),
              topic_name_.c_str(),
              previous,
              current.ToString().c_str(),
              expected_.ToString().c_str());
    // We now expect the next sequence number.
    // TODO(pja): For now, the source is ignored, but we just keep the
    // last received source. Comparisons should eventually be per source.
    expected_ = std::move(current);
    expected_.seqno++;
    return true;
  }
}

////////////////////////////////////////////////////////////////////////////////
SubscriptionsMap::SubscriptionsMap(
    EventLoop* event_loop,
    std::function<void(Flow*, std::unique_ptr<Message>)> message_handler,
    UserDataCleanupCb user_data_cleanup_cb)
: event_loop_(event_loop)
, user_data_cleanup_cb_(std::move(user_data_cleanup_cb))
, pending_subscriptions_(event_loop, "pending_subs")
, pending_unsubscribes_(event_loop, "pending_unsubs")
, message_handler_(std::move(message_handler)) {
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

  // Wire the source of unsubscribe events.
  flow_control->Register<Subscription>(
      &pending_unsubscribes_,
      std::bind(&SubscriptionsMap::HandlePendingUnsubscription, this, _1, _2));

  // Disable sources that point to non-existent sink.
  pending_subscriptions_.SetReadEnabled(event_loop_, false);
  pending_unsubscribes_.SetReadEnabled(event_loop_, false);
}

SubscriptionsMap::~SubscriptionsMap() {
  pending_subscriptions_.Modify([&](Subscriptions& map) {
    for (auto it = map.Begin(); it != map.End(); ++it) {
      CleanupSubscription(*it);
    }
  });

  for (auto it = synced_subscriptions_.Begin();
      it != synced_subscriptions_.End(); ++it) {
    CleanupSubscription(*it);
  }

  auto flow_control = event_loop_->GetFlowControl();
  flow_control->UnregisterSource(&pending_unsubscribes_);
  flow_control->UnregisterSource(&pending_subscriptions_);
}

void SubscriptionsMap::Subscribe(
    SubscriptionID sub_id,
    TenantID tenant_id,
    const Slice& namespace_id,
    const Slice& topic_name,
    const CursorVector& start,
    void* user_data) {
  // TODO(pja) : Only supporting a single source at a time for now.
  RS_ASSERT(start.size() == 1);

  LOG_DEBUG(GetLogger(),
            "Subscribe(%llu, %u, %s, %s, %s)",
            sub_id.ForLogging(),
            tenant_id,
            namespace_id.ToString().c_str(),
            topic_name.ToString().c_str(),
            start[0].ToString().c_str());

  auto tenant_and_namespace = tenant_and_namespace_factory_.GetFlyweight(
      {tenant_id, namespace_id.ToString()});
  // Record the subscription, we check that the ID is unique among all active
  // subscriptions.
  SubscriptionBase* state = new SubscriptionBase(
            tenant_and_namespace, topic_name, sub_id, start[0], user_data);
  pending_subscriptions_.Modify([&](Subscriptions& map) {
    auto inserted = map.emplace(sub_id, state);
    RS_ASSERT(inserted);
  });
  // Pending subscriptions will be synced opportunistically, as adding an
  // element renders the Source readable.
}

SubscriptionBase* SubscriptionsMap::Find(
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

bool SubscriptionsMap::Select(
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
    if (flags & Info::kCursor) {
      info->SetCursor(sub->GetExpected());
    }
    if (flags & Info::kUserData) {
      info->SetUserData(sub->GetUserData());
    }
    return true;
  }
  return false;
}


bool SubscriptionsMap::Exists(SubscriptionID sub_id) const {
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

bool SubscriptionsMap::IsSynced(SubscriptionID sub_id) const {
  return synced_subscriptions_.Find(sub_id) != synced_subscriptions_.end();
}

void SubscriptionsMap::Rewind(SubscriptionID old_sub_id,
                              SubscriptionID new_sub_id,
                              Cursor new_cursor) {
  auto ptr = Find(old_sub_id);
  RS_ASSERT(ptr);

  LOG_DEBUG(GetLogger(),
            "Rewind(%llu, %llu, %s)",
            ptr->GetIDWhichMayChange().ForLogging(),
            new_sub_id.ForLogging(),
            new_cursor.ToString().c_str());

  RS_ASSERT(new_sub_id != old_sub_id);

  // Reinsert the subscription, as we may not change the
  NamespaceID namespace_id;
  Topic topic;
  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
    SubscriptionBase* state = nullptr;
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
    state->Rewind(new_sub_id, std::move(new_cursor));
    namespace_id = state->GetNamespace().ToString();
    topic = state->GetTopicName().ToString();

    // Reinsert the subscription as pending one.
    auto inserted = pending_subscriptions.emplace(new_sub_id, state);
    RS_ASSERT(inserted);
  });
  // Terminate the subscription on old ID, the server sees rewound
  // subscription as a new one.
  pending_unsubscribes_.Modify([&](Unsubscribes& set) {
    set.emplace(old_sub_id, std::move(namespace_id), std::move(topic));
  });
  // Pending subscribe and unsubscribe events will be synced opportunistically,
  // as adding an element renders the Sources readable.
}

void SubscriptionsMap::Unsubscribe(SubscriptionID sub_id) {
  LOG_DEBUG(GetLogger(),
            "Unsubscribe(%llu)",
            sub_id.ForLogging());

  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
    auto sync_it = synced_subscriptions_.Find(sub_id);
    if (sync_it != synced_subscriptions_.End()) {
      NamespaceID namespace_id = (*sync_it)->GetNamespace().ToString();
      Topic topic = (*sync_it)->GetTopicName().ToString();
      CleanupSubscription(*sync_it);
      synced_subscriptions_.erase(sync_it);
      // Schedule an unsubscribe message to be sent only if a subscription has
      // been sent out.
      pending_unsubscribes_.Modify([&](Unsubscribes& set) {
        set.emplace(sub_id, std::move(namespace_id), std::move(topic));
      });
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

bool SubscriptionsMap::Empty() const {
  return synced_subscriptions_.Empty() && pending_subscriptions_->Empty();
}

void SubscriptionsMap::SetUserData(SubscriptionID sub_id, void* user_data) {
  auto sub = Find(sub_id);
  RS_ASSERT(sub);
  sub->SetUserData(user_data);
}

Logger* SubscriptionsMap::GetLogger() const {
  return event_loop_->GetLog().get();
}

void SubscriptionsMap::HandlePendingSubscription(
    Flow* flow, std::unique_ptr<SubscriptionBase> state) {
  LOG_DEBUG(GetLogger(),
            "HandlePendingSubscription(%llu)",
            state->GetIDWhichMayChange().ForLogging());

  // Send a message.
  std::unique_ptr<Message> subscribe(new MessageSubscribe(
      state->GetTenant(),
      state->GetNamespace().ToString(),
      state->GetTopicName().ToString(),
      {state->GetExpected()},
      state->GetSubscriptionID()));
  message_handler_(flow, std::move(subscribe));

  // Mark the subscription as synced.
  // We own the state pointer now.
  auto inserted = synced_subscriptions_.Insert(state.release());
  RS_ASSERT(inserted);
}

void SubscriptionsMap::HandlePendingUnsubscription(
    Flow* flow, Subscription sub) {
  LOG_DEBUG(
      GetLogger(), "HandlePendingUnsubscription(%llu)",
      sub.sub_id.ForLogging());

  // Send the message.
  std::unique_ptr<Message> unsubscribe(new MessageUnsubscribe(
      GuestTenant,
      std::move(sub.namespace_id),
      std::move(sub.topic),
      sub.sub_id,
      MessageUnsubscribe::Reason::kRequested));
  message_handler_(flow, std::move(unsubscribe));
}

void SubscriptionsMap::CleanupSubscription(
    SubscriptionBase* sub) {
  if (user_data_cleanup_cb_) {
    user_data_cleanup_cb_(sub->GetUserData());
  }
  sub->SetUserData(nullptr);
  delete sub;
}

void SubscriptionsMap::StartSync() {
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

void SubscriptionsMap::StopSync() {
  // Disable sources that point to the destroyed sink.
  pending_subscriptions_.SetReadEnabled(event_loop_, false);
  pending_unsubscribes_.SetReadEnabled(event_loop_, false);
}

bool SubscriptionsMap::ProcessUnsubscribe(
    Flow* flow,
    const MessageUnsubscribe& message,
    Info::Flags flags,
    Info* info) {
  auto sub_id = message.GetSubID();
  auto reason = message.GetReason();

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
        Select(sub_id, flags, info);
        synced_subscriptions_.erase(it);
        return true;
      } else {
        // A natural race between the server and the client terminating a
        // subscription.
        // State is null at this point, No need to call the terminate callback,
        // as the client has already Unsubscribed.
      }
    } break;
  }
  return false;
}

bool SubscriptionsMap::ProcessDeliver(
    Flow* flow, const MessageDeliver& message) {
  auto sub_id = message.GetSubID();

  // Sanity check that the message did not refer to a subscription that has
  // not been synced to the server.
  RS_ASSERT(pending_subscriptions_->Find(sub_id)
    == pending_subscriptions_->End());
  // Find the subscription.
  SubscriptionBase* state = nullptr;
  auto it = synced_subscriptions_.Find(sub_id);
  if (it != synced_subscriptions_.End()) {
    state = *it; // don't delete since it stays in the map
  } else {
    // A natural race between the server delivering a message and the client
    // terminating a subscription.
    return false;
  }
  // Update the state.
  if (!state->ProcessUpdate(event_loop_->GetLog().get(),
                            message.GetPrevSequenceNumber(),
                            Cursor(message.GetDataSource().ToString(),
                                   message.GetSequenceNumber()))) {
    // Drop the update.
    return false;
  }
  // Deliver.
  return true;
}

size_t SubscriptionsMap::Subscription::Hash::operator()(
    const Subscription& sub) const {
  const uint64_t seed = 0xf92601a646f1828fULL;
  XXH64_stateSpace_t state;
  XXH64_resetState(&state, seed);
  XXH64_update(&state, &sub.sub_id, sizeof(sub.sub_id));
  XXH64_update(&state, sub.namespace_id.data(), sub.namespace_id.size());
  XXH64_update(&state, sub.topic.data(), sub.topic.size());
  return XXH64_intermediateDigest(&state);
}

}  // namespace rocketspeed
