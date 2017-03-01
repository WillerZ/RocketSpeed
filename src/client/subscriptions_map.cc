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
                                     Cursor current) {
  // TODO(pja): Do state checks per source.
  if (expected_.source == current.source &&
      expected_.seqno > current.seqno /* must not go back in time */) {
    LOG_WARN(info_log,
             "SubscriptionBase(%llu, %s, %s)::ProcessUpdate("
             "%s) expected %s, dropped",
             GetIDWhichMayChange().ForLogging(),
             GetNamespace().ToString().c_str(),
             GetTopicName().ToString().c_str(),
             current.ToString().c_str(),
             expected_.ToString().c_str());
    return false;
  }

  LOG_DEBUG(info_log,
            "SubscriptionBase(%llu, %s, %s)::ProcessUpdate("
            "%s)",
            GetIDWhichMayChange().ForLogging(),
            GetNamespace().ToString().c_str(),
            GetTopicName().ToString().c_str(),
            current.ToString().c_str());
  // We now expect the next sequence number.
  // TODO(pja): For now, the source is ignored, but we just keep the
  // last received source. Comparisons should eventually be per source.
  expected_ = std::move(current);
  expected_.seqno++;
  return true;
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
  flow_control->Register<Unsubscribes::value_type>(
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

  for (auto it = pending_ack_subscriptions_.Begin();
      it != pending_ack_subscriptions_.End(); ++it) {
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

  // Record the subscription.
  // If we have pending sub on same topic, we should override it (new sub_id).
  // If we have pending unsubs on same topic, we should leave it (diff sub_id).
  // If we have synced sub on same topic, we should unsubscribe it.

  TopicUUID key(namespace_id, topic_name);

  // Unsubscribe any already synced sub ID.
  Unsubscribe(key);

  // Add new subscription.
  SubscriptionBase* state = new SubscriptionBase(
            tenant_id, namespace_id, topic_name, sub_id, start[0], user_data);
  pending_subscriptions_.Modify([&](Subscriptions& map) {
    map.emplace(std::move(key), state);
  });

  // Pending subscriptions will be synced opportunistically, as adding an
  // element renders the Source readable.
}

SubscriptionBase* SubscriptionsMap::Find(const SubscriptionKey& key) const {
  LOG_DEBUG(GetLogger(), "Find(%s)", key.ToString().c_str());

  auto sync_it = synced_subscriptions_.Find(key);
  if (sync_it != synced_subscriptions_.End()) {
    return *sync_it;
  }
  auto ack_it = pending_ack_subscriptions_.Find(key);
  if (ack_it != pending_ack_subscriptions_.End()) {
    return *ack_it;
  }
  auto pend_it = pending_subscriptions_->Find(key);
  if (pend_it != pending_subscriptions_->End()) {
    return *pend_it;
  }
  return nullptr;
}

bool SubscriptionsMap::Select(
    const SubscriptionKey& key, typename Info::Flags flags, Info* info) const {
  if (auto sub = Find(key)) {
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
    if (flags & Info::kSubID) {
      info->SetSubID(sub->GetSubscriptionID());
    }
    return true;
  }
  return false;
}


bool SubscriptionsMap::Exists(const SubscriptionKey& key) const {
  LOG_DEBUG(GetLogger(), "Exists(%s)", key.ToString().c_str());

  return Find(key) != nullptr;
}

bool SubscriptionsMap::IsSent(const SubscriptionKey& key) const {
  return synced_subscriptions_.Find(key) != synced_subscriptions_.end() ||
    pending_ack_subscriptions_.Find(key) != pending_ack_subscriptions_.end();
}

void SubscriptionsMap::Rewind(const SubscriptionKey& key,
                              SubscriptionID new_sub_id,
                              Cursor new_cursor) {
  auto ptr = Find(key);
  RS_ASSERT(ptr);
  auto old_sub_id = ptr->GetIDWhichMayChange();

  LOG_DEBUG(GetLogger(),
            "Rewind(%s, %llu, %s)",
            key.ToString().c_str(),
            new_sub_id.ForLogging(),
            new_cursor.ToString().c_str());

  RS_ASSERT(new_sub_id != old_sub_id);

  // Reinsert the subscription, as we may not change the
  NamespaceID namespace_id;
  Topic topic;
  key.GetTopicID(&namespace_id, &topic);
  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
    SubscriptionBase* state = nullptr;
    {  // We have to remove the state before modifying it.
      auto sync_it = synced_subscriptions_.Find(key);
      if (sync_it != synced_subscriptions_.End()) {
        state = *sync_it; // don't delete, it will be inserted to map
        synced_subscriptions_.erase(sync_it);
      } else {
        auto ack_it = pending_ack_subscriptions_.Find(key);
        if (ack_it != pending_ack_subscriptions_.End()) {
          state = *ack_it; // don't delete, it will be inserted to map
          pending_ack_subscriptions_.erase(ack_it);
        } else {
          auto pend_it = pending_subscriptions.Find(key);
          RS_ASSERT(pend_it != pending_subscriptions.End());
          state = *pend_it;
          pending_subscriptions.erase(pend_it);
        }
      }
    }
    // Rewind the state.
    state->Rewind(new_sub_id, std::move(new_cursor));

    // Reinsert the subscription as pending one.
    auto inserted = pending_subscriptions.emplace(key, state);
    RS_ASSERT(inserted);
  });
  // Terminate the subscription on old ID, the server sees rewound
  // subscription as a new one.
  pending_unsubscribes_.Modify([&](Unsubscribes& set) {
    set[key].push_back(old_sub_id);
  });
  // Pending subscribe and unsubscribe events will be synced opportunistically,
  // as adding an element renders the Sources readable.
}

void SubscriptionsMap::Unsubscribe(const SubscriptionKey& key) {
  LOG_DEBUG(GetLogger(),
            "Unsubscribe(%s)",
            key.ToString().c_str());

  {
    auto sync_it = synced_subscriptions_.Find(key);
    if (sync_it != synced_subscriptions_.End()) {
      auto sub_id = (*sync_it)->GetSubscriptionID();
      CleanupSubscription(*sync_it);
      synced_subscriptions_.erase(sync_it);
      // Schedule an unsubscribe message to be sent only if a subscription has
      // been sent out.
      pending_unsubscribes_.Modify([&](Unsubscribes& set) {
          set[key].push_back(sub_id);
        });

      return;
    }
  }

  {
    auto pend_it = pending_ack_subscriptions_.Find(key);
    if (pend_it != pending_ack_subscriptions_.End()) {
      auto sub_id = (*pend_it)->GetSubscriptionID();
      CleanupSubscription(*pend_it);
      pending_ack_subscriptions_.erase(pend_it);
      // Schedule an unsubscribe message to be sent only if a subscription has
      // been sent out.
      pending_unsubscribes_.Modify([&](Unsubscribes& set) {
          set[key].push_back(sub_id);
        });

      return;
    }
  }

  // Pending unsubscribe events will be synced opportunistically, as adding an
  // element renders the Source readable.
  pending_subscriptions_.Modify([&](Subscriptions& pending_subscriptions) {
      {
        auto pend_it = pending_subscriptions.Find(key);
        if (pend_it != pending_subscriptions.End()) {
          CleanupSubscription(*pend_it);
          pending_subscriptions.erase(pend_it);
        }
      }
  });
}

bool SubscriptionsMap::Empty() const {
  return synced_subscriptions_.Empty() &&
    pending_ack_subscriptions_.Empty() &&
    pending_subscriptions_->Empty();
}

void SubscriptionsMap::SetUserData(
    const SubscriptionKey& key, void* user_data) {
  auto sub = Find(key);
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

  // If we have any pending unsubscribes then we need to send those first
  // so that the server doesn't see two subscriptions on the same topic for
  // this stream.
  TopicUUID key(state->GetNamespace(), state->GetTopicName());
  {
    auto it = pending_unsubscribes_->find(key);
    if (it != pending_unsubscribes_->end()) {
      HandlePendingUnsubscription(flow, *it);
      pending_unsubscribes_.Modify([&] (Unsubscribes& map) {
          map.erase(it);
        });
    }
  }

  CursorVector start = {state->GetExpected()};

  // Send a message.
  std::unique_ptr<Message> subscribe(new MessageSubscribe(
      state->GetTenant(),
      state->GetNamespace().ToString(),
      state->GetTopicName().ToString(),
      std::move(start),
      state->GetSubscriptionID()));
  message_handler_(flow, std::move(subscribe));

  // Mark the subscription as synced.
  // We own the state pointer now.
  auto inserted = pending_ack_subscriptions_.Insert(state.release());
  RS_ASSERT(inserted);
}

void SubscriptionsMap::HandlePendingUnsubscription(
    Flow* flow, Unsubscribes::value_type subs) {
  // Send the message.
  NamespaceID namespace_id;
  Topic topic;
  subs.first.GetTopicID(&namespace_id, &topic);
  for (SubscriptionID sub_id : subs.second) {
    LOG_DEBUG(
      GetLogger(), "HandlePendingUnsubscription(%llu, %s, %s)",
      sub_id.ForLogging(),
      namespace_id.c_str(),
      topic.c_str());

    std::unique_ptr<Message> unsubscribe(new MessageUnsubscribe(
        GuestTenant,
        namespace_id,
        topic,
        sub_id,
        MessageUnsubscribe::Reason::kRequested));
    message_handler_(flow, std::move(unsubscribe));
  }
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

      for (auto sub : synced_subscriptions_) {
        pending_subscriptions.emplace(sub->GetKey(), sub);
      }
      for (auto sub : pending_ack_subscriptions_) {
        pending_subscriptions.emplace(sub->GetKey(), sub);
      }
      synced_subscriptions_.Clear();
      pending_ack_subscriptions_.Clear();
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

void SubscriptionsMap::ProcessAckSubscribe(Slice namespace_id,
                                           Slice topic,
                                           const CursorVector& cursors) {
  TopicUUID key(namespace_id, topic);

  RS_ASSERT(cursors.size() == 1);

  auto pend_it = pending_ack_subscriptions_.Find(key);
  if (pend_it == pending_ack_subscriptions_.End()) {
    return;
  }

  if ((*pend_it)->GetExpected() != cursors[0]) {
    return;
  }

  synced_subscriptions_.Insert(*pend_it);
  pending_ack_subscriptions_.erase(pend_it);
}

bool SubscriptionsMap::ProcessUnsubscribe(
    Flow* flow,
    const MessageUnsubscribe& message,
    Info::Flags flags,
    Info* info) {
  const TopicUUID key(message.GetNamespace(), message.GetTopicName());
  auto reason = message.GetReason();


  switch (reason) {
    case MessageUnsubscribe::Reason::kInvalid:
    case MessageUnsubscribe::Reason::kRequested: {
      // Terminate the subscription only if it is already ack'd.
      {
        auto it = synced_subscriptions_.Find(key);
        if (it != synced_subscriptions_.End()) {
          // No need to send unsubscribe request, as we've just received one.
          // Notify via callback.
          Select(key, flags, info);
          synced_subscriptions_.erase(it);
          return true;
        }
      }
      // A natural race between the server and the client terminating a
      // subscription.
      // State is null at this point, No need to call the terminate callback,
      // as the client has already Unsubscribed.
    } break;
  }
  return false;
}

bool SubscriptionsMap::ProcessDeliver(
    Flow* flow, const MessageDeliver& message) {
  const TopicUUID key(message.GetNamespace(), message.GetTopicName());

  // Find the subscription.
  SubscriptionBase* state = nullptr;
  auto it = synced_subscriptions_.Find(key);
  if (it != synced_subscriptions_.End()) {
    state = *it; // don't delete since it stays in the map
  } else {
    // A natural race between the server delivering a message and the client
    // terminating a subscription.
    LOG_DEBUG(event_loop_->GetLog().get(),
              "Could not find sub");
    return false;
  }
  // Update the state.
  if (!state->ProcessUpdate(event_loop_->GetLog().get(),
                            Cursor(message.GetDataSource().ToString(),
                                   message.GetSequenceNumber()))) {
    // Drop the update.
    return false;
  }
  // Deliver.
  return true;
}

}  // namespace rocketspeed
