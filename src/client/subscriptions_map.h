/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_set>

#include "include/RocketSpeed.h"
#include "include/Types.h"
#include "sparsehash/sparse_hash_set"
#include "src/messages/types.h"
#include "src/util/common/observable_container.h"
#include "src/util/common/sparse_hash_maps.h"
#include "src/util/common/subscription_id.h"
#include "src/util/topic_uuid.h"

namespace rocketspeed {

class EventLoop;
class Flow;
class MessageDeliver;
class MyReceiver;
class Logger;
class Slice;

using SubscriptionKey = TopicUUID;

/// A base information required by the SubscriptionsMap.
///
/// The layout is optimised primarly for memory usage, and secondary for the
/// performance of metadata updates.
class SubscriptionBase {
 public:
  using ReplicaIndex = size_t;

  SubscriptionBase(Slice namespace_id,
                   Slice topic_name,
                   SubscriptionID sub_id,
                   Cursor start)
  : topic_uuid_(namespace_id, topic_name)
  , sub_id_(sub_id)
  , expected_(std::move(start)) {}

  ~SubscriptionBase();

  const SubscriptionKey& GetKey() const {
    return topic_uuid_;
  }

  Slice GetNamespace() const {
    Slice namespace_id;
    Slice topic;
    topic_uuid_.GetTopicID(&namespace_id, &topic);
    return namespace_id;
  }

  Slice GetTopicName() const {
    Slice namespace_id;
    Slice topic;
    topic_uuid_.GetTopicID(&namespace_id, &topic);
    return topic;
  }

  const Cursor& GetExpected() const { return expected_; }

  // TODO:
  /// Returns true if the state transition carried by the update has been
  /// recorded and the update shall be delivered, false if the update could not
  /// be applied due to mismatched sequence numbers.
  bool ProcessUpdate(ReplicaIndex replica, Logger* info_log, Cursor current);

  /// Retrieves an ID that the subscription currently uses in communication with
  /// the server.
  ///
  /// The ID is invariant for the whole duration of the subscription iff the
  /// subscription have not been rewound.
  SubscriptionID GetIDWhichMayChange() const { return sub_id_; }

 private:
  friend class SubscriptionsMap;

  const TopicUUID topic_uuid_;
  /// An ID of this subscription known to the remote end.
  SubscriptionID sub_id_;
  /// Next expected sequence number on this subscription.
  Cursor expected_;

  /// @{
  /// These methods shall not be accessed anyone but the SubscriptionMap that
  /// stores the subscription. No other piece of code may rely on invariance of
  /// a subscription ID stored _inside_ of the SubscriptionBase.
  /// The SubscriptionID can potentially change when a subscription is rewound.
  /// No intrusive map may contain the subscription when it happens.
  SubscriptionID GetSubscriptionID() const { return sub_id_; }

  void Rewind(SubscriptionID sub_id, Cursor expected) {
    RS_ASSERT(sub_id_ != sub_id);
    sub_id_ = sub_id;
    expected_ = std::move(expected);
  }

  void SetCursor(Cursor cursor) { expected_ = std::move(cursor); }
  /// @}
};

/// Interface for accessing subscription metadata internals that is decoupled
/// from the specifics of storage.
class SubscriptionData {
 public:
  /// Tenant of the subscription.
  virtual TenantID GetTenant() const = 0;

  /// Namespace of the subscription. The lifetime of the slice is equal to that
  /// of the SubscriptionData.
  virtual Slice GetNamespace() const = 0;

  /// Topic name of the subscription. The lifetime of the slice is equal to that
  /// of the SubscriptionData.
  virtual Slice GetTopicName() const = 0;

  /// Next sequence number expected on the subscription, e.g. if last received
  /// was N then the next expected seqno is N+1.
  virtual Cursor GetExpected() const = 0;

  /// Subscription's ID.
  virtual SubscriptionID GetID() const = 0;

  /// User data.
  virtual void* GetUserData() const = 0;

 protected:
  virtual ~SubscriptionData() {}
};

/// A map of active subscriptions that replicates itself to the remove end over
/// provided sink and processes messages delivered on a subscription.
///
/// Stores absolutely minimal amount of information (per subscription) that is
/// needed to process updates and handle reconnections. Enabled users to attach
/// arbitrary state and functionality to a subscription.
///
/// The class is optimised for memory usage per subscription and is not
/// thread-safe.
// TODO(stupaq): generalise the sink and reshard by host in the proxy
class SubscriptionsMap {
 public:
  using UserDataCleanupCb = std::function<void(void*)>;
  using ReplicaIndex = size_t;

  SubscriptionsMap(
      EventLoop* event_loop,
      std::function<void(Flow*, ReplicaIndex, std::unique_ptr<Message>)>
          message_handler,
      UserDataCleanupCb user_data_cleanup_cb,
      TenantID tenant_id);
  ~SubscriptionsMap();

  /// Returns true if a new subscription was added, and false if an existing
  /// subscription was overridden.
  bool Subscribe(SubscriptionID sub_id,
                 const Slice& namespace_id,
                 const Slice& topic_name,
                 const CursorVector& start,
                 void* user);

  /// Contains information of the result of a selection.
  class Info {
   public:
    using Flags = uint64_t;

    enum : Flags {
      kNone = 0,

      kTenant = 1 << 0,
      kNamespace = 1 << 1,
      kTopic = 1 << 2,
      kCursor = 1 << 3,
      kUserData = 1 << 4,
      kSubID = 1 << 5,

      kAll = ~0ULL,
    };

    TenantID GetTenant() const {
      RS_ASSERT(flags_ & kTenant);
      return tenant_id_;
    }

    const NamespaceID& GetNamespace() const {
      RS_ASSERT(flags_ & kNamespace);
      return namespace_id_;
    }

    const Topic& GetTopic() const {
      RS_ASSERT(flags_ & kTopic);
      return topic_name_;
    }

    const Cursor& GetCursor() const {
      RS_ASSERT(flags_ & kCursor);
      return cursor_;
    }

    void* GetUserData() const {
      RS_ASSERT(flags_ & kUserData);
      return user_data_;
    }

    SubscriptionID GetSubID() const {
      RS_ASSERT(flags_ & kSubID);
      return sub_id_;
    }

    void SetTenant(TenantID tenant_id) {
      flags_ |= kTenant;
      tenant_id_ = tenant_id;
    }

    void SetNamespace(NamespaceID namespace_id) {
      flags_ |= kNamespace;
      namespace_id_ = std::move(namespace_id);
    }

    void SetTopic(Topic topic_name) {
      flags_ |= kTopic;
      topic_name_ = std::move(topic_name);
    }

    void SetCursor(Cursor cursor) {
      flags_ |= kCursor;
      cursor_ = std::move(cursor);
    }

    void SetUserData(void* user_data) {
      flags_ |= kUserData;
      user_data_ = user_data;
    }

    void SetSubID(SubscriptionID sub_id) {
      flags_ |= kSubID;
      sub_id_ = sub_id;
    }

   private:
    TenantID tenant_id_;
    NamespaceID namespace_id_;
    Topic topic_name_;
    Cursor cursor_;
    void* user_data_;
    SubscriptionID sub_id_;
    Flags flags_ = kNone;
  };

  /// Extracts information about a subscription.
  bool Select(const SubscriptionKey& key,
              Info::Flags flags,
              Info* info) const;

  /// Checks if subscription exists.
  bool Exists(const SubscriptionKey& key) const;

  /// Checks if a subscription has been synced to the server.
  bool IsSent(const SubscriptionKey& key) const;

  /// Rewinds provided subscription to a given cursor.
  void Rewind(const SubscriptionKey& key,
              SubscriptionID new_sub_id,
              Cursor new_cursor);

  /// Returns true if a subscription was terminated, false if it didn't exist.
  bool Unsubscribe(const SubscriptionKey& key);

  bool Empty() const;

  /// Iterates over the all subscriptions or (until false is returned from the
  /// callback)
  /// in arbitrary order and invokes
  /// provided callback with a `const SubscriptionData&` for each subscription.
  /// The SubscriptionData reference is only valid for the duration of the
  /// callback, and references must not escape that scope.
  ///
  /// The map must not be modified during the loop.
  template <typename Iter>
  void Iterate(Iter&& iter);

  /// Sets the user data for a subscription.
  void SetUserData(const SubscriptionKey& key, void* user_data);

  void StartSync(ReplicaIndex replica);
  void StopSync(ReplicaIndex replica);

  void ProcessAckSubscribe(ReplicaIndex replica,
                           Slice namespace_id,
                           Slice topic_name,
                           const CursorVector& start);

  /// Returns true iff the unsubscribe matched a subscription, and fills the
  /// info with the removed subscription.
  bool ProcessUnsubscribe(
      ReplicaIndex replica,
      const MessageUnsubscribe& message,
      Info::Flags flags,
      Info* info);

  /// Returns true iff a subscription was advanced by the deliver message.
  bool ProcessDeliver(ReplicaIndex replica, const MessageDeliver& message);

 private:
  EventLoop* const event_loop_;
  const UserDataCleanupCb user_data_cleanup_cb_;

  struct SubscriptionsMapping {
    const SubscriptionKey& ExtractKey(const SubscriptionBase* sub) const {
      return sub->GetKey();
    }
    size_t Hash(const SubscriptionKey& id) const {
      return id.Hash();
    }
    bool Equals(const SubscriptionKey& id1, const SubscriptionKey& id2) const {
      return id1 == id2;
    }
  };

  // NOTE: these raw SubscriptionBase pointers are owned by the maps.
  // If you to remove an element from the map it's your responsibility
  // to take care of the memory.
  using Subscriptions =
    STLAdapter<SparseKeylessMap<
                  SubscriptionKey, SubscriptionBase*, SubscriptionsMapping>,
                SubscriptionsMapping>;
  ObservableContainer<Subscriptions> pending_subscriptions_;
  Subscriptions synced_subscriptions_;
  Subscriptions pending_ack_subscriptions_;

  using Unsubscribes =
      std::unordered_map<TopicUUID, std::vector<SubscriptionID>>;
  ObservableContainer<Unsubscribes> pending_unsubscribes_;

  // Stores per-topic user data.
  using UserDataMap = std::unordered_map<TopicUUID, void*>;
  UserDataMap user_data_;
  const TenantID tenant_id_;

  std::function<void(Flow*, ReplicaIndex, std::unique_ptr<Message>)>
      message_handler_;

  /// Returns a non-owning pointer to the SubscriptionBase or null if doesn't
  /// exist.
  SubscriptionBase* Find(const SubscriptionKey& key) const;

  Logger* GetLogger() const;

  // Function takes upstream_sub ownership
  void HandlePendingSubscription(
      Flow* flow,
      ReplicaIndex replica,
      std::unique_ptr<SubscriptionBase> upstream_sub);

  void HandlePendingUnsubscription(
      Flow* flow,
      ReplicaIndex replica,
      Unsubscribes::value_type sub);

  void CleanupSubscription(SubscriptionBase* sub);

  // Checks some invariants about subscription state for a key.
  void CheckInvariants(const SubscriptionKey& key);
};

template <typename Iter>
void SubscriptionsMap::Iterate(Iter&& iter) {
  class SubscriptionStateData : public SubscriptionData {
   public:
    explicit SubscriptionStateData(SubscriptionBase* state,
                                   SubscriptionsMap* sub_map)
    : state_(state), sub_map_(sub_map) {}

    TenantID GetTenant() const override {
      return sub_map_->tenant_id_;
    }

    Slice GetNamespace() const override {
      return state_->GetNamespace();
    }

    Slice GetTopicName() const override {
      return state_->GetTopicName();
    }

    Cursor GetExpected() const override {
      return state_->GetExpected();
    }

    SubscriptionID GetID() const override {
      return state_->GetIDWhichMayChange();
    }

    void* GetUserData() const override {
      TopicUUID uuid(GetNamespace(), GetTopicName());
      auto it = sub_map_->user_data_.find(uuid);
      return it == sub_map_->user_data_.end() ? nullptr : it->second;
    }

   private:
    SubscriptionBase* state_;
    SubscriptionsMap* sub_map_;
  };

  for (auto ptr : pending_subscriptions_.Read()) {
    const SubscriptionStateData data(ptr, this);
    bool keep_iterating = iter(data);
    if (!keep_iterating) {
      return;
    }
  }
  for (auto ptr : pending_ack_subscriptions_) {
    const SubscriptionStateData data(ptr, this);
    bool keep_iterating = iter(data);
    if (!keep_iterating) {
      return;
    }
  }
  for (auto ptr : synced_subscriptions_) {
    const SubscriptionStateData data(ptr, this);
    bool keep_iterating = iter(data);
    if (!keep_iterating) {
      return;
    }
  }
}

}  // namespace rocketspeed
