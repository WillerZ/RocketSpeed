/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstdint>
#include <functional>
#include <google/sparse_hash_set>
#include <memory>

#include "include/RocketSpeed.h"
#include "include/Types.h"
#include "src/util/common/subscription_id.h"
#include "src/messages/types.h"
#include "src/util/common/observable_container.h"
#include "src/util/common/ref_count_flyweight.h"
#include "src/util/common/sparse_hash_maps.h"

namespace rocketspeed {

class EventLoop;
class Flow;
class MessageDeliver;
class Logger;
class Slice;
template <typename>
class Sink;

/// A flyweight-pattern-based storage for topics and namespaces.
struct TenantAndNamespace {
  TenantID tenant_id;
  NamespaceID namespace_id;

  friend bool operator<(const TenantAndNamespace& lhs,
                        const TenantAndNamespace& rhs) {
    if (lhs.tenant_id == rhs.tenant_id) {
      return lhs.namespace_id < rhs.namespace_id;
    }
    return lhs.tenant_id < rhs.tenant_id;
  }
};
using TenantAndNamespaceFactory = RefCountFlyweightFactory<TenantAndNamespace>;
using TenantAndNamespaceFlyweight = RefCountFlyweight<TenantAndNamespace>;

/// A base information required by the SubscriptionsMap.
///
/// The layout is optimised primarly for memory usage, and secondary for the
/// performance of metadata updates.
class SubscriptionBase {
 public:
  SubscriptionBase(TenantAndNamespaceFlyweight tenant_and_namespace,
                   const Slice& topic_name,
                   SubscriptionID sub_id,
                   SequenceNumber initial_seqno,
                   void* user_data)
  : tenant_and_namespace_(std::move(tenant_and_namespace))
  , topic_name_(topic_name.ToString())
  , sub_id_(sub_id)
  , expected_seqno_(initial_seqno)
  , user_data_(user_data) {}

  ~SubscriptionBase();

  TenantID GetTenant() const {
    return tenant_and_namespace_.Get().tenant_id;
  }

  Slice GetNamespace() const {
    return tenant_and_namespace_.Get().namespace_id;
  }

  Slice GetTopicName() const { return topic_name_; }

  SequenceNumber GetExpectedSeqno() const { return expected_seqno_; }

  void* GetUserData() const { return user_data_; }

  void SetUserData(void* user_data) { user_data_ = user_data; }

  /// Returns true if the state transition carried by the update has been
  /// recorded and the update shall be delivered, false if the update could not
  /// be applied due to mismatched sequence numbers.
  bool ProcessUpdate(Logger* info_log,
                     SequenceNumber previous,
                     SequenceNumber current);

  /// Retrieves an ID that the subscription currently uses in communication with
  /// the server.
  ///
  /// The ID is invariant for the whole duration of the subscription iff the
  /// subscription have not been rewound.
  SubscriptionID GetIDWhichMayChange() const { return sub_id_; }

 private:
  friend class SubscriptionsMap;

  const TenantAndNamespaceFlyweight tenant_and_namespace_;
  // TODO(stupaq): NTS
  const std::string topic_name_;
  /// An ID of this subscription known to the remote end.
  SubscriptionID sub_id_;
  /// Next expected sequence number on this subscription.
  SequenceNumber expected_seqno_;
  /// Opaque user data.
  void* user_data_;

  /// @{
  /// These methods shall not be accessed anyone but the SubscriptionMap that
  /// stores the subscription. No other piece of code may rely on invariance of
  /// a subscription ID stored _inside_ of the SubscriptionBase.
  /// The SubscriptionID can potentially change when a subscription is rewound.
  /// No intrusive map may contain the subscription when it happens.
  SubscriptionID GetSubscriptionID() const { return sub_id_; }

  void Rewind(SubscriptionID sub_id, SequenceNumber expected_seqno) {
    RS_ASSERT(sub_id_ != sub_id);
    sub_id_ = sub_id;
    expected_seqno_ = expected_seqno;
  }
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
  virtual SequenceNumber GetExpectedSeqno() const = 0;

  /// Subscription's ID.
  virtual SubscriptionID GetID() const = 0;

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
class SubscriptionsMap : public ConnectionAwareReceiver {
 public:
  using DeliverCb = std::function<void(
      Flow* flow, SubscriptionID, std::unique_ptr<MessageDeliver>)>;
  using TerminateCb = std::function<void(
      Flow* flow, SubscriptionID, std::unique_ptr<MessageUnsubscribe>)>;
  using UserDataCleanupCb = std::function<void(void*)>;

  SubscriptionsMap(EventLoop* event_loop,
                   DeliverCb deliver_cb,
                   TerminateCb terminate_cb,
                   UserDataCleanupCb user_data_cleanup_cb);
  ~SubscriptionsMap();

  /// Returns a non-owning pointer to the SubscriptionBase.
  ///
  /// The pointer is valid until matching ::Unsubscribe call.
  void Subscribe(SubscriptionID sub_id,
                 TenantID tenant_id,
                 const Slice& namespace_id,
                 const Slice& topic_name,
                 SequenceNumber initial_seqno,
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
      kSequenceNumber = 1 << 3,
      kUserData = 1 << 4,

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

    SequenceNumber GetSequenceNumber() const {
      RS_ASSERT(flags_ & kSequenceNumber);
      return seqno_;
    }

    void* GetUserData() const {
      RS_ASSERT(flags_ & kUserData);
      return user_data_;
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

    void SetSequenceNumber(SequenceNumber seqno) {
      flags_ |= kSequenceNumber;
      seqno_ = seqno;
    }

    void SetUserData(void* user_data) {
      flags_ |= kUserData;
      user_data_ = user_data;
    }

   private:
    TenantID tenant_id_;
    NamespaceID namespace_id_;
    Topic topic_name_;
    SequenceNumber seqno_;
    void* user_data_;
    Flags flags_ = kNone;
  };

  /// Extracts information about a subscription.
  bool Select(SubscriptionID sub_id,
              Info::Flags flags,
              Info* info) const;

  /// Checks if subscription exists.
  bool Exists(SubscriptionID sub_id) const;

  /// Rewinds provided subscription to a given sequence number.
  void Rewind(SubscriptionID old_sub_id,
              SubscriptionID new_sub_id,
              SequenceNumber new_seqno);

  /// Returns true if a subscription was terminated, false if it didn't exist.
  void Unsubscribe(SubscriptionID sub_id);

  bool Empty() const;

  /// Iterates over the all subscriptions or (until false is returned from the callback)
  /// in arbitrary order and invokes
  /// provided callback with a `const SubscriptionData&` for each subscription.
  /// The SubscriptionData reference is only valid for the duration of the
  /// callback, and references must not escape that scope.
  /// 
  /// The map must not be modified during the loop.
  template <typename Iter>
  void Iterate(Iter&& iter);

  /// Sets the user data for a subscription.
  void SetUserData(SubscriptionID sub_id, void* user_data);

 private:
  EventLoop* const event_loop_;
  const DeliverCb deliver_cb_;
  const TerminateCb terminate_cb_;
  const UserDataCleanupCb user_data_cleanup_cb_;

  TenantAndNamespaceFactory tenant_and_namespace_factory_;

  struct SubscriptionsMapping {
    SubscriptionID ExtractKey(const SubscriptionBase* sub) const {
      return sub->GetSubscriptionID();
    }
    size_t Hash(const SubscriptionID& id) const {
      return std::hash<SubscriptionID>()(id);
    }
    bool Equals(const SubscriptionID& id1, const SubscriptionID& id2) const {
      return id1 == id2;
    }
  };

  // NOTE: these raw SubscriptionBase pointers are owned by the maps.
  // If you to remove an element from the map it's your responsibility
  // to take care of the memory.
  using Subscriptions =
    STLAdapter<SparseKeylessMap<
                  SubscriptionID, SubscriptionBase*, SubscriptionsMapping>,
                SubscriptionsMapping>;
  ObservableContainer<Subscriptions> pending_subscriptions_;
  Subscriptions synced_subscriptions_;

  using Unsubscribes = google::sparse_hash_set<SubscriptionID>;
  ObservableContainer<Unsubscribes> pending_unsubscribes_;

  std::unique_ptr<Sink<SharedTimestampedString>> sink_;

  /// Returns a non-owning pointer to the SubscriptionBase or null if doesn't
  /// exist.
  SubscriptionBase* Find(SubscriptionID sub_id) const;

  Logger* GetLogger() const;

  // Function takes upstream_sub ownership
  void HandlePendingSubscription(
        Flow* flow, std::unique_ptr<SubscriptionBase> upstream_sub);

  void HandlePendingUnsubscription(Flow* flow, SubscriptionID sub_id);

  void ConnectionDropped() final override;
  void ConnectionCreated(
    std::unique_ptr<Sink<SharedTimestampedString>> sink) final override;
  void ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe>) final override;
  void ReceiveDeliver(StreamReceiveArg<MessageDeliver>) final override;

  void CleanupSubscription(SubscriptionBase* sub);
};

template <typename Iter>
void SubscriptionsMap::Iterate(Iter&& iter) {
  class SubscriptionStateData : public SubscriptionData {
   public:
    explicit SubscriptionStateData(SubscriptionBase* state) : state_(state) {}

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
    SubscriptionBase* state_;
  };

  for (auto ptr : pending_subscriptions_.Read()) {
    const SubscriptionStateData data(ptr);
    bool keep_iterating = iter(data);
    if (!keep_iterating) {
      return;
    }
  }
  for (auto ptr : synced_subscriptions_) {
    const SubscriptionStateData data(ptr);
    bool keep_iterating = iter(data);
    if (!keep_iterating) {
      return;
    }
  }
}

}  // namespace rocketspeed
