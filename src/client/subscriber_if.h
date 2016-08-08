/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <memory>

#include "include/SubscriptionStorage.h"
#include "subscriptions_map.h"

namespace rocketspeed {

class Observer;
using SequenceNumber = uint64_t;
class SubscriptionID;
class SubscriptionParameters;

/// An interface shared by all layers of subscribers.
///
/// Common interface helps in proper unit testing of higher-level subscribers,
/// promotes separation of concerns and code reuse.
class SubscriberIf {
 public:
  /// Destructor must close all remaining active subscriptions.
  virtual ~SubscriberIf() = default;

  /// Establishes a subscription with provided SubscriptionParameters.
  ///
  /// Once the subscription is established, the application will be notified
  /// about new data messages, gaps and termination of the subscription via
  /// provided observer object.
  virtual void StartSubscription(SubscriptionID sub_id,
                                 SubscriptionParameters parameters,
                                 std::unique_ptr<Observer> observer) = 0;

  /// Marks provided message as acknowledged.
  ///
  /// If SubscriptionStorage is being used, the Subscriber can resume
  /// subscripions from storage starting from next unacknowledged message.
  virtual void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno) = 0;

  /// Terminates previously established subscription.
  virtual void TerminateSubscription(SubscriptionID sub_id) = 0;

  /// True iff subscriber has no active subscriptions.
  virtual bool Empty() const = 0;

  /// Saves state of the subscriber using provided storage strategy.
  virtual Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                           size_t worker_id) = 0;

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
      kObserver = 1 << 4,

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

    Observer* GetObserver() const {
      RS_ASSERT(flags_ & kObserver);
      return observer_;
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

    void SetObserver(Observer* observer) {
      flags_ |= kObserver;
      observer_ = observer;
    }

   private:
    TenantID tenant_id_;
    NamespaceID namespace_id_;
    Topic topic_name_;
    SequenceNumber seqno_;
    Observer* observer_;
    Flags flags_ = kNone;
  };

  /// Queries a subscription for a subset of fields, specified by flags.
  /// If subscription exists, the specified fields will be populated in info,
  /// and call will return true.
  /// If subscription doesn't exist, will return false.
  virtual bool Select(
      SubscriptionID sub_id, Info::Flags flags, Info* info) const {
    RS_ASSERT(false);
    return false;
  }

  /// Overrides the user data of the subscription.
  /// Does NOT cleanup the existing user data.
  virtual void SetUserData(SubscriptionID sub_id, void* user_data) {
    RS_ASSERT(false);
  }

  /// Check if routing has changed and reroute subscriptions if necessary.
  virtual void RefreshRouting() = 0;

  /// Tell the subscriber that it is healthy or unhealthy.
  virtual void NotifyHealthy(bool isHealthy) = 0;
};

}  // namespace rocketspeed
