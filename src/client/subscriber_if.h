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

/**
 * Interface letting user to trace or debug Subscriber with a set of hooks
 * corresponding to its API and Observer API.
 * Multithread clients will callback from thread specific
 * to particular subscription.
 */
class SubscriberHooks {
 public:
  virtual ~SubscriberHooks() = default;
  virtual void SubscriptionExists() = 0;
  virtual void OnStartSubscription() = 0;
  virtual void OnAcknowledge(SequenceNumber seqno) = 0;
  virtual void OnTerminateSubscription() = 0;
  virtual void OnReceiveTerminate() = 0;
  virtual void OnMessageReceived(const MessageReceived* msg) = 0;
  virtual void OnSubscriptionStatusChange(const SubscriptionStatus&) = 0;
  virtual void OnDataLoss(const DataLossInfo& info) = 0;
};

/// An interface shared by all layers of subscribers.
///
/// Common interface helps in proper unit testing of higher-level subscribers,
/// promotes separation of concerns and code reuse.
class SubscriberIf {
 public:
  /// Destructor must close all remaining active subscriptions.
  virtual ~SubscriberIf() = default;

  /// Install hooks for specific subscription
  virtual void InstallHooks(const HooksParameters& params,
                            std::shared_ptr<SubscriberHooks> hooks) = 0;
  /// Uninstall previously installed hooks
  virtual void UnInstallHooks(const HooksParameters& params) = 0;

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
  class Info : public SubscriptionsMap::Info {
   public:
    enum : Flags {
      kObserver = kUserData,
    };

    Observer* GetObserver() const {
      return static_cast<Observer*>(GetUserData());
    }

    void SetObserver(Observer* observer) {
      SetUserData(static_cast<void*>(observer));
    }
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

  virtual bool CallInSubscriptionThread(SubscriptionParameters params,
                                        std::function<void()> job) = 0;
};
}  // namespace rocketspeed
