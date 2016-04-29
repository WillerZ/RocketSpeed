/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <memory>

#include "include/SubscriptionStorage.h"

namespace rocketspeed {

class Observer;
using SequenceNumber = uint64_t;
using SubscriptionID = uint64_t;
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
};

}  // namespace rocketspeed
