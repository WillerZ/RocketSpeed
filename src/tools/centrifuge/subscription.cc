// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Centrifuge.h"
#include <thread>

namespace rocketspeed {

CentrifugeSubscription::CentrifugeSubscription(
    TenantID tenant_id,
    NamespaceID namespace_id,
    Topic topic,
    SequenceNumber seqno,
    std::unique_ptr<Observer> _observer)
: params(tenant_id, std::move(namespace_id), std::move(topic), seqno)
, observer(std::move(_observer)) {
  if (!observer) {
    observer.reset(new Observer());  // observer does nothing
  }
}

std::unique_ptr<Observer> ExpectInvalidObserver() {
  class ExpectInvalid : public Observer {
   public:
    ExpectInvalid() {}

    void OnMessageReceived(Flow*, std::unique_ptr<MessageReceived>&) override {
      CentrifugeError(
        Status::InvalidArgument("Received message on invalid subscription"));
    }

    void OnSubscriptionStatusChange(const SubscriptionStatus& st) override {
      // Subscription could have been unsubscribed before the server told us
      // it was invalid, so we have to accept ok() status.
      if (!st.GetStatus().IsInvalidArgument() || !st.GetStatus().ok()) {
        CentrifugeError(
          Status::InvalidArgument("Received non-invalid termination status"));
      }
    }

    void OnDataLoss(Flow* flow, const DataLossInfo& info) override {
      // Shouldn't receive data loss since the subscription should be
      // terminated immediately.
      CentrifugeError(
        Status::InvalidArgument("Received data loss on invalid subscription"));
    }
  };
  return std::unique_ptr<Observer>(new ExpectInvalid());
}

std::unique_ptr<Observer> ExpectNotInvalidObserver() {
  class ExpectNotInvalid : public Observer {
   public:
    ExpectNotInvalid() {}

    void OnSubscriptionStatusChange(const SubscriptionStatus& st) override {
      // Subscription could have been unsubscribed before the server told us
      // it was invalid, so we have to accept ok() status.
      if (st.GetStatus().IsInvalidArgument()) {
        CentrifugeError(Status::InvalidArgument(
          "Received invalid sub termination for valid subscription"));
      }
    }
  };
  return std::unique_ptr<Observer>(new ExpectNotInvalid());
}

std::unique_ptr<Observer> ExpectNoUpdatesObserver() {
  class ExpectNoUpdates : public Observer {
   public:
    ExpectNoUpdates() {}

    void OnMessageReceived(Flow*, std::unique_ptr<MessageReceived>&) override {
      CentrifugeError(
        Status::InvalidArgument("Received message on invalid subscription"));
    }
  };
  return std::unique_ptr<Observer>(new ExpectNoUpdates());
}

namespace {
class SlowObserver : public Observer {
 public:
  SlowObserver(std::unique_ptr<Observer> wrapped,
               std::chrono::milliseconds sleep_time)
  : wrapped_(std::move(wrapped))
  , sleep_time_(sleep_time) {}

  void OnMessageReceived(
      Flow* flow, std::unique_ptr<MessageReceived>& msg) override {
    if (wrapped_) {
      wrapped_->OnMessageReceived(flow, msg);
    }
    // Sleep to slow down processing -- should cause backpressure.
    /* sleep override */
    std::this_thread::sleep_for(sleep_time_);
  }

  void OnSubscriptionStatusChange(const SubscriptionStatus& st) override {
    if (wrapped_) {
      wrapped_->OnSubscriptionStatusChange(st);
    }
  }

  void OnDataLoss(Flow* flow, const DataLossInfo& info) override {
    if (wrapped_) {
      wrapped_->OnDataLoss(flow, info);
    }
  }

 private:
  std::unique_ptr<Observer> wrapped_;
  const std::chrono::milliseconds sleep_time_;
};
}  // namespace anonymous

std::unique_ptr<Observer> SlowConsumerObserver(
    std::unique_ptr<Observer> observer,
    std::chrono::milliseconds sleep_time) {
  return std::unique_ptr<Observer>(
    new SlowObserver(std::move(observer), sleep_time));
}
}  // namespace rocketspeed
