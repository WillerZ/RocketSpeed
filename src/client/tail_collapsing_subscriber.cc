// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "tail_collapsing_subscriber.h"

#include <chrono>
#include <cmath>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "external/folly/Memory.h"
#include "external/folly/move_wrapper.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/single_shard_subscriber.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
namespace detail {

class TailCollapsingObserver : public Observer {
 public:
  TailCollapsingSubscriber* subscriber_;
  /**
   * Maps subscription ID to original observer for subscriptions served by this
   * upstream subscription.
   */
  std::unordered_map<SubscriptionID, std::unique_ptr<Observer>>
      downstream_observers_;

  explicit TailCollapsingObserver(TailCollapsingSubscriber* subscriber)
  : subscriber_(subscriber) {}

  void OnMessageReceived(
      Flow* flow, std::unique_ptr<MessageReceived>& up_message) override {
    // Prepare proxy that maintains shared ownership of the message.
    class SharedMessageReceived : public MessageReceived {
     public:
      SubscriptionID id_;
      std::shared_ptr<MessageReceived> message_;

      SubscriptionHandle GetSubscriptionHandle() const override { return id_; }

      SequenceNumber GetSequenceNumber() const override {
        return message_->GetSequenceNumber();
      }

      Slice GetContents() const override { return message_->GetContents(); }
    };
    std::shared_ptr<MessageReceived> shared_message(std::move(up_message));

    // Deliver to every observer served by this upstream subscription.
    for (const auto& entry : downstream_observers_) {
      // TODO(t10075129)
      auto down_message = folly::make_unique<SharedMessageReceived>();
      down_message->id_ = entry.first;
      down_message->message_ = shared_message;
      std::unique_ptr<MessageReceived> tmp(std::move(down_message));
      entry.second->OnMessageReceived(flow, tmp);
    }
  }

  void OnSubscriptionStatusChange(
      const SubscriptionStatus& up_status) override {
    // We have to override the SubscriptionHandle for each downstream
    // subscription.
    class SubscriptionStatusImpl : public SubscriptionStatus {
     public:
      SubscriptionID sub_id_;
      const SubscriptionStatus& status_;

      explicit SubscriptionStatusImpl(const SubscriptionStatus& status)
      : status_(status) {}

      SubscriptionHandle GetSubscriptionHandle() const override {
        return sub_id_;
      }

      TenantID GetTenant() const override { return status_.GetTenant(); }

      const NamespaceID& GetNamespace() const override {
        return status_.GetNamespace();
      }

      const Topic& GetTopicName() const override {
        return status_.GetTopicName();
      }

      SequenceNumber GetSequenceNumber() const override {
        return status_.GetSequenceNumber();
      }

      const Status& GetStatus() const override { return status_.GetStatus(); }
    } down_status(up_status);

    for (const auto& entry : downstream_observers_) {
      down_status.sub_id_ = entry.first;
      entry.second->OnSubscriptionStatusChange(down_status);
    }
    // State of downstream subscriptions might have been removed, if we got this
    // notification as a confirmation that TerminateSubscription(...) finished.
    // Remove all data associated with all subscriptions on this topic.
    for (const auto& entry : downstream_observers_) {
      SubscriptionID downstream_id = entry.first;
      subscriber_->downstream_to_upstream_.erase(downstream_id);
      subscriber_->special_observers_.erase(this);
    }
    // Remove metadata for this topic, as it was the only subscription on it.
    subscriber_->upstream_subscriptions_.Remove(
      up_status.GetNamespace(),
      up_status.GetTopicName(),
      SubscriptionID::Unsafe(up_status.GetSubscriptionHandle()));
  }

  void OnDataLoss(Flow* flow, const DataLossInfo& up_info) override {
    // We have to override the SubscriptionHandle for each downstream
    // subscription.
    class DataLossInfoImpl : public DataLossInfo {
     public:
      SubscriptionID id_;
      DataLossType type_;
      SequenceNumber first_;
      SequenceNumber last_;

      SubscriptionHandle GetSubscriptionHandle() const override { return id_; }

      DataLossType GetLossType() const override { return type_; }

      SequenceNumber GetFirstSequenceNumber() const override { return first_; }

      SequenceNumber GetLastSequenceNumber() const override { return last_; }
    };

    // Deliver to every observer served by this upstream subscription.
    for (const auto& entry : downstream_observers_) {
      // TODO(t10075129)

      DataLossInfoImpl down_info;
      down_info.id_ = entry.first;
      down_info.type_ = up_info.GetLossType();
      down_info.first_ = up_info.GetFirstSequenceNumber();
      down_info.last_ = up_info.GetLastSequenceNumber();
      entry.second->OnDataLoss(flow, down_info);
    }
  }
};

}  // namespace details

TailCollapsingSubscriber::TailCollapsingSubscriber(
    std::unique_ptr<SubscriberIf> subscriber)
: subscriber_(std::move(subscriber))
, upstream_subscriptions_(
  [this](SubscriptionID sub_id, NamespaceID* namespace_id, Topic* topic_name) {
    Info info;
    Info::Flags flags = Info::kNamespace | Info::kTopic;
    if (subscriber_->Select(sub_id, flags, &info)) {
      *namespace_id = info.GetNamespace();
      *topic_name = info.GetTopic();
      return true;
    }
    return false;
  }) {}

void TailCollapsingSubscriber::StartSubscription(
    SubscriptionID downstream_id,
    SubscriptionParameters parameters,
    std::unique_ptr<Observer> new_observer) {
  SubscriptionID upstream_id = upstream_subscriptions_.Find(
      parameters.namespace_id, parameters.topic_name);
  Info info;
  Info::Flags flags = Info::kNamespace | Info::kTopic | Info::kObserver;
  if (subscriber_->Select(upstream_id, flags, &info)) {
    RS_ASSERT(info.GetNamespace() == parameters.namespace_id);
    RS_ASSERT(info.GetTopic() == parameters.topic_name);
    // There exists a subscription on the topic already.
    detail::TailCollapsingObserver* collapsing_observer;
    if (special_observers_.count(info.GetObserver()) == 0) {
      {  // There exists only one subscription on the topic, set up
         // multiplexing.
        collapsing_observer = new detail::TailCollapsingObserver(this);
        auto result = special_observers_.insert(collapsing_observer);
        (void)result;
        RS_ASSERT(result.second);
      }
      std::unique_ptr<Observer> old_observer(info.GetObserver());
      subscriber_->SetUserData(upstream_id, collapsing_observer);

      // Add existing subscription to the multiplexer.
      auto result = collapsing_observer->downstream_observers_.emplace(
          upstream_id, std::move(old_observer));
      (void)result;
      RS_ASSERT(result.second);
    } else {
      // We can safely downcast.
      collapsing_observer = static_cast<detail::TailCollapsingObserver*>(
          info.GetObserver());
    }
    RS_ASSERT(collapsing_observer);

    {  // We've already set up multiplexing for the upstream subscription, just
       // add the new downstream subscription.
      auto result = collapsing_observer->downstream_observers_.emplace(
          downstream_id, std::move(new_observer));
      (void)result;
      RS_ASSERT(result.second);
    }
    {  // Redirect any inquiries for this subscription ID.
      auto result = downstream_to_upstream_.emplace(downstream_id, upstream_id);
      (void)result;
      RS_ASSERT(result.second);
    }
  } else {
    // There exists no subscription on the topic, just create one.
    upstream_subscriptions_.Insert(
        parameters.namespace_id, parameters.topic_name, downstream_id);
    subscriber_->StartSubscription(
        downstream_id, parameters, std::move(new_observer));
  }
}

void TailCollapsingSubscriber::Acknowledge(SubscriptionID downstream_id,
                                           SequenceNumber seqno) {
  // TODO(t10075879)
}

void TailCollapsingSubscriber::TerminateSubscription(
    SubscriptionID downstream_id) {
  SubscriptionID upstream_id;
  {  // Find the subscription ID of the upstream subscription.
    auto it = downstream_to_upstream_.find(downstream_id);
    if (it == downstream_to_upstream_.end()) {
      // There is no remapping set up for this subscription. It either doesn't
      // exist or is not multiplexed, or upstream subscription is using the same
      // ID as the downstream one.
      upstream_id = downstream_id;
    } else {
      // We've found the remapping, now remove it, as the downstream
      // subscription will be gone shortly.
      upstream_id = it->second;
      downstream_to_upstream_.erase(it);
    }
  }
  // Find the upstream subscription's state.
  Info info;
  Info::Flags flags = Info::kNamespace | Info::kTopic | Info::kObserver;
  if (!subscriber_->Select(upstream_id, flags, &info)) {
    RS_ASSERT(upstream_id == downstream_id);
    // The subscription just doesn't exist, this is fine.
    return;
  }

  if (special_observers_.count(info.GetObserver()) == 0) {
    // This subscription is unique, just unsubscribe.
    upstream_subscriptions_.Remove(
        info.GetNamespace(), info.GetTopic(), upstream_id);
    subscriber_->TerminateSubscription(upstream_id);
  } else {
    // This subscription is multiplexed, we can downcast the observer.
    auto* collapsing_observer = static_cast<detail::TailCollapsingObserver*>(
        info.GetObserver());

    auto& downstream_observers = collapsing_observer->downstream_observers_;
    if (downstream_observers.size() > 1) {
      // Remove the downstream subscription only, we still have another
      // downstream subscription being served by this upstream subscription.
      auto result = downstream_observers.erase(downstream_id);
      (void)result;
      RS_ASSERT(result == 1);
    } else {
      RS_ASSERT(downstream_observers.size() == 1);
      RS_ASSERT(downstream_observers.count(downstream_id) == 1);

      // This is the last downstream subscription on this upstream subscription,
      // we just unsubscribe.
      // Remove the observer pointer from the set.
      auto result = special_observers_.erase(info.GetObserver());
      (void)result;
      RS_ASSERT(result);
      subscriber_->TerminateSubscription(upstream_id);
    }
  }
}

Status TailCollapsingSubscriber::SaveState(
    SubscriptionStorage::Snapshot* snapshot, size_t worker_id) {
  // TODO(t10075879)
  return Status::NotSupported("");
}

}  // namespace rocketspeed
