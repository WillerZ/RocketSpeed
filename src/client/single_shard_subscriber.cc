/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "single_shard_subscriber.h"

#include <memory>

#include "external/folly/Memory.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/subscriber_stats.h"
#include "src/client/subscriptions_map.tcc"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////
Subscriber::Subscriber(const ClientOptions& options,
                       EventLoop* event_loop,
                       std::shared_ptr<SubscriberStats> stats,
                       std::unique_ptr<SubscriptionRouter> router)
: options_(options)
, event_loop_(event_loop)
, stats_(std::move(stats))
, subscriptions_map_(event_loop_,
                     std::bind(&Subscriber::ReceiveDeliver, this, _1, _2, _3),
                     std::bind(&Subscriber::ReceiveTerminate, this, _1, _2, _3),
                     options_.backoff_strategy)
, last_router_version_(0)
, router_(std::move(router)) {
  thread_check_.Check();

  router_timer_ = event_loop_->CreateTimedEventCallback(
      std::bind(&Subscriber::CheckRouterVersion, this), options_.timer_period);
  router_timer_->Enable();

  CheckRouterVersion();
}

void Subscriber::StartSubscription(SubscriptionID sub_id,
                                   SubscriptionParameters parameters,
                                   std::unique_ptr<Observer> observer) {
  thread_check_.Check();

  auto ptr = subscriptions_map_.Subscribe(sub_id,
                                          parameters.tenant_id,
                                          parameters.namespace_id,
                                          parameters.topic_name,
                                          parameters.start_seqno);
  ptr->SwapObserver(&observer);
}

void Subscriber::Acknowledge(SubscriptionID sub_id,
                             SequenceNumber acked_seqno) {
  if (!subscriptions_map_.Find(sub_id)) {
    LOG_WARN(options_.info_log,
             "Cannot acknowledge missing subscription ID (%lld)",
             sub_id.ForLogging());
    return;
  }

  last_acks_map_[sub_id] = acked_seqno;
}

void Subscriber::TerminateSubscription(SubscriptionID sub_id) {
  thread_check_.Check();

  if (auto ptr = subscriptions_map_.Find(sub_id)) {
    // Notify the user.
    SourcelessFlow no_flow(event_loop_->GetFlowControl());
    ReceiveTerminate(&no_flow,
                     ptr,
                     folly::make_unique<MessageUnsubscribe>(
                         ptr->GetTenant(),
                         ptr->GetIDWhichMayChange(),
                         MessageUnsubscribe::Reason::kRequested));

    // Terminate the subscription, which will invalidate the pointer.
    subscriptions_map_.Unsubscribe(ptr);
    ptr = nullptr;
  }

  last_acks_map_.erase(sub_id);
}

Status Subscriber::SaveState(SubscriptionStorage::Snapshot* snapshot,
                             size_t worker_id) {
  Status status;
  subscriptions_map_.Iterate([&](SubscriptionState* state) {
    if (!status.ok()) {
      return;
    }

    SequenceNumber start_seqno =
        GetLastAcknowledged(state->GetIDWhichMayChange());
    // Subscription storage stores parameters of subscribe requests that shall
    // be reissued, therefore we must persiste the next sequence number.
    if (start_seqno > 0) {
      ++start_seqno;
    }
    status = snapshot->Append(worker_id,
                              state->GetTenant(),
                              state->GetNamespace().ToString(),
                              state->GetTopicName().ToString(),
                              start_seqno);
  });
  return status;
}

SequenceNumber Subscriber::GetLastAcknowledged(SubscriptionID sub_id) const {
  auto it = last_acks_map_.find(sub_id);
  return it == last_acks_map_.end() ? 0 : it->second;
}

void Subscriber::CheckRouterVersion() {
  thread_check_.Check();
  const auto version = router_->GetVersion();
  if (last_router_version_ != version || last_router_version_ == 0) {
    last_router_version_ = version;

    subscriptions_map_.ReconnectTo(router_->GetHost());
  }
}

namespace {

class MessageReceivedImpl : public MessageReceived {
 public:
  explicit MessageReceivedImpl(std::unique_ptr<MessageDeliverData> data)
  : data_(std::move(data)) {}

  SubscriptionHandle GetSubscriptionHandle() const override {
    return data_->GetSubID();
  }

  SequenceNumber GetSequenceNumber() const override {
    return data_->GetSequenceNumber();
  }

  Slice GetContents() const override { return data_->GetPayload(); }

 private:
  std::unique_ptr<MessageDeliverData> data_;
};

class DataLossInfoImpl : public DataLossInfo {
 public:
  explicit DataLossInfoImpl(std::unique_ptr<MessageDeliverGap> gap_data)
  : gap_data_(std::move(gap_data)) {}

  SubscriptionHandle GetSubscriptionHandle() const override {
    return gap_data_->GetSubID();
  }

  DataLossType GetLossType() const override {
    switch (gap_data_->GetGapType()) {
      case GapType::kDataLoss:
        return DataLossType::kDataLoss;
      case GapType::kRetention:
        return DataLossType::kRetention;
      case GapType::kBenign:
        RS_ASSERT(false);
        return DataLossType::kRetention;
        // No default, we will be warned about unhandled code.
    }
    RS_ASSERT(false);
    return DataLossType::kRetention;
  }

  SequenceNumber GetFirstSequenceNumber() const override {
    return gap_data_->GetFirstSequenceNumber();
  }

  SequenceNumber GetLastSequenceNumber() const override {
    return gap_data_->GetLastSequenceNumber();
  }

 private:
  std::unique_ptr<MessageDeliverGap> gap_data_;
};
}

void Subscriber::ReceiveDeliver(Flow* flow,
                                SubscriptionState* state,
                                std::unique_ptr<MessageDeliver> deliver) {
  thread_check_.Check();

  switch (deliver->GetMessageType()) {
    case MessageType::mDeliverData: {
      // Deliver data message to the application.
      std::unique_ptr<MessageDeliverData> data(
          static_cast<MessageDeliverData*>(deliver.release()));
      std::unique_ptr<MessageReceived> received(
          new MessageReceivedImpl(std::move(data)));
      state->GetObserver()->OnMessageReceived(flow, received);
      break;
    }
    case MessageType::mDeliverGap: {
      std::unique_ptr<MessageDeliverGap> gap(
          static_cast<MessageDeliverGap*>(deliver.release()));

      if (gap->GetGapType() != GapType::kBenign) {
        DataLossInfoImpl data_loss_info(std::move(gap));
        state->GetObserver()->OnDataLoss(flow, data_loss_info);
      }
      break;
    }
    default:
      RS_ASSERT(false);
  }
}

void Subscriber::ReceiveTerminate(
    Flow* flow,
    SubscriptionState* state,
    std::unique_ptr<MessageUnsubscribe> unsubscribe) {
  SubscriptionStatusImpl sub_status(*state);
  switch (unsubscribe->GetReason()) {
    case MessageUnsubscribe::Reason::kRequested:
      break;
    case MessageUnsubscribe::Reason::kInvalid:
      sub_status.status_ = Status::InvalidArgument("Invalid subscription");
      break;
      // No default, we will be warned about unhandled code.
  }
  state->GetObserver()->OnSubscriptionStatusChange(sub_status);

  auto sub_id = state->GetIDWhichMayChange();
  last_acks_map_.erase(sub_id);
}

}  // namespace rocketspeed
