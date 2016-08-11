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
#include "src/client/subscriptions_map.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"

namespace {

using namespace rocketspeed;
void UserDataCleanup(void* user_data) {
  delete static_cast<Observer*>(user_data);
}
}  // anonymous namespace

namespace rocketspeed {

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////
Subscriber::Subscriber(const ClientOptions& options,
                       EventLoop* event_loop,
                       std::shared_ptr<SubscriberStats> stats,
                       size_t shard_id,
                       size_t max_active_subscriptions,
                       std::shared_ptr<size_t> num_active_subscriptions,
                       TimeoutList<size_t>* hb_timeout_list)
: options_(options)
, event_loop_(event_loop)
, stats_(std::move(stats))
, subscriptions_map_(event_loop_,
                     std::bind(&Subscriber::ReceiveDeliver, this, _1, _2, _3),
                     std::bind(&Subscriber::ReceiveTerminate,
                               this, _1, _2, _3),
                     &UserDataCleanup)
, stream_supervisor_(event_loop_, &subscriptions_map_,
                     std::bind(&Subscriber::ReceiveConnectionStatus, this, _1),
                     options.backoff_strategy,
                     options_.max_silent_reconnects)
, shard_id_(shard_id)
, max_active_subscriptions_(max_active_subscriptions)
, num_active_subscriptions_(std::move(num_active_subscriptions))
, hb_timeout_list_(hb_timeout_list) {
  thread_check_.Check();
  RefreshRouting();
}

void Subscriber::StartSubscription(SubscriptionID sub_id,
                                   SubscriptionParameters parameters,
                                   std::unique_ptr<Observer> observer) {
  thread_check_.Check();

  if (*num_active_subscriptions_ >= max_active_subscriptions_) {
    LOG_WARN(options_.info_log,
             "Subscription limit of %zu reached.",
             max_active_subscriptions_);

    // Cancel subscription
    SubscriptionStatusImpl sub_status(
        sub_id,
        parameters.tenant_id,
        parameters.namespace_id,
        parameters.topic_name,
        parameters.start_seqno);
    sub_status.status_ = Status::InvalidArgument(
        "Invalid subscription as maximum subscription limit reached.");
    observer->OnSubscriptionStatusChange(sub_status);
    return;
  }

  if (!currently_healthy_) {
    SubscriptionStatusImpl status(sub_id, parameters.tenant_id,
                                  parameters.namespace_id,
                                  parameters.topic_name,
                                  parameters.start_seqno);
    status.status_ = Status::ShardUnhealthy();
    observer->OnSubscriptionStatusChange(status);
  }

  auto user_data = static_cast<void*>(observer.release());
  subscriptions_map_.Subscribe(sub_id,
                               parameters.tenant_id,
                               parameters.namespace_id,
                               parameters.topic_name,
                               parameters.start_seqno,
                               user_data);
  (*num_active_subscriptions_)++;
}

void Subscriber::Acknowledge(SubscriptionID sub_id,
                             SequenceNumber acked_seqno) {
  if (!subscriptions_map_.Exists(sub_id)) {
    LOG_WARN(options_.info_log,
             "Cannot acknowledge missing subscription ID (%lld)",
             sub_id.ForLogging());
    return;
  }

  last_acks_map_[sub_id] = acked_seqno;
}

void Subscriber::TerminateSubscription(SubscriptionID sub_id) {
  thread_check_.Check();

  Info info;
  if (Select(sub_id, Info::kTenant, &info)) {
    // Notify the user.
    SourcelessFlow no_flow(event_loop_->GetFlowControl());
    ReceiveTerminate(&no_flow,
                     sub_id,
                     folly::make_unique<MessageUnsubscribe>(
                         info.GetTenant(),
                         sub_id,
                         MessageUnsubscribe::Reason::kRequested));

    // Terminate the subscription, which will invalidate the pointer.
    subscriptions_map_.Unsubscribe(sub_id);
  }

  last_acks_map_.erase(sub_id);
}

Status Subscriber::SaveState(SubscriptionStorage::Snapshot* snapshot,
                             size_t worker_id) {
  Status status;
  subscriptions_map_.Iterate([&](const SubscriptionData& state) {
    if (!status.ok()) {
      return;
    }

    SequenceNumber start_seqno =
        GetLastAcknowledged(state.GetID());
    // Subscription storage stores parameters of subscribe requests that shall
    // be reissued, therefore we must persiste the next sequence number.
    if (start_seqno > 0) {
      ++start_seqno;
    }
    status = snapshot->Append(worker_id,
                              state.GetTenant(),
                              state.GetNamespace().ToString(),
                              state.GetTopicName().ToString(),
                              start_seqno);
  });
  return status;
}

SequenceNumber Subscriber::GetLastAcknowledged(SubscriptionID sub_id) const {
  auto it = last_acks_map_.find(sub_id);
  return it == last_acks_map_.end() ? 0 : it->second;
}

bool Subscriber::Select(
    SubscriptionID sub_id, Info::Flags flags, Info* info) const {
  return subscriptions_map_.Select(sub_id, flags, info);
}

void Subscriber::SetUserData(SubscriptionID sub_id, void* user_data) {
  subscriptions_map_.SetUserData(sub_id, user_data);
}

void Subscriber::RefreshRouting() {
  thread_check_.Check();
  stream_supervisor_.ConnectTo(options_.sharding->GetHost(shard_id_));
  hb_timeout_list_->Add(shard_id_);
}

void Subscriber::NotifyHealthy(bool isHealthy) {
  thread_check_.Check();

  if (currently_healthy_ == isHealthy) {
    return;
  }
  currently_healthy_ = isHealthy;

  LOG_WARN(options_.info_log,
           "Notified that subscriptions for shard %zu are now %s.",
           shard_id_, (isHealthy ? "healthy" : "unhealthy"));

  if (!options_.should_notify_health) {
    return;
  }

  auto start = std::chrono::steady_clock::now();

  subscriptions_map_.Iterate([this, isHealthy](const SubscriptionData& data) {
      Info info;
      bool success = Select(data.GetID(), Info::kAll, &info);
      RS_ASSERT(success);
      SubscriptionStatusImpl status(data.GetID(), info.GetTenant(),
          info.GetNamespace(), info.GetTopic(), info.GetSequenceNumber());
      status.status_ = isHealthy ? Status::OK() : Status::ShardUnhealthy();
      info.GetObserver()->OnSubscriptionStatusChange(status);
    });

  auto delta = std::chrono::steady_clock::now() - start;
  if (delta > std::chrono::milliseconds(10)) {
    auto delta_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(delta).count();
    LOG_WARN(options_.info_log,
             "Took a long time notifying subscriptions of health: %li ms.",
             static_cast<long int>(delta_ms));
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
                                SubscriptionID sub_id,
                                std::unique_ptr<MessageDeliver> deliver) {
  thread_check_.Check();

  Info info;
  bool success = Select(sub_id, Info::kObserver, &info);
  RS_ASSERT(success);

  switch (deliver->GetMessageType()) {
    case MessageType::mDeliverData: {
      // Deliver data message to the application.
      std::unique_ptr<MessageDeliverData> data(
          static_cast<MessageDeliverData*>(deliver.release()));
      std::unique_ptr<MessageReceived> received(
          new MessageReceivedImpl(std::move(data)));
      info.GetObserver()->OnMessageReceived(flow, received);
      break;
    }
    case MessageType::mDeliverGap: {
      std::unique_ptr<MessageDeliverGap> gap(
          static_cast<MessageDeliverGap*>(deliver.release()));

      if (gap->GetGapType() != GapType::kBenign) {
        DataLossInfoImpl data_loss_info(std::move(gap));
        info.GetObserver()->OnDataLoss(flow, data_loss_info);
      }
      break;
    }
    default:
      RS_ASSERT(false);
  }
}

void Subscriber::ReceiveTerminate(
    Flow* flow,
    SubscriptionID sub_id,
    std::unique_ptr<MessageUnsubscribe> unsubscribe) {
  Info info;
  bool success = Select(sub_id, Info::kAll, &info);
  RS_ASSERT(success);

  // The callback would only be called if the state is not null
  // at this point, that is the Client must have Unsubscribed before we receive
  // a Terminate from the server.
  SubscriptionStatusImpl sub_status(sub_id, info.GetTenant(),
      info.GetNamespace(), info.GetTopic(), info.GetSequenceNumber());
  switch (unsubscribe->GetReason()) {
    case MessageUnsubscribe::Reason::kRequested:
      break;
    case MessageUnsubscribe::Reason::kInvalid:
      sub_status.status_ = Status::InvalidArgument("Invalid subscription");
      break;
      // No default, we will be warned about unhandled code.
  }
  RS_ASSERT(info.GetObserver());
  info.GetObserver()->OnSubscriptionStatusChange(sub_status);

  last_acks_map_.erase(sub_id);

  // Decrement number of active subscriptions
  (*num_active_subscriptions_)--;
}

void Subscriber::ReceiveConnectionStatus(bool isHealthy) {
  if (isHealthy) {
    hb_timeout_list_->Add(shard_id_);
  }

  NotifyHealthy(isHealthy);
}
}  // namespace rocketspeed
