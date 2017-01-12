/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "single_shard_subscriber.h"

#include <memory>

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/backlog_query_store.h"
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

bool DataMatchParams(const SubscriptionData& data,
                     const HooksParameters& params) {
  return params.tenant_id == data.GetTenant() &&
         params.namespace_id == data.GetNamespace() &&
         params.topic_name == data.GetTopicName();
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
                       std::shared_ptr<const IntroParameters> intro_parameters)
: options_(options)
, event_loop_(event_loop)
, stats_(std::move(stats))
, subscriptions_map_(event_loop_,
                     std::bind(&Subscriber::SendMessage, this, _1, _2),
                     &UserDataCleanup)
, stream_supervisor_(event_loop_,
                     this,
                     std::bind(&Subscriber::ReceiveConnectionStatus, this, _1),
                     options.backoff_strategy,
                     options_.max_silent_reconnects,
                     shard_id,
                     intro_parameters)
, backlog_query_store_(
      new BacklogQueryStore(options_.info_log,
                            std::bind(&Subscriber::SendMessage, this, _1, _2),
                            event_loop))
, shard_id_(shard_id)
, max_active_subscriptions_(max_active_subscriptions)
, num_active_subscriptions_(std::move(num_active_subscriptions)) {
  thread_check_.Check();
  RefreshRouting();
}

Subscriber::~Subscriber() {}

void Subscriber::SendMessage(Flow* flow, std::unique_ptr<Message> message) {
  if (message->GetMessageType() == MessageType::mSubscribe) {
    // If we are sending a subscribe to the server, ensure that the any pending
    // backlog query requests for this subscription are now sent.
    auto subscribe = static_cast<MessageSubscribe*>(message.get());
    backlog_query_store_->MarkSynced(subscribe->GetSubID());
  }
  flow->Write(GetConnection(), message);
}

void Subscriber::InstallHooks(const HooksParameters& params,
                              std::shared_ptr<SubscriberHooks> hooks) {
  hooks_.Install(params, hooks);
  // TODO: this linear search to be removed when we change the data structure to
  // be topic-oriented
  subscriptions_map_.Iterate([&](const SubscriptionData& data) mutable {
    bool keep_iterating = true;
    if (DataMatchParams(data, params)) {
      keep_iterating = false;
      hooks_.SubscriptionStarted(params, data.GetID());
      Info info;
      bool success = Select(data.GetID(), Info::kAll, &info);
      RS_ASSERT(success);
      SubscriptionStatusImpl status(data.GetID(), info.GetTenant(),
          info.GetNamespace(), info.GetTopic(), info.GetSequenceNumber());
      status.status_ = currently_healthy_ ? Status::OK() : Status::ShardUnhealthy();
      StatusForHooks sfh(&status, &stream_supervisor_.GetCurrentHost());
      hooks_[data.GetID()].SubscriptionExists(sfh);
    }
    return keep_iterating;
  });
}

void Subscriber::UnInstallHooks(const HooksParameters& params) {
  hooks_.UnInstall(params);
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
    StatusForHooks sfh(&sub_status, &stream_supervisor_.GetCurrentHost());
    hooks_[sub_id].OnSubscriptionStatusChange(sfh);
    observer->OnSubscriptionStatusChange(sub_status);
    return;
  }

  if (!currently_healthy_) {
    SubscriptionStatusImpl status(sub_id, parameters.tenant_id,
                                  parameters.namespace_id,
                                  parameters.topic_name,
                                  parameters.start_seqno);
    status.status_ = Status::ShardUnhealthy();
    StatusForHooks sfh(&status, &stream_supervisor_.GetCurrentHost());
    hooks_[sub_id].OnSubscriptionStatusChange(sfh);
    observer->OnSubscriptionStatusChange(status);
  }

  hooks_.SubscriptionStarted(HooksParameters(parameters), sub_id);
  hooks_[sub_id].OnStartSubscription();
  auto user_data = static_cast<void*>(observer.release());
  subscriptions_map_.Subscribe(sub_id,
                               parameters.tenant_id,
                               parameters.namespace_id,
                               parameters.topic_name,
                               parameters.start_seqno,
                               user_data);
  (*num_active_subscriptions_)++;
  stats_->active_subscriptions->Set(*num_active_subscriptions_);
}

void Subscriber::Acknowledge(SubscriptionID sub_id,
                             SequenceNumber acked_seqno) {
  if (!subscriptions_map_.Exists(sub_id)) {
    LOG_WARN(options_.info_log,
             "Cannot acknowledge missing subscription ID (%lld)",
             sub_id.ForLogging());
    return;
  }

  hooks_[sub_id].OnAcknowledge(acked_seqno);
  last_acks_map_[sub_id] = acked_seqno;
}

void Subscriber::HasMessageSince(HasMessageSinceParams params) {
  // We need to wait for the subscription to exist on the server before we can
  // send the BacklogQuery. If it is already synced, we put it in the pending
  // list to be sent when the connection is ready, otherwise we put it into a
  // waiting queue until the relevant subscription is synced.
  auto mode = subscriptions_map_.IsSynced(params.sub_id) ?
      BacklogQueryStore::Mode::kPendingSend :
      BacklogQueryStore::Mode::kAwaitingSync;

  backlog_query_store_->Insert(
      mode, params.sub_id, std::move(params.namespace_id),
      std::move(params.topic), std::move(params.epoch), params.seqno,
      std::move(params.callback));
}

void Subscriber::TerminateSubscription(SubscriptionID sub_id) {
  thread_check_.Check();

  Info info;
  if (Select(sub_id, Info::kAll, &info)) {
    // Notify the user.
    ProcessUnsubscribe(sub_id, info, Status::OK());

    // Terminate the subscription, which will invalidate the pointer.
    subscriptions_map_.Unsubscribe(sub_id);
  }

  hooks_[sub_id].OnTerminateSubscription();
  hooks_.SubscriptionEnded(sub_id);
  last_acks_map_.erase(sub_id);
}

Status Subscriber::SaveState(SubscriptionStorage::Snapshot* snapshot,
                             size_t worker_id) {
  Status status;
  subscriptions_map_.Iterate([&](const SubscriptionData& state) {
    if (!status.ok()) {
      return true;
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
    return true;
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
      StatusForHooks sfh(&status, &stream_supervisor_.GetCurrentHost());
      hooks_[data.GetID()].OnSubscriptionStatusChange(sfh);
      info.GetObserver()->OnSubscriptionStatusChange(status);
      return true;
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

void Subscriber::ReceiveConnectionStatus(bool isHealthy) {
  NotifyHealthy(isHealthy);
}

void Subscriber::ConnectionChanged() {
  if (GetConnection()) {
    subscriptions_map_.StartSync();
    backlog_query_store_->StartSync();
  } else {
    subscriptions_map_.StopSync();
    backlog_query_store_->StopSync();
  }
}

void Subscriber::ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe> arg) {
  auto sub_id = arg.message->GetSubID();

  LOG_DEBUG(options_.info_log,
            "ReceiveUnsubscribe(%llu, %llu, %d)",
            arg.stream_id,
            sub_id.ForLogging(),
            static_cast<int>(arg.message->GetMessageType()));

  Info info;
  if (!subscriptions_map_.ProcessUnsubscribe(
      arg.flow, *arg.message, Info::kAll, &info)) {
    // Didn't match a subscription.
    return;
  }

  Status status;
  switch (arg.message->GetReason()) {
    case MessageUnsubscribe::Reason::kRequested:
      break;
    case MessageUnsubscribe::Reason::kInvalid:
      status = Status::InvalidArgument("Invalid subscription");
      break;
      // No default, we will be warned about unhandled code.
  }
  ProcessUnsubscribe(sub_id, info, status);
}

void Subscriber::ProcessUnsubscribe(
    SubscriptionID sub_id, Info& info, Status status) {
  SubscriptionStatusImpl sub_status(sub_id, info.GetTenant(),
      info.GetNamespace(), info.GetTopic(), info.GetSequenceNumber());
  sub_status.status_ = status;

  RS_ASSERT(info.GetObserver());
  StatusForHooks sfh(&sub_status, &stream_supervisor_.GetCurrentHost());
  hooks_[sub_id].OnSubscriptionStatusChange(sfh);
  info.GetObserver()->OnSubscriptionStatusChange(sub_status);
  hooks_[sub_id].OnReceiveTerminate();
  last_acks_map_.erase(sub_id);

  // Decrement number of active subscriptions
  (*num_active_subscriptions_)--;
  stats_->active_subscriptions->Set(*num_active_subscriptions_);
}

void Subscriber::ReceiveDeliver(StreamReceiveArg<MessageDeliver> arg) {
  thread_check_.Check();
  auto flow = arg.flow;
  auto& deliver = arg.message;
  auto sub_id = deliver->GetSubID();

  LOG_DEBUG(options_.info_log,
            "ReceiveDeliver(%llu, %llu, %s)",
            arg.stream_id,
            sub_id.ForLogging(),
            MessageTypeName(deliver->GetMessageType()));

  if (!subscriptions_map_.ProcessDeliver(flow, *deliver)) {
    // Message didn't match a subscription.
    return;
  }
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
      hooks_[sub_id].OnMessageReceived(received.get());
      info.GetObserver()->OnMessageReceived(flow, received);
      break;
    }
    case MessageType::mDeliverGap: {
      std::unique_ptr<MessageDeliverGap> gap(
          static_cast<MessageDeliverGap*>(deliver.release()));

      if (gap->GetGapType() != GapType::kBenign) {
        DataLossInfoImpl data_loss_info(std::move(gap));
        hooks_[sub_id].OnDataLoss(data_loss_info);
        info.GetObserver()->OnDataLoss(flow, data_loss_info);
      }
      break;
    }
    default:
      RS_ASSERT(false);
  }
}

void Subscriber::ReceiveBacklogFill(StreamReceiveArg<MessageBacklogFill> arg) {
  thread_check_.Check();
  backlog_query_store_->ProcessBacklogFill(*arg.message);
}

}  // namespace rocketspeed
