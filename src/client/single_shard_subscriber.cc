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
#include "src/util/common/retry_later_sink.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"

namespace {

using namespace rocketspeed;
void UserDataCleanup(void* user_data) {
  delete static_cast<Observer*>(user_data);
}

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

  Slice GetDataSource() const override {
    return data_->GetDataSource();
  }

  Slice GetContents() const override { return data_->GetPayload(); }

 private:
  std::unique_ptr<MessageDeliverData> data_;
};

class DataLossInfoImpl : public DataLossInfo {
 public:
  DataLossInfoImpl() = default;

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

}  // anonymous namespace

namespace rocketspeed {

class ApplicationMessage {
 public:
  enum { kData, kLoss } type;
  Observer* observer;
  TopicUUID uuid;
  std::unique_ptr<MessageReceived> data;
  DataLossInfoImpl data_loss;
};

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
, stats_(std::move(stats))
, subscriptions_map_(event_loop,
                     std::bind(&Subscriber::SendMessage, this, _1, _2),
                     &UserDataCleanup)
, stream_supervisor_(event_loop,
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
, num_active_subscriptions_(std::move(num_active_subscriptions))
, app_sink_(new RetryLaterSink<ApplicationMessage>(
      std::bind(&Subscriber::InvokeApplication, this, _1))) {
  thread_check_.Check();
  RefreshRouting();
}

Subscriber::~Subscriber() {}

BackPressure Subscriber::InvokeApplication(ApplicationMessage& msg) {
  // This is called when calling into application callbacks, e.g. delivering
  // a message, or a gap. It returns BackPressure::None() when the message was
  // processed, otherwise requests the message to be delivered again.
  //
  // Important: msg must not be moved from if backpressure is applied since we
  // need to retry with the same message later.
  Info info;
  if (!Select(msg.uuid, Info::kObserver, &info)) {
    // Subscription has been unsubscribed while backpressure being applied.
    // No need to deliver anything now.
    return BackPressure::None();
  }
  switch (msg.type) {
    case ApplicationMessage::kData: {
      BackPressure bp = BackPressure::None();
      if (info.GetObserver()) {
        bp = info.GetObserver()->OnData(msg.data);
        RS_ASSERT(!bp || msg.data) << "Cannot consume data and retry";
      }
      if (!bp && options_.deliver_callback && msg.data) {
        bp = options_.deliver_callback(msg.data);
        RS_ASSERT(!bp || msg.data) << "Cannot consume data and retry";
      }
      return bp;
    }
    case ApplicationMessage::kLoss: {
      BackPressure bp = BackPressure::None();
      if (info.GetObserver()) {
        bp = info.GetObserver()->OnLoss(msg.data_loss);
      }
      if (!bp && options_.data_loss_callback) {
        bp = options_.data_loss_callback(msg.data_loss);
      }
      return bp;
    }
  }
  RS_ASSERT(false);
  return BackPressure::None();
}

void Subscriber::SendMessage(Flow* flow, std::unique_ptr<Message> message) {
  if (message->GetMessageType() == MessageType::mSubscribe) {
    // If we are sending a subscribe to the server, ensure that the any pending
    // backlog query requests for this subscription are now sent.
    auto subscribe = static_cast<MessageSubscribe*>(message.get());
    TopicUUID uuid(subscribe->GetNamespace(), subscribe->GetTopicName());
    backlog_query_store_->MarkSynced(uuid);
  }
  flow->Write(GetConnection(), message);
}

void Subscriber::InstallHooks(const HooksParameters& params,
                              std::shared_ptr<SubscriberHooks> hooks) {
  hooks_.Install(params, hooks);
  TopicUUID uuid(params.namespace_id, params.topic_name);
  Info info;
  if (Select(uuid, Info::kAll, &info)) {
    hooks_.SubscriptionStarted(params, info.GetSubID());
    SubscriptionStatusImpl status(info.GetSubID(), info.GetTenant(),
        info.GetNamespace(), info.GetTopic());
    status.status_ = currently_healthy_ ? Status::OK() : Status::ShardUnhealthy();
    StatusForHooks sfh(&status, &stream_supervisor_.GetCurrentHost());
    hooks_[info.GetSubID()].SubscriptionExists(sfh);
  }
}

void Subscriber::UnInstallHooks(const HooksParameters& params) {
  hooks_.UnInstall(params);
}

void Subscriber::StartSubscription(SubscriptionID sub_id,
                                   SubscriptionParameters parameters,
                                   std::unique_ptr<Observer> observer) {
  thread_check_.Check();

  if (options_.compatibility_allow_sub_handles) {
    TopicUUID topic(parameters.namespace_id, parameters.topic_name);
    id_to_topic_.emplace(sub_id, std::move(topic));
  }

  if (*num_active_subscriptions_ >= max_active_subscriptions_) {
    LOG_WARN(options_.info_log,
             "Subscription limit of %zu reached.",
             max_active_subscriptions_);

    // Cancel subscription
    SubscriptionStatusImpl sub_status(
        sub_id,
        parameters.tenant_id,
        parameters.namespace_id,
        parameters.topic_name);
    sub_status.status_ = Status::InvalidArgument(
        "Invalid subscription as maximum subscription limit reached.");
    StatusForHooks sfh(&sub_status, &stream_supervisor_.GetCurrentHost());
    hooks_[sub_id].OnSubscriptionStatusChange(sfh);
    if (observer) {
      observer->OnSubscriptionStatusChange(sub_status);
    }
    if (options_.subscription_callback) {
      options_.subscription_callback(sub_status);
    }
    return;
  }

  if (!currently_healthy_) {
    SubscriptionStatusImpl status(sub_id, parameters.tenant_id,
                                  parameters.namespace_id,
                                  parameters.topic_name);
    status.status_ = Status::ShardUnhealthy();
    StatusForHooks sfh(&status, &stream_supervisor_.GetCurrentHost());
    hooks_[sub_id].OnSubscriptionStatusChange(sfh);
    if (observer) {
      observer->OnSubscriptionStatusChange(status);
    }
    if (options_.subscription_callback) {
      options_.subscription_callback(status);
    }
  }

  hooks_.SubscriptionStarted(HooksParameters(parameters), sub_id);
  hooks_[sub_id].OnStartSubscription();
  auto user_data = static_cast<void*>(observer.release());
  subscriptions_map_.Subscribe(sub_id,
                               parameters.tenant_id,
                               parameters.namespace_id,
                               parameters.topic_name,
                               parameters.cursors,
                               user_data);
  (*num_active_subscriptions_)++;
  stats_->active_subscriptions->Set(*num_active_subscriptions_);
}

void Subscriber::Acknowledge(SubscriptionID sub_id,
                             SequenceNumber acked_seqno) {
  TopicUUID uuid;
  if (options_.compatibility_allow_sub_handles) {
    auto it = id_to_topic_.find(sub_id);
    if (it != id_to_topic_.end()) {
      uuid = it->second;
    } else {
      LOG_WARN(options_.info_log,
               "Acknowledge called with unknown ID (%llu)",
               sub_id.ForLogging());
      return;
    }
  } else {
    // TODO(pja) : Allow acknowledging with topic.
    RS_ASSERT(false) << "Not implemented";
  }

  if (!subscriptions_map_.Exists(uuid)) {
    LOG_WARN(options_.info_log,
             "Cannot acknowledge missing subscription ID (%s)",
             uuid.ToString().c_str());
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
  TopicUUID uuid(params.namespace_id, params.topic);
  auto mode = subscriptions_map_.IsSynced(uuid) ?
      BacklogQueryStore::Mode::kPendingSend :
      BacklogQueryStore::Mode::kAwaitingSync;

  if (!params.sub_id) {
    // If sub ID wasn't provided in API, we can recover one from the map.
    // Servers still rely on there being a sub ID.
    Info info;
    if (Select(uuid, Info::kSubID, &info)) {
      params.sub_id = info.GetSubID();
    }
  }

  backlog_query_store_->Insert(
      mode, params.sub_id, std::move(params.namespace_id),
      std::move(params.topic), std::move(params.source), params.seqno,
      std::move(params.callback));
}

void Subscriber::TerminateSubscription(NamespaceID namespace_id,
                                       Topic topic,
                                       SubscriptionID sub_id) {
  thread_check_.Check();

  if (topic.empty() && options_.compatibility_allow_sub_handles) {
    auto it = id_to_topic_.find(sub_id);
    if (it != id_to_topic_.end()) {
      it->second.GetTopicID(&namespace_id, &topic);
      RS_ASSERT_DBG(!topic.empty());
      id_to_topic_.erase(it);
    } else {
      LOG_WARN(options_.info_log,
               "TerminateSubscription called with unsubscribed topic (%s)",
               topic.c_str());
    }
  }

  if (!topic.empty()) {
    TopicUUID uuid(namespace_id, topic);
    Info info;
    if (Select(uuid, Info::kAll, &info)) {
      // Notify the user.
      ProcessUnsubscribe(sub_id, info, Status::OK());

      // Terminate the subscription, which will invalidate the pointer.
      subscriptions_map_.Unsubscribe(uuid);
    }
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
    const TopicUUID& uuid, Info::Flags flags, Info* info) const {
  return subscriptions_map_.Select(uuid, flags, info);
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
      SubscriptionStatusImpl status(data.GetID(), data.GetTenant(),
          data.GetNamespace().ToString(), data.GetTopicName().ToString());
      status.status_ = isHealthy ? Status::OK() : Status::ShardUnhealthy();
      StatusForHooks sfh(&status, &stream_supervisor_.GetCurrentHost());
      hooks_[data.GetID()].OnSubscriptionStatusChange(sfh);
      auto observer = static_cast<Observer*>(data.GetUserData());
      if (observer) {
        observer->OnSubscriptionStatusChange(status);
      }
      if (options_.subscription_callback) {
        options_.subscription_callback(status);
      }
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
            "ReceiveUnsubscribe(%" PRIu64 ", %llu, %d)",
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
      info.GetNamespace(), info.GetTopic());
  sub_status.status_ = status;

  StatusForHooks sfh(&sub_status, &stream_supervisor_.GetCurrentHost());
  hooks_[sub_id].OnSubscriptionStatusChange(sfh);
  if (info.GetObserver()) {
    info.GetObserver()->OnSubscriptionStatusChange(sub_status);
  }
  if (options_.subscription_callback) {
    options_.subscription_callback(sub_status);
  }
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
  const TopicUUID uuid(deliver->GetNamespace(), deliver->GetTopicName());

  LOG_DEBUG(options_.info_log,
            "ReceiveDeliver(%" PRIu64 ", %llu, %s)",
            arg.stream_id,
            sub_id.ForLogging(),
            MessageTypeName(deliver->GetMessageType()));

  if (!subscriptions_map_.ProcessDeliver(flow, *deliver)) {
    // Message didn't match a subscription.
    return;
  }

  switch (deliver->GetMessageType()) {
    case MessageType::mDeliverData: {
      // Deliver data message to the application.
      std::unique_ptr<MessageDeliverData> data(
          static_cast<MessageDeliverData*>(deliver.release()));
      std::unique_ptr<MessageReceived> received(
          new MessageReceivedImpl(std::move(data)));
      hooks_[sub_id].OnMessageReceived(received.get());
      ApplicationMessage msg;
      msg.type = ApplicationMessage::kData;
      msg.uuid = std::move(uuid);
      msg.data = std::move(received);
      flow->Write(app_sink_.get(), msg);
      break;
    }
    case MessageType::mDeliverGap: {
      std::unique_ptr<MessageDeliverGap> gap(
          static_cast<MessageDeliverGap*>(deliver.release()));

      if (gap->GetGapType() != GapType::kBenign) {
        ApplicationMessage msg;
        msg.type = ApplicationMessage::kLoss;
        msg.uuid = std::move(uuid);
        msg.data_loss = DataLossInfoImpl(std::move(gap));
        hooks_[sub_id].OnDataLoss(msg.data_loss);
        flow->Write(app_sink_.get(), msg);
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
