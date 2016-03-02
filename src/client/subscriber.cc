// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "subscriber.h"

#include <cmath>
#include <chrono>
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
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
#include "src/port/port.h"
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/common/rate_limiter_sink.h"
#include "src/util/timeout_list.h"
#include "src/util/xxhash.h"

namespace rocketspeed {

///////////////////////////////////////////////////////////////////////////////
void SubscriptionState::Terminate(const std::shared_ptr<Logger>& info_log,
                                  SubscriptionID sub_id,
                                  MessageUnsubscribe::Reason reason) {
  ThreadCheck::Check();

  class SubscriptionStatusImpl : public SubscriptionStatus {
   public:
    SubscriptionID sub_id_;
    const SubscriptionState& sub_state_;
    Status status_;

    SubscriptionStatusImpl(SubscriptionID _sub_id,
                           const SubscriptionState& sub_state)
    : sub_id_(_sub_id), sub_state_(sub_state) {}

    SubscriptionHandle GetSubscriptionHandle() const override {
      return sub_id_;
    }

    TenantID GetTenant() const override { return sub_state_.GetTenant(); }

    const NamespaceID& GetNamespace() const override {
      return sub_state_.GetNamespace();
    }

    const Topic& GetTopicName() const override {
      return sub_state_.GetTopicName();
    }

    SequenceNumber GetSequenceNumber() const override {
      return sub_state_.GetExpected();
    }

    bool IsSubscribed() const override { return false; }

    const Status& GetStatus() const override { return status_; }
  } sub_status(sub_id, *this);
  switch (reason) {
    case MessageUnsubscribe::Reason::kRequested:
      LOG_INFO(info_log,
               "Unsubscribed ID(%" PRIu64 ") on Topic(%s, %s)@%" PRIu64,
               sub_id,
               GetNamespace().c_str(),
               topic_name_.c_str(),
               expected_seqno_);
      break;
    case MessageUnsubscribe::Reason::kInvalid:
      LOG_INFO(info_log,
               "Kicked subscription ID (%" PRIu64 ") on Topic(%s, %s)@%" PRIu64,
               sub_id,
               GetNamespace().c_str(),
               topic_name_.c_str(),
               expected_seqno_);
      sub_status.status_ = Status::InvalidArgument("Invalid subscription");
      break;
    case MessageUnsubscribe::Reason::kBackOff:
      RS_ASSERT(false);
      sub_status.status_ =
          Status::InternalError("Received backoff termination");
      break;
      // No default, we will be warned about unhandled code.
  }

  RS_ASSERT(!!observer_);
  observer_->OnSubscriptionStatusChange(sub_status);
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

void SubscriptionState::ReceiveMessage(
    Flow* flow,
    const std::shared_ptr<Logger>& info_log,
    std::unique_ptr<MessageDeliver> deliver) {
  RS_ASSERT(!!observer_);
  ThreadCheck::Check();

  if (!ProcessMessage(info_log, *deliver)) {
    return;
  }

  switch (deliver->GetMessageType()) {
    case MessageType::mDeliverData: {
      // Deliver data message to the application.
      std::unique_ptr<MessageDeliverData> data(
          static_cast<MessageDeliverData*>(deliver.release()));
      std::unique_ptr<MessageReceived> received(
          new MessageReceivedImpl(std::move(data)));
      observer_->OnMessageReceived(flow, received);
      break;
    }
    case MessageType::mDeliverGap: {
      std::unique_ptr<MessageDeliverGap> gap(
          static_cast<MessageDeliverGap*>(deliver.release()));

      if (gap->GetGapType() != GapType::kBenign) {
        std::unique_ptr<DataLossInfo> data_loss_info(
            new DataLossInfoImpl(std::move(gap)));
        observer_->OnDataLoss(flow, data_loss_info);
      }
      break;
    }
    default:
      RS_ASSERT(false);
  }
}

bool SubscriptionState::ProcessMessage(const std::shared_ptr<Logger>& info_log,
                                       const MessageDeliver& deliver) {
  ThreadCheck::Check();

  const auto current = deliver.GetSequenceNumber(),
             previous = deliver.GetPrevSequenceNumber();
  RS_ASSERT(current >= previous);

  if (expected_seqno_ > current || expected_seqno_ < previous ||
      (expected_seqno_ == 0 && previous != 0) ||
      (expected_seqno_ != 0 && previous == 0)) {
    LOG_WARN(info_log,
             "Duplicate message %" PRIu64 "-%" PRIu64
             " on Topic(%s, %s) expected %" PRIu64,
             previous,
             current,
             GetNamespace().c_str(),
             topic_name_.c_str(),
             expected_seqno_);
    return false;
  }

  const char* type_description =
      deliver.GetMessageType() == MessageType::mDeliverGap ? "gap" : "data";
  LOG_DEBUG(info_log,
            "Received %s %" PRIu64 "-%" PRIu64 " on Topic(%s, %s)@%" PRIu64,
            type_description,
            previous,
            current,
            GetNamespace().c_str(),
            topic_name_.c_str(),
            expected_seqno_);

  expected_seqno_ = current + 1;
  return true;
}

////////////////////////////////////////////////////////////////////////////////
class SubscriberStats {
 public:
  explicit SubscriberStats(const std::string& prefix) {
    active_subscriptions = all.AddCounter(prefix + "active_subscriptions");
    unsubscribes_invalid_handle =
        all.AddCounter(prefix + "unsubscribes_invalid_handle");
  }

  Counter* active_subscriptions;
  Counter* unsubscribes_invalid_handle;
  Statistics all;
};

////////////////////////////////////////////////////////////////////////////////
Subscriber::Subscriber(const ClientOptions& options,
                       EventLoop* event_loop,
                       std::shared_ptr<SubscriberStats> stats,
                       std::unique_ptr<SubscriptionRouter> router)
: options_(options)
, event_loop_(event_loop)
, stats_(std::move(stats))
, backoff_until_time_(0)
, last_send_time_(0)
, consecutive_goodbyes_count_(0)
, rng_(ThreadLocalPRNG())
, last_router_version_(0)
, router_(std::move(router))
, pending_subscriptions_(event_loop) {
  thread_check_.Check();

  // Cannot use InstallSource here because it is asynchronous.
  // It must register synchronously, otherwise MultiThreadedSubscriber
  // can destroy object before the registration actually could be completed.
  std::function<void(Flow*, SubscriptionID)> cb =
      [this](Flow* flow, SubscriptionID sub_id) {
        auto it = subscriptions_.find(sub_id);
        SubscriptionState* sub_state =
            it == subscriptions_.end() ? nullptr : &it->second;
        ProcessPendingSubscription(flow, sub_id, sub_state);
      };
  event_loop_->GetFlowControl()->Register(&pending_subscriptions_, cb);

  // This could return nullptr only due to malloc failure.
  start_timer_callback_ = event_loop_->CreateTimedEventCallback(
      [this]() { Tick(); }, options_.timer_period);
  start_timer_callback_->Enable();
}

Subscriber::~Subscriber() {
  thread_check_.Check();
  CloseServerStream();
  event_loop_->GetFlowControl()->UnregisterSource(&pending_subscriptions_);
}

void Subscriber::CheckInvariants() {
  RS_ASSERT(server_stream_ ||
            (!server_stream_ && pending_subscriptions_.Empty()));
}

void Subscriber::StartSubscription(SubscriptionID sub_id,
                                   SubscriptionParameters parameters,
                                   std::unique_ptr<Observer> observer) {
  thread_check_.Check();
  SubscriptionState* sub_state;
  {  // Store the subscription state.
    auto tn = TenantAndNamespace{std::move(parameters.tenant_id),
                                 std::move(parameters.namespace_id)};
    auto emplace_result = subscriptions_.emplace(
        sub_id,
        SubscriptionState(thread_check_,
                          std::move(parameters),
                          std::move(observer),
                          tenant_and_namespace_factory_.GetFlyweight(tn)));
    if (!emplace_result.second) {
      LOG_ERROR(
          options_.info_log, "Duplicate subscription ID(%" PRIu64 ")", sub_id);
      RS_ASSERT(false);
      return;
    }
    sub_state = &emplace_result.first->second;
  }

  LOG_INFO(options_.info_log,
           "Subscribed on Topic(%s,%s)@%" PRIu64 " Tenant(%u) ID(%" PRIu64 ")",
           sub_state->GetNamespace().c_str(),
           sub_state->GetTopicName().c_str(),
           sub_state->GetExpected(),
           sub_state->GetTenant(),
           sub_id);

  // Issue subscription.
  pending_subscriptions_.Add(sub_id);
}

void Subscriber::Acknowledge(SubscriptionID sub_id,
                             SequenceNumber acked_seqno) {
  if (subscriptions_.count(sub_id) == 0) {
    LOG_WARN(options_.info_log,
             "Cannot acknowledge missing subscription ID (%" PRIu64 ")",
             sub_id);
    return;
  }

  last_acks_map_[sub_id] = acked_seqno;
}

void Subscriber::TerminateSubscription(SubscriptionID sub_id) {
  thread_check_.Check();

  // Remove subscription state entry.
  auto it = subscriptions_.find(sub_id);
  if (it == subscriptions_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot remove missing subscription ID(%" PRIu64 ")",
             sub_id);
    stats_->unsubscribes_invalid_handle->Add(1);
    return;
  }
  SubscriptionState sub_state(std::move(it->second));
  subscriptions_.erase(it);

  last_acks_map_.erase(sub_id);

  // Update subscription state, which announces subscription status to the
  // application.
  sub_state.Terminate(
      options_.info_log, sub_id, MessageUnsubscribe::Reason::kRequested);

  // Issue unsubscribe request.
  pending_subscriptions_.Add(sub_id);
}

SequenceNumber Subscriber::GetLastAcknowledged(SubscriptionID sub_id) const {
  auto it = last_acks_map_.find(sub_id);
  return it == last_acks_map_.end() ? 0 : it->second;
}

Status Subscriber::SaveState(SubscriptionStorage::Snapshot* snapshot,
                             size_t worker_id) {
  for (const auto& entry : subscriptions_) {
    const SubscriptionState* sub_state = &entry.second;
    SequenceNumber start_seqno = GetLastAcknowledged(entry.first);
    // Subscription storage stores parameters of subscribe requests that shall
    // be reissued, therefore we must persiste the next sequence number.
    if (start_seqno > 0) {
      ++start_seqno;
    }
    Status status = snapshot->Append(worker_id,
                                     sub_state->GetTenant(),
                                     sub_state->GetNamespace(),
                                     sub_state->GetTopicName(),
                                     start_seqno);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

void Subscriber::CloseServerStream() {
  if (server_stream_) {
    event_loop_->GetFlowControl()->UnregisterSink(server_stream_.get());
  }
  if (limited_server_stream_) {
    event_loop_->GetFlowControl()->UnregisterSink(limited_server_stream_.get());
  }

  limited_server_stream_.reset();
  server_stream_.reset();

  // If server stream is closed there is no point to keep old events.
  // It will reissue all subscriptions anyway on new connection.
  pending_subscriptions_.Clear();
  recent_terminations_.Clear();

  CheckInvariants();
}

void Subscriber::CheckRouterVersion() {
  // Close the connection if the router has changed.
  const auto current_router_version = router_->GetVersion();
  if (last_router_version_ != current_router_version) {
    if (server_stream_) {
      LOG_INFO(options_.info_log,
               "Configuration version has changed %zu -> %zu",
               last_router_version_,
               current_router_version);

      CloseServerStream();
    }
  }
}

void Subscriber::UpdateRecentTerminations() {
  // Evict entries from a list of recently sent unsubscribe messages.
  recent_terminations_.ProcessExpired(
      options_.unsubscribe_deduplication_timeout, [](SubscriptionID) {}, -1);
}

void Subscriber::RestoreServerStream() {
  RS_ASSERT(!server_stream_);

  CloseServerStream(); // it must be closed already

  // Check if we're still in backoff mode
  const uint64_t now = options_.env->NowMicros();
  if (now <= backoff_until_time_) {
    return;
  }

  // Get router version of the host that we will fetch later on.
  last_router_version_ = router_->GetVersion();
  // Get the server's address.
  server_host_ = router_->GetHost();
  if (!server_host_) {
    LOG_WARN(options_.info_log, "Failed to obtain Copilot address");
    // We'll try to obtain the address and reconnect on next occasion.
    return;
  }

  // And create socket to it.
  server_stream_ = event_loop_->OpenStream(server_host_);
  if (!server_stream_) {
    LOG_ERROR(options_.info_log,
              "Failed to open connection to %s",
              server_host_.ToString().c_str());
    return;
  }
  server_stream_->SetReceiver(this);

  if (options_.subscription_rate_limit > 0) {
    using namespace std::chrono;
    const size_t actual_rate =
      std::max(1UL,
        options_.subscription_rate_limit *
        duration_cast<milliseconds>(options_.timer_period).count() / 1000);
    limited_server_stream_ =
        folly::make_unique<RateLimiterSink<SharedTimestampedString>>(
            actual_rate, options_.timer_period, server_stream_.get());
    LOG_INFO(options_.info_log,
             "Applied subscription rate limit %zu per second (%zu per %lld ms)",
             options_.subscription_rate_limit, actual_rate,
             static_cast<long long>(options_.timer_period.count()));
  }

  LOG_INFO(options_.info_log,
           "Reconnected to %s on stream %llu",
           server_host_.ToString().c_str(),
           server_stream_->GetLocalID());

  // Reissue all active subscriptions for new connection
  for (auto& entry : subscriptions_) {
    SubscriptionID sub_id = entry.first;
    pending_subscriptions_.Add(sub_id);
  }

  CheckInvariants();
}

void Subscriber::ProcessPendingSubscription(Flow *flow,
                                            SubscriptionID sub_id,
                                            SubscriptionState* sub_state) {
  thread_check_.Check();
  RS_ASSERT(flow);

  UpdateRecentTerminations();
  CheckRouterVersion();

  // If we have no subscriptions and some pending requests, close the
  // connection. This way we unsubscribe all of them.
  if (server_stream_ && subscriptions_.empty()) {
    LOG_INFO(options_.info_log,
             "Closing stream (%llu) with no active subscriptions",
             server_stream_->GetLocalID());
    CloseServerStream();

    // We've just sent a message.
    last_send_time_ = options_.env->NowMicros();
    return;
  }

  if (!server_stream_) {
    RestoreServerStream();
    // Once connection is restored all pending subscriptions will be
    // invalidated and repopulated again into the set,
    // we can abort processing for now and wait for the next tick
    return;
  }

  CheckInvariants();

  if (!sub_state) {
    // Process unsubscribe request
    if (recent_terminations_.Contains(sub_id)) {
      // Skip the unsubscribe message if we sent it recently.
      return;
    }

    // Record the fact, so we don't send duplicates of the message.
    recent_terminations_.Add(sub_id);
    // Record last send time for back-off logic.
    last_send_time_ = options_.env->NowMicros();

    MessageUnsubscribe unsubscribe(
        GuestTenant, sub_id, MessageUnsubscribe::Reason::kRequested);
    WriteToServerStream(flow, unsubscribe);

    LOG_INFO(options_.info_log, "Unsubscribed ID (%" PRIu64 ")", sub_id);
  } else {
    // Process subscribe request
    MessageSubscribe subscribe(sub_state->GetTenant(),
                               sub_state->GetNamespace(),
                               sub_state->GetTopicName(),
                               sub_state->GetExpected(),
                               sub_id);
    WriteToServerStream(flow, subscribe);

    // Remember last sent time for backoff.
    last_send_time_ = options_.env->NowMicros();

    LOG_INFO(options_.info_log, "Subscribed ID (%" PRIu64 ")", sub_id);
  }
}

void Subscriber::WriteToServerStream(Flow* flow, const Message& msg) {
  RS_ASSERT(flow);
  RS_ASSERT(server_stream_);

  auto ts = server_stream_->ToTimestampedString(msg);
  bool can_write_more = true;
  if (limited_server_stream_) {
    can_write_more = flow->Write(limited_server_stream_.get(), ts);
  } else {
    can_write_more = flow->Write(server_stream_.get(), ts);
  }

  if (!can_write_more) {
    LOG_INFO(options_.info_log, "Backoff when syncing subscriptions");
  }
}

void Subscriber::Tick() {
  thread_check_.Check();

  UpdateRecentTerminations();
  CheckRouterVersion();

  if (!server_stream_ && !subscriptions_.empty()) {
    RestoreServerStream();
  }
}

void Subscriber::ReceiveDeliver(StreamReceiveArg<MessageDeliver> arg) {
  consecutive_goodbyes_count_ = 0;

  auto deliver = std::move(arg.message);
  // Find the right subscription and deliver the message to it.
  SubscriptionID sub_id = deliver->GetSubID();
  auto it = subscriptions_.find(sub_id);
  if (it != subscriptions_.end()) {
    it->second.ReceiveMessage(arg.flow, options_.info_log, std::move(deliver));
  } else {
    UpdateRecentTerminations();
    if (recent_terminations_.Contains(sub_id)) {
      LOG_DEBUG(options_.info_log,
                "Subscription ID(%" PRIu64
                ") for delivery not found, unsubscribed recently",
                sub_id);
    } else {
      LOG_WARN(options_.info_log,
               "Subscription ID(%" PRIu64 ") for delivery not found",
               sub_id);
      // Issue unsubscribe request.
      pending_subscriptions_.Add(sub_id);
    }
  }
}

void Subscriber::ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe> arg) {
  consecutive_goodbyes_count_ = 0;

  auto unsubscribe = std::move(arg.message);
  const SubscriptionID sub_id = unsubscribe->GetSubID();
  // Find the right subscription and deliver the message to it.
  auto it = subscriptions_.find(sub_id);
  if (it == subscriptions_.end()) {
    LOG_WARN(options_.info_log,
             "Received unsubscribe with unrecognised ID(%" PRIu64 ")",
             sub_id);
    return;
  }

  if (unsubscribe->GetReason() == MessageUnsubscribe::Reason::kBackOff) {
    // Handle back off unsubscribes separately, as they don't change the state
    // of the subscription (Copilot could send goodbye message equivalently).
    LOG_INFO(options_.info_log,
             "Received back off for ID(%" PRIu64 "), resubscribing",
             unsubscribe->GetSubID());

    // Reissue subscription.
    pending_subscriptions_.Add(sub_id);

    // Do not terminate subscription in this case.
    return;
  }

  // Terminate subscription, and notify the application.
  it->second.Terminate(options_.info_log, sub_id, unsubscribe->GetReason());
  // Remove the corresponding state entry.
  subscriptions_.erase(it);
  last_acks_map_.erase(sub_id);
}

void Subscriber::ReceiveGoodbye(StreamReceiveArg<MessageGoodbye> arg) {
  thread_check_.Check();

  const auto origin = arg.stream_id;

  // A duration type unit-compatible with BaseEnv::NowMicros().
  typedef std::chrono::microseconds EnvClockDuration;

  CloseServerStream();

  // Notify the router.
  router_->MarkHostDown(server_host_);

  // Failed to reconnect, apply back off logic.
  ++consecutive_goodbyes_count_;
  EnvClockDuration backoff_initial = options_.backoff_initial;
  RS_ASSERT(consecutive_goodbyes_count_ > 0);
  double backoff_value = static_cast<double>(backoff_initial.count()) *
                         std::pow(static_cast<double>(options_.backoff_base),
                                  consecutive_goodbyes_count_ - 1);
  EnvClockDuration backoff_limit = options_.backoff_limit;
  backoff_value =
      std::min(backoff_value, static_cast<double>(backoff_limit.count()));
  backoff_value *= options_.backoff_distribution(&rng_);
  const uint64_t backoff_period = static_cast<uint64_t>(backoff_value);
  // We count backoff period from the time of sending the last message, so
  // that we can attempt to reconnect right after getting goodbye message, if
  // we got it after long period of inactivity.
  backoff_until_time_ = last_send_time_ + backoff_period;

  LOG_WARN(options_.info_log,
           "Received %zu goodbye messages in a row (on stream %llu),"
           " back off for %" PRIu64 " ms",
           consecutive_goodbyes_count_,
           origin,
           backoff_period / 1000);
}

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
      : sub_id_(0), status_(status) {}

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

      bool IsSubscribed() const override { return status_.IsSubscribed(); }

      const Status& GetStatus() const override { return status_.GetStatus(); }
    } down_status(up_status);

    for (const auto& entry : downstream_observers_) {
      down_status.sub_id_ = entry.first;
      entry.second->OnSubscriptionStatusChange(down_status);
    }
    // State of downstream subscriptions might have been removed, if we got this
    // notification as a confirmation that TerminateSubscription(...) finished.
    if (!up_status.IsSubscribed()) {
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
          up_status.GetSubscriptionHandle());
    }
  }

  void OnDataLoss(Flow* flow, std::unique_ptr<DataLossInfo>& up_info) override {
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
      auto down_info = folly::make_unique<DataLossInfoImpl>();
      down_info->id_ = entry.first;
      down_info->type_ = up_info->GetLossType();
      down_info->first_ = up_info->GetFirstSequenceNumber();
      down_info->last_ = up_info->GetLastSequenceNumber();
      std::unique_ptr<DataLossInfo> tmp(std::move(down_info));
      entry.second->OnDataLoss(flow, tmp);
    }
  }
};

}  // namespace details

TailCollapsingSubscriber::TailCollapsingSubscriber(
    std::unique_ptr<Subscriber> subscriber)
: subscriber_(std::move(subscriber))
, upstream_subscriptions_(
      [this](SubscriptionID sub_id) { return subscriber_->GetState(sub_id); }) {
}

void TailCollapsingSubscriber::StartSubscription(
    SubscriptionID downstream_id,
    SubscriptionParameters parameters,
    std::unique_ptr<Observer> observer) {
  SubscriptionID upstream_id;
  SubscriptionState* upstream_state;
  std::tie(upstream_id, upstream_state) = upstream_subscriptions_.Find(
      parameters.namespace_id, parameters.topic_name);
  if (upstream_state) {
    RS_ASSERT(upstream_state->GetNamespace() == parameters.namespace_id);
    RS_ASSERT(upstream_state->GetTopicName() == parameters.topic_name);
    // There exists a subscription on the topic already.
    detail::TailCollapsingObserver* collapsing_observer;
    if (special_observers_.count(upstream_state->GetObserver()) == 0) {
      {  // There exists only one subscription on the topic, set up
         // multiplexing.
        collapsing_observer = new detail::TailCollapsingObserver(this);
        auto result = special_observers_.insert(collapsing_observer);
        (void)result;
        RS_ASSERT(result.second);
      }
      std::unique_ptr<Observer> old_observer(collapsing_observer);
      upstream_state->SwapObserver(&old_observer);

      // Add existing subscription to the multiplexer.
      auto result = collapsing_observer->downstream_observers_.emplace(
          upstream_id, std::move(old_observer));
      (void)result;
      RS_ASSERT(result.second);
    } else {
      // We can safely downcast.
      collapsing_observer = static_cast<detail::TailCollapsingObserver*>(
          upstream_state->GetObserver());
    }
    RS_ASSERT(collapsing_observer);

    {  // We've already set up multiplexing for the upstream subscription, just
       // add the new downstream subscription.
      auto result = collapsing_observer->downstream_observers_.emplace(
          downstream_id, std::move(observer));
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
        downstream_id, parameters, std::move(observer));
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
  auto* upstream_state = subscriber_->GetState(upstream_id);
  if (!upstream_state) {
    RS_ASSERT(upstream_id == downstream_id);
    // The subscription just doesn't exist, this is fine.
    return;
  }

  if (special_observers_.count(upstream_state->GetObserver()) == 0) {
    // This subscription is unique, just unsubscribe.
    upstream_subscriptions_.Remove(upstream_state->GetNamespace(),
                                   upstream_state->GetTopicName(),
                                   upstream_id);
    subscriber_->TerminateSubscription(upstream_id);
    upstream_state = nullptr;
  } else {
    // This subscription is multiplexed, we can downcast the observer.
    auto* collapsing_observer = static_cast<detail::TailCollapsingObserver*>(
        upstream_state->GetObserver());

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
      auto result = special_observers_.erase(upstream_state->GetObserver());
      (void)result;
      RS_ASSERT(result);

      subscriber_->TerminateSubscription(upstream_id);
      upstream_state = nullptr;
    }
  }
}

Status TailCollapsingSubscriber::SaveState(
    SubscriptionStorage::Snapshot* snapshot, size_t worker_id) {
  // TODO(t10075879)
  return Status::NotSupported("");
}

///////////////////////////////////////////////////////////////////////////////
MultiShardSubscriber::MultiShardSubscriber(
    const ClientOptions& options,
    EventLoop* event_loop,
    std::shared_ptr<SubscriberStats> stats)
: options_(options), event_loop_(event_loop), stats_(std::move(stats)) {
}

MultiShardSubscriber::~MultiShardSubscriber() {
  subscribers_.clear();
}

void MultiShardSubscriber::StartSubscription(
    SubscriptionID sub_id,
    SubscriptionParameters parameters,
    std::unique_ptr<Observer> observer) {
  // Determine the shard ID.
  size_t shard_id = options_.sharding->GetShard(parameters.namespace_id,
                                                parameters.topic_name);
  subscription_to_shard_.emplace(sub_id, shard_id);

  // Find or create a subscriber for this shard.
  auto it = subscribers_.find(shard_id);
  if (it == subscribers_.end()) {
    // Subscriber is missing, create and start one.
    std::unique_ptr<SubscriberIf> subscriber(new Subscriber(
        options_, event_loop_, stats_, options_.sharding->GetRouter(shard_id)));
    if (options_.collapse_subscriptions_to_tail) {
      // TODO(t10132320)
      RS_ASSERT(parameters.start_seqno == 0);
      auto sub = static_cast<Subscriber*>(subscriber.release());
      subscriber.reset(
          new TailCollapsingSubscriber(std::unique_ptr<Subscriber>(sub)));
    }

    // Put it back in the map so it can be resued.
    auto result = subscribers_.emplace(shard_id, std::move(subscriber));
    RS_ASSERT(result.second);
    it = result.first;
  }

  it->second->StartSubscription(
      sub_id, std::move(parameters), std::move(observer));
}

void MultiShardSubscriber::Acknowledge(SubscriptionID sub_id,
                                       SequenceNumber seqno) {
  if (auto subscriber = GetSubscriberForSubscription(sub_id)) {
    subscriber->Acknowledge(sub_id, seqno);
  }
}

void MultiShardSubscriber::TerminateSubscription(SubscriptionID sub_id) {
  if (auto subscriber = GetSubscriberForSubscription(sub_id)) {
    subscriber->TerminateSubscription(sub_id);
    if (subscriber->Empty()) {
      // Subscriber no longer serves any subscriptions, destroy it
      auto it = subscription_to_shard_.find(sub_id);
      RS_ASSERT(it != subscription_to_shard_.end());
      subscribers_.erase(it->second);
    }
  }
  // Remove the mapping from subscription ID to a shard.
  subscription_to_shard_.erase(sub_id);
}

Status MultiShardSubscriber::SaveState(SubscriptionStorage::Snapshot* snapshot,
                                       size_t worker_id) {
  for (const auto& subscriber : subscribers_) {
    auto st = subscriber.second->SaveState(snapshot, worker_id);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

SubscriberIf* MultiShardSubscriber::GetSubscriberForSubscription(
    SubscriptionID sub_id) {
  auto it = subscription_to_shard_.find(sub_id);
  if (it == subscription_to_shard_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot find subscriber for SubscriptionID(%" PRIu64 ")",
             sub_id);
    return nullptr;
  }

  auto it1 = subscribers_.find(it->second);
  if (it1 == subscribers_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot find subscriber for ShardID(%zu)",
             it->second);
    return nullptr;
  }
  return it1->second.get();
}

///////////////////////////////////////////////////////////////////////////////
MultiThreadedSubscriber::MultiThreadedSubscriber(
    const ClientOptions& options, std::shared_ptr<MsgLoop> msg_loop)
: options_(options), msg_loop_(std::move(msg_loop)), next_sub_id_(0) {
  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    statistics_.emplace_back(std::make_shared<SubscriberStats>("subscriber."));
    subscribers_.emplace_back(new MultiShardSubscriber(
        options_, msg_loop_->GetEventLoop(i), statistics_.back()));
    subscriber_queues_.emplace_back(msg_loop_->CreateThreadLocalQueues(i));
  }
}

void MultiThreadedSubscriber::Stop() {
  // If the subscriber's loop has not been started, we can perform cleanup the
  // calling thread.
  if (msg_loop_->IsRunning()) {
    // Ensure subscribers are destroyed in the event_loop thread
    int nworkers = msg_loop_->GetNumWorkers();
    RS_ASSERT(nworkers == static_cast<int>(subscribers_.size()));

    port::Semaphore destroy_sem;
    std::atomic<int> count(nworkers);
    for (int i = 0; i < nworkers; ++i) {
      std::unique_ptr<Command> cmd(
          MakeExecuteCommand([this, &destroy_sem, &count, i]() {
            // We replace the underlying subscriber with a one that'll ignore
            // all calls.
            class NullSubscriber : public SubscriberIf {
              void StartSubscription(
                  SubscriptionID sub_id,
                  SubscriptionParameters parameters,
                  std::unique_ptr<Observer> observer) override{};

              void Acknowledge(SubscriptionID sub_id,
                               SequenceNumber seqno) override{};

              void TerminateSubscription(SubscriptionID sub_id) override{};

              bool Empty() const override { return true; };

              Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                               size_t worker_id) override {
                return Status::InternalError("Stopped");
              };
            };
            subscribers_[i].reset(new NullSubscriber());
            if (--count == 0) {
              destroy_sem.Post();
            }
          }));
      msg_loop_->SendControlCommand(std::move(cmd), i);
    }
    destroy_sem.Wait();  // Wait until subscribers are destroyed
  }
}

MultiThreadedSubscriber::~MultiThreadedSubscriber() {
  RS_ASSERT(!msg_loop_->IsRunning());
}

SubscriptionHandle MultiThreadedSubscriber::Subscribe(
    Flow* flow,
    SubscriptionParameters parameters,
    std::unique_ptr<Observer> observer) {
  // Choose worker for this subscription and find appropriate queue.
  const auto worker_id = options_.thread_selector(msg_loop_->GetNumWorkers(),
                                                  parameters.namespace_id,
                                                  parameters.topic_name);
  RS_ASSERT(worker_id < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id]->GetThreadLocal();

  // Create new subscription handle that encodes destination worker.
  const auto sub_handle = CreateNewHandle(worker_id);
  if (!sub_handle) {
    LOG_ERROR(options_.info_log, "Client run out of subscription handles");
    RS_ASSERT(false);
    return SubscriptionHandle(0);
  }
  const SubscriptionID sub_id = sub_handle;

  // Send command to responsible worker.
  auto moved_args = folly::makeMoveWrapper(
      std::make_tuple(std::move(parameters), std::move(observer)));
  std::unique_ptr<Command> command(
      MakeExecuteCommand([this, sub_id, moved_args, worker_id]() mutable {
        subscribers_[worker_id]->StartSubscription(
            sub_id,
            std::move(std::get<0>(*moved_args)),
            std::move(std::get<1>(*moved_args)));
      }));
  if (flow) {
    flow->Write(worker_queue, command);
  } else {
    if (!worker_queue->TryWrite(command)) {
      return SubscriptionHandle(0);
    }
  }
  return sub_handle;
}

bool MultiThreadedSubscriber::Unsubscribe(Flow* flow,
                                          SubscriptionHandle sub_handle) {
  if (!sub_handle) {
    LOG_ERROR(options_.info_log,
              "Cannot unsubscribe unengaged subscription handle.");
    return true;
  }

  // Determine corresponding worker, its queue, and subscription ID.
  const auto worker_id = GetWorkerID(sub_handle);
  if (worker_id < 0) {
    LOG_ERROR(options_.info_log, "Invalid worker encoded in the handle");
    return true;
  }
  auto* worker_queue = subscriber_queues_[worker_id]->GetThreadLocal();
  const SubscriptionID sub_id = sub_handle;

  // Send command to responsible worker.
  std::unique_ptr<Command> command(
      MakeExecuteCommand([this, sub_id, worker_id]() mutable {
        subscribers_[worker_id]->TerminateSubscription(sub_id);
      }));

  if (flow) {
    flow->Write(worker_queue, command);
  } else {
    if (!worker_queue->TryWrite(command)) {
      return false;
    }
  }
  return true;
}

bool MultiThreadedSubscriber::Acknowledge(Flow* flow,
                                          const MessageReceived& message) {
  const SubscriptionHandle sub_handle = message.GetSubscriptionHandle();
  if (!sub_handle) {
    LOG_ERROR(options_.info_log,
              "Cannot unsubscribe unengaged subscription handle.");
    return true;
  }

  // Determine corresponding worker, its queue, and subscription ID.
  const auto worker_id = GetWorkerID(sub_handle);
  if (worker_id < 0) {
    LOG_ERROR(options_.info_log, "Invalid worker encoded in the handle");
    return true;
  }
  auto* worker_queue = subscriber_queues_[worker_id]->GetThreadLocal();
  const SubscriptionID sub_id = sub_handle;

  // Send command to responsible worker.
  const auto seqno = message.GetSequenceNumber();
  std::unique_ptr<Command> command(
      MakeExecuteCommand([this, sub_id, worker_id, seqno]() mutable {
        subscribers_[worker_id]->Acknowledge(sub_id, seqno);
      }));

  if (flow) {
    flow->Write(worker_queue, command);
  } else {
    if (!worker_queue->TryWrite(command)) {
      return false;
    }
  }
  return true;
}

void MultiThreadedSubscriber::SaveSubscriptions(
    SaveSubscriptionsCallback save_callback) {
  if (!options_.storage) {
    save_callback(Status::NotInitialized());
    return;
  }

  std::shared_ptr<SubscriptionStorage::Snapshot> snapshot;
  Status st =
      options_.storage->CreateSnapshot(msg_loop_->GetNumWorkers(), &snapshot);
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
             "Failed to create snapshot to save subscriptions: %s",
             st.ToString().c_str());
    save_callback(std::move(st));
    return;
  }

  // For each worker we attempt to append entries for all subscriptions.
  auto map = [this, snapshot](int worker_id) {
    const auto& subscriber = subscribers_[worker_id];
    return subscriber->SaveState(snapshot.get(), worker_id);
  };

  // Once all workers are done, we commit the snapshot and call the callback if
  // necessary.
  auto reduce = [save_callback, snapshot](std::vector<Status> statuses) {
    for (auto& status : statuses) {
      if (!status.ok()) {
        save_callback(std::move(status));
        return;
      }
    }
    Status status = snapshot->Commit();
    save_callback(std::move(status));
  };

  // Fan out commands to all workers.
  st = msg_loop_->Gather(std::move(map), std::move(reduce));
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
             "Failed to send snapshot command to all workers: %s",
             st.ToString().c_str());
    save_callback(std::move(st));
    return;
  }
}

Statistics MultiThreadedSubscriber::GetStatisticsSync() {
  return msg_loop_->AggregateStatsSync(
      [this](int i) -> Statistics { return statistics_[i]->all; });
}

SubscriptionHandle MultiThreadedSubscriber::CreateNewHandle(size_t worker_id) {
  const auto num_workers = msg_loop_->GetNumWorkers();
  const auto handle = 1 + worker_id + num_workers * next_sub_id_++;
  if (static_cast<size_t>(GetWorkerID(handle)) != worker_id) {
    return SubscriptionHandle(0);
  }
  return handle;
}

ssize_t MultiThreadedSubscriber::GetWorkerID(
    SubscriptionHandle sub_handle) const {
  const auto num_workers = msg_loop_->GetNumWorkers();
  const auto worker_id = static_cast<int>((sub_handle - 1) % num_workers);
  if (worker_id < 0 || worker_id >= num_workers) {
    return -1;
  }
  return worker_id;
}

}  // namespace rocketspeed
