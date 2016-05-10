/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "single_shard_subscriber.h"

#include <chrono>
#include <cmath>
#include <memory>
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
#include "src/client/subscriber_stats.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
#include "src/util/common/random.h"
#include "src/util/common/rate_limiter_sink.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"

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
        DataLossInfoImpl data_loss_info(std::move(gap));
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
  event_loop_->GetFlowControl()->Register<SubscriptionID>(
      &pending_subscriptions_, [this](Flow* flow, SubscriptionID sub_id) {
        auto it = subscriptions_.find(sub_id);
        SubscriptionState* sub_state =
            it == subscriptions_.end() ? nullptr : &it->second;
        ProcessPendingSubscription(flow, sub_id, sub_state);
      });

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

  CloseServerStream();  // it must be closed already

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
    const size_t actual_rate = std::max<size_t>(
        1,
        options_.subscription_rate_limit *
            duration_cast<milliseconds>(options_.timer_period).count() / 1000);
    limited_server_stream_ =
        folly::make_unique<RateLimiterSink<SharedTimestampedString>>(
            actual_rate, options_.timer_period, server_stream_.get());
    LOG_INFO(options_.info_log,
             "Applied subscription rate limit %zu per second (%zu per %lld ms)",
             options_.subscription_rate_limit,
             actual_rate,
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

void Subscriber::ProcessPendingSubscription(Flow* flow,
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

}  // namespace rocketspeed
