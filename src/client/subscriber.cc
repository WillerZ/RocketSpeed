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

#include "external/folly/move_wrapper.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "include/WakeLock.h"
#include "src/client/smart_wake_lock.h"
#include "src/messages/event_loop.h"
#include "src/port/port.h"
#include "src/util/common/random.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

///////////////////////////////////////////////////////////////////////////////
void SubscriptionState::Terminate(const std::shared_ptr<Logger>& info_log,
                                  SubscriptionID sub_id,
                                  MessageUnsubscribe::Reason reason) {
  thread_check_.Check();

  switch (reason) {
    case MessageUnsubscribe::Reason::kRequested:
      LOG_INFO(info_log,
               "Unsubscribed ID(%" PRIu64 ") on Topic(%s, %s)@%" PRIu64,
               sub_id,
               namespace_id_.c_str(),
               topic_name_.c_str(),
               expected_seqno_);
      AnnounceStatus(false, Status::OK());
      break;
    case MessageUnsubscribe::Reason::kInvalid:
      LOG_INFO(info_log,
               "Kicked subscription ID (%" PRIu64 ") on Topic(%s, %s)@%" PRIu64,
               sub_id,
               namespace_id_.c_str(),
               topic_name_.c_str(),
               expected_seqno_);
      AnnounceStatus(false, Status::InvalidArgument("Invalid subscription"));
      break;
    case MessageUnsubscribe::Reason::kBackOff:
      RS_ASSERT(false);
      break;
      // No default, we will be warned about unhandled code.
  }
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
    const std::shared_ptr<Logger>& info_log,
    std::unique_ptr<MessageDeliver> deliver) {
  thread_check_.Check();

  if (!ProcessMessage(info_log, *deliver)) {
    return;
  }

  switch (deliver->GetMessageType()) {
    case MessageType::mDeliverData:
      // Deliver data message to the application.
      if (deliver_callback_) {
        std::unique_ptr<MessageDeliverData> data(
            static_cast<MessageDeliverData*>(deliver.release()));
        std::unique_ptr<MessageReceived> received(
            new MessageReceivedImpl(std::move(data)));
        deliver_callback_(received);
      }
      break;
    case MessageType::mDeliverGap:
      if (data_loss_callback_) {
        std::unique_ptr<MessageDeliverGap> gap(
            static_cast<MessageDeliverGap*>(deliver.release()));

        if (gap->GetGapType() != GapType::kBenign) {
          std::unique_ptr<DataLossInfo> data_loss_info(
              new DataLossInfoImpl(std::move(gap)));
          data_loss_callback_(data_loss_info);
        }
      }
      break;
    default:
      RS_ASSERT(false);
  }
}

bool SubscriptionState::ProcessMessage(const std::shared_ptr<Logger>& info_log,
                                       const MessageDeliver& deliver) {
  thread_check_.Check();

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
             namespace_id_.c_str(),
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
            namespace_id_.c_str(),
            topic_name_.c_str(),
            expected_seqno_);

  expected_seqno_ = current + 1;
  return true;
}

void SubscriptionState::Acknowledge(SequenceNumber seqno) {
  thread_check_.Check();

  if (last_acked_seqno_ < seqno) {
    last_acked_seqno_ = seqno;
  }
}

class SubscriptionStatusImpl : public SubscriptionStatus {
 public:
  SubscriptionStatusImpl(const SubscriptionState& sub_state,
                         bool subscribed,
                         Status status)
  : sub_state_(sub_state)
  , subscribed_(subscribed)
  , status_(std::move(status)) {}

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

  bool IsSubscribed() const override { return subscribed_; }

  const Status& GetStatus() const override { return status_; }

 private:
  const SubscriptionState& sub_state_;
  bool subscribed_;
  Status status_;
};

void SubscriptionState::AnnounceStatus(bool subscribed, Status status) {
  thread_check_.Check();

  if (subscription_callback_) {
    SubscriptionStatusImpl sub_status(*this, subscribed, std::move(status));
    subscription_callback_(sub_status);
  }
}

////////////////////////////////////////////////////////////////////////////////
Subscriber::Subscriber(const ClientOptions& options, EventLoop* event_loop)
: options_(options)
, event_loop_(event_loop)
, backoff_until_time_(0)
, last_send_time_(0)
, consecutive_goodbyes_count_(0)
, rng_(ThreadLocalPRNG())
, copilot_socket_valid_(false)
, last_config_version_(0) {}

Subscriber::~Subscriber() {}

Status Subscriber::Start() {
  return Status::OK();
}

const Statistics& Subscriber::GetStatistics() {
  stats_.active_subscriptions->Set(subscriptions_.size());
  return stats_.all;
}

void Subscriber::StartSubscription(SubscriptionID sub_id,
                                   SubscriptionParameters parameters,
                                   MessageReceivedCallback deliver_callback,
                                   SubscribeCallback subscription_callback,
                                   DataLossCallback data_loss_callback) {
  SubscriptionState* sub_state;
  {  // Store the subscription state.
    auto emplace_result = subscriptions_.emplace(
        sub_id,
        SubscriptionState(std::move(parameters),
                          std::move(subscription_callback),
                          std::move(deliver_callback),
                          std::move(data_loss_callback)));
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
  pending_subscribes_.emplace(sub_id);
  SendPendingRequests();
}

void Subscriber::Acknowledge(SubscriptionID sub_id,
                             SequenceNumber acked_seqno) {
  // Find corresponding subscription state.
  auto it = subscriptions_.find(sub_id);
  if (it == subscriptions_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot acknowledge missing subscription ID (%" PRIu64 ")",
             sub_id);
    return;
  }

  // Record acknowledgement in the state.
  it->second.Acknowledge(acked_seqno);
}

void Subscriber::TerminateSubscription(SubscriptionID sub_id) {
  // Remove subscription state entry.
  auto it = subscriptions_.find(sub_id);
  if (it == subscriptions_.end()) {
    LOG_WARN(options_.info_log,
             "Cannot remove missing subscription ID(%" PRIu64 ")",
             sub_id);
    stats_.unsubscribes_invalid_handle->Add(1);
    return;
  }
  SubscriptionState sub_state(std::move(it->second));
  subscriptions_.erase(it);

  // Update subscription state, which announces subscription status to the
  // application.
  sub_state.Terminate(
      options_.info_log, sub_id, MessageUnsubscribe::Reason::kRequested);

  // Issue unsubscribe request.
  pending_terminations_.emplace(sub_id, sub_state.GetTenant());
  // Remove pending subscribe request, if any.
  pending_subscribes_.erase(sub_id);
  SendPendingRequests();
}

Status Subscriber::SaveState(SubscriptionStorage::Snapshot* snapshot,
                             size_t worker_id) {
  for (const auto& entry : subscriptions_) {
    const SubscriptionState* sub_state = &entry.second;
    SequenceNumber start_seqno = sub_state->GetLastAcknowledged();
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

bool Subscriber::ExpectsMessage(const std::shared_ptr<Logger>& info_log,
                                StreamID origin) {
  if (!copilot_socket_valid_) {
    LOG_WARN(info_log,
             "Unexpected message on stream: (%llu), no valid stream",
             origin);
    return false;
  }
  if (copilot_socket.GetStreamID() != origin) {
    LOG_WARN(info_log,
             "Unexpected message on stream: (%llu), expected: (%llu)",
             origin,
             copilot_socket.GetStreamID());
    return false;
  }
  return true;
}

void Subscriber::SendPendingRequests() {
  thread_check_.Check();

  // Evict entries from a list of recently sent unsubscribe messages.
  recent_terminations_.ProcessExpired(
      options_.unsubscribe_deduplication_timeout, [](SubscriptionID) {}, -1);

  // Close the connection if the configuration has changed.
  const auto current_config_version = options_.config->GetCopilotVersion();
  if (last_config_version_ != current_config_version) {
    if (copilot_socket_valid_) {
      LOG_INFO(options_.info_log,
               "Configuration version has changed %" PRIu64 " -> %" PRIu64,
               last_config_version_,
               current_config_version);

      // Send goodbye message.
      std::unique_ptr<MessageGoodbye> goodbye(
          new MessageGoodbye(GuestTenant,
                             MessageGoodbye::Code::Graceful,
                             MessageGoodbye::OriginType::Client));
      StreamID stream = copilot_socket.GetStreamID();
      Status st = event_loop_->SendResponse(*goodbye, stream);
      if (st.ok()) {
        // Update internal state as if we received goodbye message.
        Receive(std::move(goodbye), stream);
      } else {
        LOG_WARN(options_.info_log,
                 "Failed to close stream (%llu) on configuration change",
                 copilot_socket.GetStreamID());
        // No harm done, we will have a chance to try again, but need to exit
        // now, because we shouldn't be sending more subscriptions according to
        // the old configuration.
        // Note that we haven't updated last configuration version.
        return;
      }
    }
  }

  // Return immediately if there is nothing to do.
  if (pending_subscribes_.empty() && pending_terminations_.empty()) {
    return;
  }

  const uint64_t now = options_.env->NowMicros();
  // Check if we have a valid socket to the Copilot, recreate it if not.
  if (!copilot_socket_valid_) {
    // Since we're not connected to the Copilot, all subscriptions were
    // terminated, so we don't need to send corresponding unsubscribes.
    // We might be disconnected and still have some pending unsubscribes, as the
    // application can terminate subscription while the client is reconnecting.
    pending_terminations_.clear();

    // If we only had pending unsubscribes, there is no point reconnecting.
    if (pending_subscribes_.empty()) {
      return;
    }

    // We do have requests to sync, check if we're still in backoff mode.
    if (now <= backoff_until_time_) {
      return;
    }

    // Get configuration version of the host that we will fetch later on.
    last_config_version_ = current_config_version;
    // Get the copilot's address.
    HostId copilot;
    Status st = options_.config->GetCopilot(&copilot);
    if (!st.ok()) {
      LOG_WARN(options_.info_log,
               "Failed to obtain Copilot address: %s",
               st.ToString().c_str());
      // We'll try to obtain the address and reconnect on next occasion.
      return;
    }

    // And create socket to it.
    copilot_socket = event_loop_->CreateOutboundStream(copilot);
    copilot_socket_valid_ = true;

    LOG_INFO(options_.info_log,
             "Reconnected to %s on stream %llu",
             copilot.ToString().c_str(),
             copilot_socket.GetStreamID());
  }

  // If we have no subscriptions and some pending requests, close the
  // connection. This way we unsubscribe all of them.
  if (options_.close_connection_with_no_subscription &&
      subscriptions_.empty()) {
    RS_ASSERT(pending_subscribes_.empty());

    // We do not use any specific tenant, there might be many unsubscribe
    // requests from arbitrary tenants.
    MessageGoodbye goodbye(GuestTenant,
                           MessageGoodbye::Code::Graceful,
                           MessageGoodbye::OriginType::Client);

    Status st = event_loop_->SendRequest(goodbye, &copilot_socket);
    if (st.ok()) {
      LOG_INFO(options_.info_log,
               "Closed stream (%llu) with no active subscriptions",
               copilot_socket.GetStreamID());
      last_send_time_ = now;
      // All unsubscribe requests were synced.
      pending_terminations_.clear();
      // Since we've sent goodbye message, we need to recreate socket when
      // sending the next message.
      copilot_socket_valid_ = false;
    } else {
      LOG_WARN(options_.info_log,
               "Failed to close stream (%llu) with no active subscrions",
               copilot_socket.GetStreamID());
    }

    // Nothing to do or we should try next time.
    return;
  }

  // Sync unsubscribe requests.
  while (!pending_terminations_.empty()) {
    auto it = pending_terminations_.begin();
    TenantID tenant_id = it->second;
    SubscriptionID sub_id = it->first;

    if (recent_terminations_.Contains(sub_id)) {
      // Skip the unsubscribe message if we sent it recently.
      pending_terminations_.erase(it);
      continue;
    }

    MessageUnsubscribe unsubscribe(
        tenant_id, sub_id, MessageUnsubscribe::Reason::kRequested);

    Status st = event_loop_->SendRequest(unsubscribe, &copilot_socket);
    if (st.ok()) {
      LOG_INFO(options_.info_log, "Unsubscribed ID (%" PRIu64 ")", sub_id);
      last_send_time_ = now;
      // Message was sent, we may clear pending request.
      pending_terminations_.erase(it);
      // Record the fact, so we don't send duplicates of the message.
      recent_terminations_.Add(sub_id);
    } else {
      LOG_WARN(options_.info_log,
               "Failed to send unsubscribe response for ID(%" PRIu64 ")",
               sub_id);
      // Try next time.
      return;
    }
  }

  // Sync subscribe requests.
  while (!pending_subscribes_.empty()) {
    auto it = pending_subscribes_.begin();
    SubscriptionID sub_id = *it;

    SubscriptionState* sub_state;
    {
      auto it_state = subscriptions_.find(sub_id);
      if (it_state == subscriptions_.end()) {
        // Subscription doesn't exist, no need to send request.
        pending_subscribes_.erase(it);
        continue;
      }
      sub_state = &it_state->second;
    }

    MessageSubscribe subscribe(sub_state->GetTenant(),
                               sub_state->GetNamespace(),
                               sub_state->GetTopicName(),
                               sub_state->GetExpected(),
                               sub_id);

    Status st = event_loop_->SendRequest(subscribe, &copilot_socket);
    if (st.ok()) {
      LOG_INFO(options_.info_log, "Subscribed ID (%" PRIu64 ")", sub_id);
      last_send_time_ = now;
      // Message was sent, we may clear pending request.
      pending_subscribes_.erase(it);
    } else {
      LOG_WARN(options_.info_log,
               "Failed to send subscribe request for ID(%" PRIu64 ")",
               sub_id);
      // Try next time.
      return;
    }
  }
}

void Subscriber::Receive(std::unique_ptr<MessageDeliver> deliver,
                         StreamID origin) {
  // Check that message arrived on correct stream.
  if (!ExpectsMessage(options_.info_log, origin)) {
    return;
  }

  consecutive_goodbyes_count_ = 0;

  // Find the right subscription and deliver the message to it.
  SubscriptionID sub_id = deliver->GetSubID();
  auto it = subscriptions_.find(sub_id);
  if (it != subscriptions_.end()) {
    it->second.ReceiveMessage(options_.info_log, std::move(deliver));
  } else {
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
      pending_terminations_.emplace(sub_id, deliver->GetTenantID());
    }
    // This also evicts entries from recent terminations list, should be called
    // in either branch.
    SendPendingRequests();
  }
}

void Subscriber::Receive(std::unique_ptr<MessageUnsubscribe> unsubscribe,
                         StreamID origin) {
  // Check that message arrived on correct stream.
  if (!ExpectsMessage(options_.info_log, origin)) {
    return;
  }

  consecutive_goodbyes_count_ = 0;

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
    pending_subscribes_.emplace(sub_id);
    SendPendingRequests();

    // Do not terminate subscription in this case.
    return;
  }

  // Terminate subscription, and notify the application.
  it->second.Terminate(options_.info_log, sub_id, unsubscribe->GetReason());
  // Remove the corresponding state entry.
  subscriptions_.erase(it);
}

void Subscriber::Receive(std::unique_ptr<MessageGoodbye> msg, StreamID origin) {
  // A duration type unit-compatible with BaseEnv::NowMicros().
  typedef std::chrono::microseconds EnvClockDuration;

  // Check that message arrived on correct stream.
  if (!ExpectsMessage(options_.info_log, origin)) {
    return;
  }

  // Mark socket as broken.
  copilot_socket_valid_ = false;

  // Clear a list of recently sent unsubscribes, these subscriptions IDs were
  // generated for different socket, so are no longer valid.
  recent_terminations_.Clear();

  // Reissue all subscriptions.
  for (auto& entry : subscriptions_) {
    SubscriptionID sub_id = entry.first;
    // Mark subscription as pending.
    pending_subscribes_.emplace(sub_id);
  }

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

  SendPendingRequests();
}

}  // namespace rocketspeed
