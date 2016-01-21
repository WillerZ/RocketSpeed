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
#include "src/messages/event_loop.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
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
    Flow* flow,
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
        deliver_callback_(flow, received);
      }
      break;
    case MessageType::mDeliverGap:
      if (data_loss_callback_) {
        std::unique_ptr<MessageDeliverGap> gap(
            static_cast<MessageDeliverGap*>(deliver.release()));

        if (gap->GetGapType() != GapType::kBenign) {
          std::unique_ptr<DataLossInfo> data_loss_info(
              new DataLossInfoImpl(std::move(gap)));
          data_loss_callback_(flow, data_loss_info);
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
, router_(std::move(router)) {
}

Subscriber::~Subscriber() {
}

Status Subscriber::Start() {
  return event_loop_->RegisterTimerCallback([this]() { SendPendingRequests(); },
                                            options_.timer_period);
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
    stats_->unsubscribes_invalid_handle->Add(1);
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

void Subscriber::SendPendingRequests() {
  thread_check_.Check();

  // Evict entries from a list of recently sent unsubscribe messages.
  recent_terminations_.ProcessExpired(
      options_.unsubscribe_deduplication_timeout, [](SubscriptionID) {}, -1);

  // Close the connection if the router has changed.
  const auto current_router_version = router_->GetVersion();
  if (last_router_version_ != current_router_version) {
    if (server_stream_) {
      LOG_INFO(options_.info_log,
               "Configuration version has changed %zu -> %zu",
               last_router_version_,
               current_router_version);

      // Mark socket as broken.
      server_stream_.reset();

      // Clear a list of recently sent unsubscribes, these subscriptions IDs
      // were generated for different socket, so are no longer valid.
      recent_terminations_.Clear();

      // Reissue all subscriptions.
      for (auto& entry : subscriptions_) {
        SubscriptionID sub_id = entry.first;
        // Mark subscription as pending.
        pending_subscribes_.emplace(sub_id);
      }
    }
  }

  // Return immediately if there is nothing to do.
  if (pending_subscribes_.empty() && pending_terminations_.empty()) {
    return;
  }

  const uint64_t now = options_.env->NowMicros();
  // Check if we have a valid socket to the Copilot, recreate it if not.
  if (!server_stream_) {
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

    // Get router version of the host that we will fetch later on.
    last_router_version_ = current_router_version;
    // Get the server's address.
    server_host_ = router_->GetHost();
    if (!server_host_) {
      LOG_WARN(options_.info_log, "Failed to obtain Copilot address");
      // We'll try to obtain the address and reconnect on next occasion.
      return;
    }

    // And create socket to it.
    server_stream_ = event_loop_->OpenStream(server_host_);
    server_stream_->SetReceiver(this);

    LOG_INFO(options_.info_log,
             "Reconnected to %s on stream %llu",
             server_host_.ToString().c_str(),
             server_stream_->GetLocalID());
  }

  // If we have no subscriptions and some pending requests, close the
  // connection. This way we unsubscribe all of them.
  if (options_.close_connection_with_no_subscription &&
      subscriptions_.empty()) {
    RS_ASSERT(pending_subscribes_.empty());

    LOG_INFO(options_.info_log,
             "Closing stream (%llu) with no active subscriptions",
             server_stream_->GetLocalID());
    server_stream_.reset();

    // We've just sent a message.
    last_send_time_ = now;
    // All unsubscribe requests were synced.
    pending_terminations_.clear();
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

    // Message will be sent, we may clear pending request.
    pending_terminations_.erase(it);
    // Record the fact, so we don't send duplicates of the message.
    recent_terminations_.Add(sub_id);
    // Record last send time for back-off logic.
    last_send_time_ = now;

    MessageUnsubscribe unsubscribe(
        tenant_id, sub_id, MessageUnsubscribe::Reason::kRequested);
    const bool can_write_more = server_stream_->Write(unsubscribe, true);

    LOG_INFO(options_.info_log, "Unsubscribed ID (%" PRIu64 ")", sub_id);

    if (!can_write_more) {
      LOG_WARN(options_.info_log, "Overflow while syncing subscriptions");
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
    const bool can_write_more = server_stream_->Write(subscribe, true);

    // Message was sent, we may clear pending request.
    pending_subscribes_.erase(it);
    // Remember last sent time for backoff.
    last_send_time_ = now;

    LOG_INFO(options_.info_log, "Subscribed ID (%" PRIu64 ")", sub_id);

    if (!can_write_more) {
      LOG_WARN(options_.info_log, "Backoff when syncing subscriptions.");
      // Try next time.
      return;
    }
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

void Subscriber::ReceiveGoodbye(StreamReceiveArg<MessageGoodbye> arg) {
  thread_check_.Check();

  const auto origin = arg.stream_id;

  // A duration type unit-compatible with BaseEnv::NowMicros().
  typedef std::chrono::microseconds EnvClockDuration;

  // Mark socket as broken.
  server_stream_.reset();

  // Notify the router.
  router_->MarkHostDown(server_host_);

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

///////////////////////////////////////////////////////////////////////////////
MultiShardSubscriber::MultiShardSubscriber(const ClientOptions& options,
                                           EventLoop* event_loop)
: options_(options), event_loop_(event_loop) {
}

MultiShardSubscriber::~MultiShardSubscriber() {
}

Status MultiShardSubscriber::Start() {
  return event_loop_->RegisterTimerCallback([this]() { SendPendingRequests(); },
                                            options_.timer_period);
}

const Statistics& MultiShardSubscriber::GetStatistics() {
  return stats_->all;
}

void MultiShardSubscriber::StartSubscription(
    SubscriptionID sub_id,
    SubscriptionParameters parameters,
    MessageReceivedCallback deliver_callback,
    SubscribeCallback subscription_callback,
    DataLossCallback data_loss_callback) {
  // Determine the shard ID.
  size_t shard_id = options_.sharding->GetShard(parameters.namespace_id,
                                                parameters.topic_name);
  subscription_to_shard_.emplace(sub_id, shard_id);

  // Find or create a subscriber for this shard.
  auto it = subscribers_.find(shard_id);
  if (it == subscribers_.end()) {
    // Subscriber is missing, create and start one.
    std::unique_ptr<Subscriber> subscriber(new Subscriber(
        options_, event_loop_, stats_, options_.sharding->GetRouter(shard_id)));
    // We do not start the subscriber as it's not operating on it's own, but as
    // a part of the multi-shard one and will reuse timer events of the latter.

    // Put it back in the map so it can be resued.
    auto result = subscribers_.emplace(shard_id, std::move(subscriber));
    RS_ASSERT(result.second);
    it = result.first;
  }

  it->second->StartSubscription(sub_id,
                                std::move(parameters),
                                std::move(deliver_callback),
                                std::move(subscription_callback),
                                std::move(data_loss_callback));
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

Subscriber* MultiShardSubscriber::GetSubscriberForSubscription(
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

void MultiShardSubscriber::SendPendingRequests() {
  for (const auto& subscriber : subscribers_) {
    subscriber.second->SendPendingRequests();
  }
}

///////////////////////////////////////////////////////////////////////////////
MultiThreadedSubscriber::MultiThreadedSubscriber(
    const ClientOptions& options, std::shared_ptr<MsgLoop> msg_loop)
: options_(options), msg_loop_(std::move(msg_loop)), next_sub_id_(0) {
  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    subscribers_.emplace_back(
        new MultiShardSubscriber(options_, msg_loop_->GetEventLoop(i)));
    subscriber_queues_.emplace_back(msg_loop_->CreateThreadLocalQueues(i));
  }
}

Status MultiThreadedSubscriber::Start() {
  for (const auto& subscriber : subscribers_) {
    RS_ASSERT(subscriber);
    auto st = subscriber->Start();
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

SubscriptionHandle MultiThreadedSubscriber::Subscribe(
    Flow* flow,
    SubscriptionParameters parameters,
    MessageReceivedCallback deliver_callback,
    SubscribeCallback subscription_callback,
    DataLossCallback data_loss_callback) {
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
  auto moved_args =
      folly::makeMoveWrapper(std::make_tuple(std::move(parameters),
                                             std::move(deliver_callback),
                                             std::move(subscription_callback),
                                             std::move(data_loss_callback)));
  std::unique_ptr<Command> command(
      MakeExecuteCommand([this, sub_id, moved_args, worker_id]() mutable {
        subscribers_[worker_id]->StartSubscription(
            sub_id,
            std::move(std::get<0>(*moved_args)),
            std::move(std::get<1>(*moved_args)),
            std::move(std::get<2>(*moved_args)),
            std::move(std::get<3>(*moved_args)));
      }));
  if (flow) {
    flow->Write(worker_queue, command);
  } else {
    if (!worker_queue->TryWrite(command, true)) {
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
    if (!worker_queue->TryWrite(command, true)) {
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
    if (!worker_queue->TryWrite(command, true)) {
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
      [this](int i) -> Statistics { return subscribers_[i]->GetStatistics(); });
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
