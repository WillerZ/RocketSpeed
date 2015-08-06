// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/client/client.h"

#include <cassert>
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
#include "src/messages/msg_loop_base.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
/** Represents a state of a single subscription. */
class SubscriptionState {
 public:
  // Noncopyable
  SubscriptionState(const SubscriptionState&) = delete;
  SubscriptionState& operator=(const SubscriptionState&) = delete;
  // Movable
  SubscriptionState(SubscriptionState&&) = default;
  SubscriptionState& operator=(SubscriptionState&&) = default;

  SubscriptionState(SubscriptionParameters parameters,
                    SubscribeCallback subscription_callback,
                    MessageReceivedCallback deliver_callback,
                    DataLossCallback data_loss_callback)
      : tenant_id_(parameters.tenant_id)
      , namespace_id_(std::move(parameters.namespace_id))
      , topic_name_(std::move(parameters.topic_name))
      , subscription_callback_(std::move(subscription_callback))
      , deliver_callback_(std::move(deliver_callback))
      , data_loss_callback_(std::move(data_loss_callback))
      , expected_seqno_(parameters.start_seqno)
      // If we were to restore state from subscription storage before the
      // subscription advances, we would restore from the next sequence number,
      // that is why we persist the previous one.
      , last_acked_seqno_(
            parameters.start_seqno == 0 ? 0 : parameters.start_seqno - 1) {}

  TenantID GetTenant() const { return tenant_id_; }

  const NamespaceID& GetNamespace() const { return namespace_id_; }

  const Topic& GetTopicName() const { return topic_name_; }

  /** Terminates subscription and notifies the application. */
  void Terminate(const std::shared_ptr<Logger>& info_log,
                 SubscriptionID sub_id,
                 MessageUnsubscribe::Reason reason);

  /** Processes gap or data message. */
  void ReceiveMessage(const std::shared_ptr<Logger>& info_log,
                      std::unique_ptr<MessageDeliver> deliver);

  /** Returns a lower bound on the seqno of the next expected message. */
  SequenceNumber GetExpected() const {
    thread_check_.Check();
    return expected_seqno_;
  }

  /** Marks provided sequence number as acknowledged. */
  void Acknowledge(SequenceNumber seqno);

  /** Returns sequnce number of last acknowledged message. */
  SequenceNumber GetLastAcknowledged() const {
    thread_check_.Check();
    return last_acked_seqno_;
  }

 private:
  ThreadCheck thread_check_;

  const TenantID tenant_id_;
  const NamespaceID namespace_id_;
  const Topic topic_name_;
  const SubscribeCallback subscription_callback_;
  const MessageReceivedCallback deliver_callback_;
  const DataLossCallback data_loss_callback_;

  /** Next expected sequence number on this subscription. */
  SequenceNumber expected_seqno_;
  /** Seqence number of the last acknowledged message. */
  SequenceNumber last_acked_seqno_;

  /** Returns true iff message arrived in order and not duplicated. */
  bool ProcessMessage(const std::shared_ptr<Logger>& info_log,
                      const MessageDeliver& deliver);

  /** Announces status of a subscription via user defined callback. */
  void AnnounceStatus(bool subscribed, Status status);
};

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
      assert(false);
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
        assert(false);
        return DataLossType::kRetention;
        // No default, we will be warned about unhandled code.
    }
    assert(false);
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
      assert(false);
  }
}

bool SubscriptionState::ProcessMessage(const std::shared_ptr<Logger>& info_log,
                                       const MessageDeliver& deliver) {
  thread_check_.Check();

  const auto current = deliver.GetSequenceNumber(),
             previous = deliver.GetPrevSequenceNumber();
  assert(current >= previous);

  if (expected_seqno_ > current ||
      expected_seqno_ < previous ||
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
/** A duration type unit-compatible with BaseEnv::NowMicros(). */
typedef std::chrono::microseconds EnvClockDuration;

/** State of a single subscriber worker, aligned to avoid false sharing. */
class alignas(CACHE_LINE_SIZE) ClientWorkerData {
 public:
  // Noncopyable
  ClientWorkerData(const ClientWorkerData&) = delete;
  ClientWorkerData& operator=(const ClientWorkerData&) = delete;
  // Nonmovable
  ClientWorkerData(ClientWorkerData&&) = delete;
  ClientWorkerData& operator=(ClientWorkerData&&) = delete;

  ClientWorkerData(const ClientOptions& options, MsgLoopBase* msg_loop)
      : options_(options)
      , msg_loop_(msg_loop)
      , backoff_until_time_(0)
      , last_send_time_(0)
      , consecutive_goodbyes_count_(0)
      , copilot_socket_valid_(false)
      , last_config_version_(0) {
    std::random_device r;
    rng_.seed(r());
  }

  Status Start() {
    return msg_loop_->RegisterTimerCallback(
        std::bind(&ClientWorkerData::SendPendingRequests, this),
        options_.timer_period);
  }

  const Statistics& GetStatistics() {
    stats_.active_subscriptions->Set(subscriptions_.size());
    return stats_.all;
  }

  /** Handles creation of a subscription on provided worker thread. */
  void StartSubscription(SubscriptionID sub_id, SubscriptionState sub_state);

  /** Handles termination of a subscription on provided worker thread. */
  void TerminateSubscription(SubscriptionID sub_id);

  /**
   * Synchronises a portion of pending subscribe and unsubscribe requests with
   * the Copilot. Takes into an account rate limits.
   */
  void SendPendingRequests();

  /** Handler for data and gap messages */
  void Receive(std::unique_ptr<MessageDeliver> msg, StreamID origin);

  /** Handler for unsubscribe messages. */
  void Receive(std::unique_ptr<MessageUnsubscribe> msg, StreamID origin);

  /** Handler for goodbye messages. */
  void Receive(std::unique_ptr<MessageGoodbye> msg, StreamID origin);

  /** Options, whose lifetime must be managed by the owning client. */
  const ClientOptions& options_;
  /** A message loop object owned by the client. */
  MsgLoopBase* const msg_loop_;

  /** Time point (in us) until which client should not attemt to reconnect. */
  uint64_t backoff_until_time_;
  /** Time point (in us) of last message sending event. */
  uint64_t last_send_time_;
  /** Number of consecutive goodbye messages. */
  size_t consecutive_goodbyes_count_;
  /** Random engine used by this client. */
  ClientRNG rng_;
  /** Stream socket used by this worker to talk to the copilot. */
  StreamSocket copilot_socket;
  /** Determines whether copilot socket is valid. */
  bool copilot_socket_valid_;
  /** Version of configuration when we last fetched hosts. */
  uint64_t last_config_version_;
  /** All subscriptions served by this worker. */
  std::unordered_map<SubscriptionID, SubscriptionState> subscriptions_;
  /**
   * A timeout list with recently sent unsubscribe requests, used to dedup
   * unsubscribes if we receive a burst of messages on terminated subscription.
   */
  TimeoutList<SubscriptionID> recent_terminations_;
  /** A set of subscriptions pending subscribe message being sent out. */
  std::unordered_set<SubscriptionID> pending_subscribes_;
  /** A set of subscriptions pending unsubscribe message being sent out. */
  std::unordered_map<SubscriptionID, TenantID> pending_terminations_;

  struct Stats {
    Stats() {
      const std::string prefix = "client.";

      active_subscriptions = all.AddCounter(prefix + "active_subscriptions");
      unsubscribes_invalid_handle =
          all.AddCounter(prefix + "unsubscribes_invalid_handle");
    }

    Counter* active_subscriptions;
    Counter* unsubscribes_invalid_handle;
    Statistics all;
  } stats_;

  bool ExpectsMessage(const std::shared_ptr<Logger>& info_log, StreamID origin);
};

bool ClientWorkerData::ExpectsMessage(const std::shared_ptr<Logger>& info_log,
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

////////////////////////////////////////////////////////////////////////////////
Status Client::Create(ClientOptions options,
                      std::unique_ptr<Client>* out_client) {
  assert(out_client);
  std::unique_ptr<ClientImpl> client_impl;
  auto st = ClientImpl::Create(std::move(options), &client_impl);
  if (st.ok()) {
    *out_client = std::move(client_impl);
  }
  return st;
}

Status ClientImpl::Create(ClientOptions options,
                          std::unique_ptr<ClientImpl>* out_client,
                          bool is_internal) {
  assert(out_client);

  // Validate arguments.
  if (!options.config) {
    return Status::InvalidArgument("Missing configuration.");
  }
  if (options.backoff_base < 1.0) {
    return Status::InvalidArgument("Backoff base must be >= 1.0");
  }
  if (!options.backoff_distribution) {
    return Status::InvalidArgument("Missing backoff distribution.");
  }
  if (!options.info_log) {
    options.info_log = std::make_shared<NullLogger>();
  }

  std::unique_ptr<MsgLoopBase> msg_loop_(
    new MsgLoop(options.env,
                EnvOptions(),
                0,
                options.num_workers,
                options.info_log,
                "client"));

  Status st = msg_loop_->Initialize();
  if (!st.ok()) {
    return st;
  }

  std::unique_ptr<ClientImpl> client(
      new ClientImpl(std::move(options), std::move(msg_loop_), is_internal));

  st = client->Start();
  if (!st.ok()) {
    return st;
  }

  *out_client = std::move(client);
  return Status::OK();
}

ClientImpl::ClientImpl(ClientOptions options,
                       std::unique_ptr<MsgLoopBase> msg_loop,
                       bool is_internal)
    : options_(std::move(options))
    , wake_lock_(std::move(options_.wake_lock))
    , msg_loop_(std::move(msg_loop))
    , msg_loop_thread_spawned_(false)
    , is_internal_(is_internal)
    , publisher_(options_.env,
                 options_.config,
                 options_.info_log,
                 msg_loop_.get(),
                 &wake_lock_)
    , next_sub_id_(0) {
  LOG_VITAL(options_.info_log, "Creating Client");

  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    worker_data_.emplace_back(new ClientWorkerData(options_, msg_loop_.get()));
  }

  // TODO(stupaq) kill it with fire
  auto goodbye_callback =
      [this](std::unique_ptr<Message> msg, StreamID origin) {
        const auto worker_id = msg_loop_->GetThreadWorkerIndex();
        auto& worker_data = worker_data_[worker_id];
        if (worker_data->copilot_socket_valid_ &&
            origin == worker_data->copilot_socket.GetStreamID()) {
          std::unique_ptr<MessageGoodbye> goodbye(
              static_cast<MessageGoodbye*>(msg.release()));
          worker_data->Receive(std::move(goodbye), origin);
        } else {
          publisher_.ProcessGoodbye(std::move(msg), origin);
        }
      };

  msg_loop_->RegisterCallbacks({
      {MessageType::mDeliverGap, CreateCallback<MessageDeliver>()},
      {MessageType::mDeliverData, CreateCallback<MessageDeliver>()},
      {MessageType::mUnsubscribe, CreateCallback<MessageUnsubscribe>()},
      {MessageType::mGoodbye, goodbye_callback},
  });
}

void ClientImpl::SetDefaultCallbacks(SubscribeCallback subscription_callback,
                                     MessageReceivedCallback deliver_callback,
                                     DataLossCallback data_loss_callback) {
  subscription_cb_fallback_ = std::move(subscription_callback);
  deliver_cb_fallback_ = std::move(deliver_callback);
  data_loss_callback_ = std::move(data_loss_callback);
}

ClientImpl::~ClientImpl() {
  // Stop the event loop. May block.
  msg_loop_->Stop();

  if (msg_loop_thread_spawned_) {
    // Wait for thread to join.
    options_.env->WaitForJoin(msg_loop_thread_);
  }
}

PublishStatus ClientImpl::Publish(const TenantID tenant_id,
                                  const Topic& name,
                                  const NamespaceID& namespace_id,
                                  const TopicOptions& options,
                                  const Slice& data,
                                  PublishCallback callback,
                                  const MsgId message_id) {
  if (!is_internal_) {
    if (tenant_id <= 100 && tenant_id != GuestTenant) {
      return PublishStatus(
        Status::InvalidArgument("TenantID must be greater than 100."),
        message_id);
    }

    if (IsReserved(namespace_id)) {
      return PublishStatus(
        Status::InvalidArgument("NamespaceID is reserved for internal usage."),
        message_id);
    }
  }
  return publisher_.Publish(tenant_id,
                            namespace_id,
                            name,
                            options,
                            data,
                            std::move(callback),
                            message_id);
}

SubscriptionHandle ClientImpl::Subscribe(
    SubscriptionParameters parameters,
    MessageReceivedCallback deliver_callback,
    SubscribeCallback subscription_callback,
    DataLossCallback data_loss_callback) {
  // Select callbacks taking fallbacks into an account.
  if (!subscription_callback) {
    subscription_callback = subscription_cb_fallback_;
  }
  if (!deliver_callback) {
    deliver_callback = deliver_cb_fallback_;
  }
  if (!data_loss_callback) {
    data_loss_callback = data_loss_callback_;
  }

  // Choose client worker for this subscription.
  const auto worker_id = msg_loop_->LoadBalancedWorkerId();

  // Allocate unique handle and ID for new subscription.
  const SubscriptionHandle sub_handle = CreateNewHandle(worker_id);
  if (!sub_handle) {
    LOG_ERROR(options_.info_log, "Client run out of subscription handles");
    assert(false);
    return SubscriptionHandle(0);
  }
  const SubscriptionID sub_id = sub_handle;

  // Create an object that manages state of the subscription.
  auto moved_sub_state =
      folly::makeMoveWrapper(SubscriptionState(std::move(parameters),
                                               std::move(subscription_callback),
                                               std::move(deliver_callback),
                                               std::move(data_loss_callback)));

  // Send command to responsible worker.
  Status st = msg_loop_->SendCommand(
      std::unique_ptr<Command>(MakeExecuteCommand(
          [this, sub_id, moved_sub_state, worker_id]() mutable {
            worker_data_[worker_id]->StartSubscription(sub_id,
                                                      moved_sub_state.move());
          })),
      worker_id);
  return st.ok() ? sub_handle : SubscriptionHandle(0);
}

Status ClientImpl::Unsubscribe(SubscriptionHandle sub_handle) {
  if (!sub_handle) {
    return Status::InvalidArgument("Unengaged handle.");
  }

  // Determine corresponding worker and subscription ID.
  const auto worker_id = GetWorkerID(sub_handle);
  if (worker_id < 0) {
    return Status::InvalidArgument("Invalid handle.");
  }
  const SubscriptionID sub_id = sub_handle;

  // Send command to responsible worker.
  return msg_loop_->SendCommand(
      std::unique_ptr<Command>(
          MakeExecuteCommand(std::bind(&ClientWorkerData::TerminateSubscription,
                                       worker_data_[worker_id].get(),
                                       sub_id))),
      worker_id);
}

Status ClientImpl::Acknowledge(const MessageReceived& message) {
  const SubscriptionHandle sub_handle = message.GetSubscriptionHandle();
  if (!sub_handle) {
    return Status::InvalidArgument("Unengaged handle.");
  }

  // Determine corresponding worker and subscription ID.
  const auto worker_id = GetWorkerID(sub_handle);
  if (worker_id < 0) {
    return Status::InvalidArgument("Invalid handle.");
  }
  const SubscriptionID sub_id = sub_handle;

  // Prepare command to be executed.
  SequenceNumber acked_seqno = message.GetSequenceNumber();
  auto action = [this, worker_id, sub_id, acked_seqno]() {
    auto& worker_data = worker_data_[worker_id];

    // Find corresponding subscription state.
    auto it = worker_data->subscriptions_.find(sub_id);
    if (it == worker_data->subscriptions_.end()) {
      LOG_WARN(options_.info_log,
               "Cannot acknowledge missing subscription ID (%" PRIu64 ")",
               sub_id);
      return;
    }

    // Record acknowledgement in the state.
    it->second.Acknowledge(acked_seqno);
  };

  // Send command to responsible worker.
  return msg_loop_->SendCommand(
      std::unique_ptr<Command>(MakeExecuteCommand(std::move(action))),
      worker_id);
}

void ClientImpl::SaveSubscriptions(SaveSubscriptionsCallback save_callback) {
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

  // For each worker we attemp to append entries for all subscriptions.
  auto map = [this, snapshot](int worker_id) {
    const auto& worker_data = worker_data_[worker_id];

    for (const auto& entry : worker_data->subscriptions_) {
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

Status ClientImpl::RestoreSubscriptions(
    std::vector<SubscriptionParameters>* subscriptions) {
  if (!options_.storage) {
    return Status::NotInitialized();
  }

  return options_.storage->RestoreSubscriptions(subscriptions);
}

Statistics ClientImpl::GetStatisticsSync() {
  Statistics aggregated = msg_loop_->GetStatisticsSync();
  aggregated.Aggregate(
      msg_loop_->AggregateStatsSync([this](int i) -> Statistics {
        return worker_data_[i]->GetStatistics();
      }));
  return aggregated;
}

Status ClientImpl::Start() {
  for (const auto& worker : worker_data_) {
    assert(worker);
    auto st = worker->Start();
    if (!st.ok()) {
      return st;
    }
  }

  msg_loop_thread_ =
      options_.env->StartThread([this]() { msg_loop_->Run(); }, "client");
  msg_loop_thread_spawned_ = true;
  return Status::OK();
}

SubscriptionHandle ClientImpl::CreateNewHandle(int worker_id) {
  const auto num_workers = msg_loop_->GetNumWorkers();
  const auto handle = 1 + worker_id + num_workers * next_sub_id_++;
  if (GetWorkerID(handle) != worker_id) {
    return SubscriptionHandle(0);
  }
  return handle;
}

int ClientImpl::GetWorkerID(SubscriptionHandle sub_handle) const {
  const auto num_workers = msg_loop_->GetNumWorkers();
  const auto worker_id = static_cast<int>((sub_handle - 1) % num_workers);
  if (worker_id < 0 || worker_id >= num_workers) {
    return -1;
  }
  return worker_id;
}

template <typename Msg>
std::function<void(std::unique_ptr<Message>, StreamID)>
ClientImpl::CreateCallback() {
  return [this](std::unique_ptr<Message> message, StreamID origin) {
    std::unique_ptr<Msg> casted(static_cast<Msg*>(message.release()));
    auto worker_id = msg_loop_->GetThreadWorkerIndex();
    worker_data_[worker_id]->Receive(std::move(casted), origin);
  };
}

void ClientWorkerData::StartSubscription(SubscriptionID sub_id,
                                         SubscriptionState sub_state_val) {
  SubscriptionState* sub_state;
  {  // Store the subscription state.
    auto emplace_result =
        subscriptions_.emplace(sub_id, std::move(sub_state_val));
    if (!emplace_result.second) {
      LOG_ERROR(
          options_.info_log, "Duplicate subscription ID(%" PRIu64 ")", sub_id);
      assert(false);
      return;
    }
    sub_state = &emplace_result.first->second;
  }

  LOG_INFO(options_.info_log,
           "Subscribed on Topic(%s, %s)@%" PRIu64 " Tenant(%u) ID(%" PRIu64 ")",
           sub_state->GetNamespace().c_str(),
           sub_state->GetTopicName().c_str(),
           sub_state->GetExpected(),
           sub_state->GetTenant(),
           sub_id);

  // Issue subscription.
  pending_subscribes_.emplace(sub_id);
  SendPendingRequests();
}

void ClientWorkerData::TerminateSubscription(SubscriptionID sub_id) {
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

void ClientWorkerData::SendPendingRequests() {
  const auto worker_id = msg_loop_->GetThreadWorkerIndex();

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
      Status st = msg_loop_->SendResponse(*goodbye, stream, worker_id);
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
    copilot_socket = msg_loop_->CreateOutboundStream(copilot, worker_id);
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
    assert(pending_subscribes_.empty());

    // We do not use any specific tenant, there might be many unsubscribe
    // requests from arbitrary tenants.
    MessageGoodbye goodbye(GuestTenant,
                           MessageGoodbye::Code::Graceful,
                           MessageGoodbye::OriginType::Client);

    Status st = msg_loop_->SendRequest(goodbye, &copilot_socket, worker_id);
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

    Status st = msg_loop_->SendRequest(unsubscribe, &copilot_socket, worker_id);
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

    Status st = msg_loop_->SendRequest(subscribe, &copilot_socket, worker_id);
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

void ClientWorkerData::Receive(std::unique_ptr<MessageDeliver> deliver,
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

void ClientWorkerData::Receive(std::unique_ptr<MessageUnsubscribe> unsubscribe,
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

void ClientWorkerData::Receive(std::unique_ptr<MessageGoodbye> msg,
                               StreamID origin) {
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
  assert(consecutive_goodbyes_count_ > 0);
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
