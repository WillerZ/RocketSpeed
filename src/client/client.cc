// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/client/client.h"

#include <cmath>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "external/folly/Memory.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "include/WakeLock.h"
#include "src/client/multi_threaded_subscriber.h"
#include "src/client/smart_wake_lock.h"
#include "src/messages/flow_control.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "src/util/common/fixed_configuration.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/random.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
Status Client::Create(ClientOptions options,
                      std::unique_ptr<Client>* out_client) {
  RS_ASSERT(out_client);
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
  RS_ASSERT(out_client);

  // Validate arguments.
  if (!options.publisher) {
    return Status::InvalidArgument("Missing publisher configuration.");
  }
  if (!options.sharding) {
    return Status::InvalidArgument("Missing sharding strategy.");
  }
  if (!options.backoff_strategy) {
    return Status::InvalidArgument("Missing backoff strategy.");
  }

  // Default to null logger.
  if (!options.info_log) {
    options.info_log = std::make_shared<NullLogger>();
  }

  MsgLoop::Options m_opts;
  m_opts.event_loop.connection_without_streams_keepalive =
    options.connection_without_streams_keepalive;
  std::unique_ptr<MsgLoop> msg_loop(new MsgLoop(options.env,
                                                EnvOptions(),
                                                -1,  // port
                                                options.num_workers,
                                                options.info_log,
                                                "client",
                                                m_opts));

  Status st = msg_loop->Initialize();
  if (!st.ok()) {
    return st;
  }

  // Assign default thread selector if not specified.
  if (!options.thread_selector) {
    auto raw_loop = msg_loop.get();
    options.thread_selector = [raw_loop](size_t num_threads, Slice, Slice) {
      return raw_loop->LoadBalancedWorkerId();
    };
  }

  std::unique_ptr<ClientImpl> client(
      new ClientImpl(std::move(options), std::move(msg_loop), is_internal));

  st = client->Start();
  if (!st.ok()) {
    return st;
  }

  *out_client = std::move(client);
  return Status::OK();
}

ClientImpl::ClientImpl(ClientOptions options,
                       std::unique_ptr<MsgLoop> msg_loop,
                       bool is_internal)
: options_(std::move(options))
, wake_lock_(std::move(options_.wake_lock))
, msg_loop_(std::move(msg_loop))
, msg_loop_thread_spawned_(false)
, is_internal_(is_internal)
, publisher_(options_, msg_loop_.get(), &wake_lock_)
, subscriber_(new MultiThreadedSubscriber(options_, msg_loop_))
, num_subscriptions_(0) {
  LOG_VITAL(options_.info_log, "Creating Client");
}

void ClientImpl::SetDefaultCallbacks(
    SubscribeCallback subscription_callback,
    std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
    DataLossCallback data_loss_callback) {
  subscription_cb_fallback_ = std::move(subscription_callback);
  deliver_cb_fallback_ = std::move(deliver_callback);
  data_loss_callback_ = std::move(data_loss_callback);
}

ClientImpl::~ClientImpl() {
  Stop();
}

void ClientImpl::Stop() {
  // Stop the subscriber. May block.
  subscriber_->Stop();

  // Stop the event loop. May block.
  msg_loop_->Stop();

  if (msg_loop_thread_spawned_) {
    // Wait for thread to join.
    options_.env->WaitForJoin(msg_loop_thread_);
    msg_loop_thread_spawned_ = false;
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
      return PublishStatus(Status::InvalidArgument(
                               "NamespaceID is reserved for internal usage."),
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

namespace {

class StdFunctionObserver : public Observer,
                            public NonCopyable,
                            public NonMovable {
 public:
  static std::unique_ptr<StdFunctionObserver> Create(
      std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
      SubscribeCallback subscription_callback,
      DataLossCallback data_loss_callback) {
    return folly::make_unique<StdFunctionObserver>(
        std::move(deliver_callback),
        std::move(subscription_callback),
        std::move(data_loss_callback));
  }

  StdFunctionObserver(
      std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
      SubscribeCallback subscription_callback,
      DataLossCallback data_loss_callback)
  : deliver_callback_(std::move(deliver_callback))
  , subscription_callback_(std::move(subscription_callback))
  , data_loss_callback_(std::move(data_loss_callback)) {}

  void OnMessageReceived(Flow*, std::unique_ptr<MessageReceived>& a) override {
    if (deliver_callback_) {
      deliver_callback_(a);
    }
  }

  void OnSubscriptionStatusChange(const SubscriptionStatus& a) override {
    if (subscription_callback_) {
      subscription_callback_(a);
    }
  }

  void OnDataLoss(Flow*, const DataLossInfo& a) override {
    if (data_loss_callback_) {
      data_loss_callback_(a);
    }
  }

 private:
  const std::function<void(std::unique_ptr<MessageReceived>&)>
      deliver_callback_;
  const SubscribeCallback subscription_callback_;
  const DataLossCallback data_loss_callback_;
};

}  // namespace

SubscriptionHandle ClientImpl::Subscribe(SubscriptionParameters parameters,
                                         std::unique_ptr<Observer>& observer) {
  RS_ASSERT(!!observer);

  if (num_subscriptions_.load() >= options_.max_subscriptions) {
    LOG_ERROR(options_.info_log,
      "Subscription limit of %zu reached.", options_.max_subscriptions);
    return SubscriptionHandle(0);
  }

  SubscriptionHandle subscription = subscriber_->Subscribe(
      std::move(parameters), observer);

  if (subscription != SubscriptionHandle(0)) {
    RS_ASSERT(!observer);  // should be consumed
    auto next = ++num_subscriptions_;
    RS_ASSERT(next != 0);
    (void)next;
  } else {
    RS_ASSERT(!!observer);  // should not be consumed
  }
  return subscription;
}

SubscriptionHandle ClientImpl::Subscribe(
    SubscriptionParameters parameters,
    std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
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

  return Subscribe(std::move(parameters),
                   StdFunctionObserver::Create(std::move(deliver_callback),
                                               std::move(subscription_callback),
                                               std::move(data_loss_callback)));
}

Status ClientImpl::Unsubscribe(SubscriptionHandle sub_handle) {
  subscriber_->Unsubscribe(sub_handle);
  auto prev = num_subscriptions_--;
  RS_ASSERT(prev != 0);
  (void)prev;
  return Status::OK();
}

Status ClientImpl::Acknowledge(const MessageReceived& message) {
  return subscriber_->Acknowledge(message) ? Status::OK() : Status::NoBuffer();
}

void ClientImpl::SaveSubscriptions(SaveSubscriptionsCallback save_callback) {
  subscriber_->SaveSubscriptions(std::move(save_callback));
}

Status ClientImpl::RestoreSubscriptions(
    std::vector<SubscriptionParameters>* subscriptions) {
  if (!options_.storage) {
    return Status::NotInitialized();
  }

  return options_.storage->RestoreSubscriptions(subscriptions);
}

void ClientImpl::ExportStatistics(StatisticsVisitor* visitor) const {
  GetStatisticsSync().Export(visitor);
}

Statistics ClientImpl::GetStatisticsSync() const {
  Statistics aggregated = msg_loop_->GetStatisticsSync();
  aggregated.Aggregate(subscriber_->GetStatisticsSync());
  return aggregated;
}

Status ClientImpl::Start() {
  msg_loop_thread_ =
      options_.env->StartThread([this]() { msg_loop_->Run(); }, "client");
  msg_loop_thread_spawned_ = true;
  return Status::OK();
}

}  // namespace rocketspeed
