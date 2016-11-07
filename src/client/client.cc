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

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "include/WakeLock.h"
#include "src/client/multi_threaded_subscriber.h"
#include "src/client/smart_wake_lock.h"
#include "src/client/subscriber_if.h"
#include "src/messages/flow_control.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "src/util/common/fixed_configuration.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/random.h"
#include "src/util/common/statistics_exporter.h"
#include "src/util/timeout_list.h"

namespace {
using namespace rocketspeed;

class SubscriberHooksAdapter : public SubscriberHooks {
 public:
  explicit SubscriberHooksAdapter(std::shared_ptr<ClientHooks> hooks)
      : hooks_(hooks) {}
  virtual void SubscriptionExists(const SubscriptionStatus& status) override {
    hooks_->SubscriptionExists(status);
  }
  virtual void OnStartSubscription() override { hooks_->OnSubscribe(); }
  virtual void OnAcknowledge(SequenceNumber seqno) override {
    hooks_->OnAcknowledge(seqno);
  }
  virtual void OnTerminateSubscription() override { hooks_->OnUnsubscribe(); }
  virtual void OnReceiveTerminate() override { hooks_->OnReceiveTerminate(); }
  virtual void OnMessageReceived(const MessageReceived* msg) override {
    hooks_->OnMessageReceived(msg);
  }
  virtual void OnSubscriptionStatusChange(
      const SubscriptionStatus& status) override {
    hooks_->OnSubscriptionStatusChange(status);
  }
  virtual void OnDataLoss(const DataLossInfo& info) override {
    hooks_->OnDataLoss(info);
  }

 private:
  std::shared_ptr<ClientHooks> hooks_;
};
}

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

namespace {
class NullPublisher : public PublisherRouter {
 public:
  Status GetPilot(HostId* /* hostOut */) const override {
    return Status::NotFound("");
  }
};

class NullSharding : public ShardingStrategy {
 public:
  size_t GetShard(Slice, Slice) const override { return 0; }
  size_t GetVersion() override { return 0; }
  HostId GetHost(size_t) override { return HostId(); }
  void MarkHostDown(const HostId&) override {}
};
}

Status ClientImpl::Create(ClientOptions options,
                          std::unique_ptr<ClientImpl>* out_client,
                          bool is_internal) {
  RS_ASSERT(out_client);

  // Validate arguments.
  if (!options.publisher) {
    options.publisher = std::make_shared<NullPublisher>();
  }
  if (!options.sharding) {
    options.sharding = std::make_shared<NullSharding>();
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
  m_opts.event_loop.heartbeat_timeout = options.heartbeat_timeout;
  std::unique_ptr<MsgLoop> msg_loop(new MsgLoop(options.env,
                                                options.env_options,
                                                -1,  // port
                                                options.num_workers,
                                                options.info_log,
                                                "rocketspeed",
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
, subscriber_(new MultiThreadedSubscriber(options_, msg_loop_)) {
  LOG_VITAL(options_.info_log, "Creating Client");

  if (options_.statistics_visitor) {
    // Start thread for exporting statistics.
    stats_exporter_.reset(new StatisticsExporter(
        options_.env,
        options_.info_log,
        [this] () { return GetStatisticsSync(); },
        options_.statistics_visitor.get()));
  }
}

void ClientImpl::SetDefaultCallbacks(
    SubscribeCallback subscription_callback,
    std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
    DataLossCallback data_loss_callback) {
  subscription_cb_fallback_ = std::move(subscription_callback);
  deliver_cb_fallback_ = std::move(deliver_callback);
  data_loss_callback_ = std::move(data_loss_callback);
}

void ClientImpl::InstallHooks(const HooksParameters& params,
                              std::shared_ptr<ClientHooks> hooks) {
  auto sub_hooks = std::make_shared<SubscriberHooksAdapter>(hooks);
  subscriber_->InstallHooks(params, sub_hooks);
}

void ClientImpl::UnInstallHooks(const HooksParameters& params) {
  subscriber_->UnInstallHooks(params);
}

ClientImpl::~ClientImpl() {
  Stop();
}

void ClientImpl::Stop() {
  // Stop the statistics exporter. May block.
  stats_exporter_.reset();

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
    return std::make_unique<StdFunctionObserver>(
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

  SubscriptionHandle subscription = subscriber_->Subscribe(
      std::move(parameters), observer);

  if (subscription != SubscriptionHandle(0)) {
    RS_ASSERT(!observer);  // should be consumed
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

bool ClientImpl::CallInSubscriptionThread(SubscriptionParameters params,
                                          std::function<void()> job) {
  return subscriber_->CallInSubscriptionThread(std::move(params),
                                               std::move(job));
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
