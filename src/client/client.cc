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
#include <tuple>
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
#include "src/client/subscriber.h"
#include "src/messages/msg_loop_base.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "src/util/common/random.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

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
    worker_data_.emplace_back(new Subscriber(options_, msg_loop_.get()));
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
  auto moved_args =
      folly::makeMoveWrapper(std::make_tuple(std::move(parameters),
                                             std::move(deliver_callback),
                                             std::move(subscription_callback),
                                             std::move(data_loss_callback)));

  // Send command to responsible worker.
  Status st = msg_loop_->SendCommand(
      std::unique_ptr<Command>(
          MakeExecuteCommand([this, sub_id, moved_args, worker_id]() mutable {
            worker_data_[worker_id]->StartSubscription(
                sub_id,
                std::move(std::get<0>(*moved_args)),
                std::move(std::get<1>(*moved_args)),
                std::move(std::get<2>(*moved_args)),
                std::move(std::get<3>(*moved_args)));
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
          MakeExecuteCommand(std::bind(&Subscriber::TerminateSubscription,
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

  // Send command to responsible worker.
  return msg_loop_->SendCommand(std::unique_ptr<Command>(MakeExecuteCommand(
                                    std::bind(&Subscriber::Acknowledge,
                                              worker_data_[worker_id].get(),
                                              sub_handle,
                                              message.GetSequenceNumber()))),
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
    return worker_data->SaveState(snapshot.get(), worker_id);
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

  auto st = msg_loop_->RegisterTimerCallback([this]() {
    const auto worker_id = msg_loop_->GetThreadWorkerIndex();
    worker_data_[worker_id]->SendPendingRequests();
  }, options_.timer_period);
  if (!st.ok()) {
    return st;
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

}  // namespace rocketspeed
