/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <unordered_map>

#include "include/BaseEnv.h"
#include "include/HostId.h"
#include "include/RocketSpeed.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/resilient_receiver.h"
#include "src/client/subscriber_hooks_container.h"
#include "src/client/subscriber_if.h"
#include "src/client/subscriptions_map.h"
#include "src/messages/messages.h"
#include "src/messages/types.h"
#include "src/util/common/statistics.h"
#include "src/util/common/subscription_id.h"

namespace rocketspeed {

class Status;
class SubscriberStats;

class SubscriptionStatusImpl : public SubscriptionStatus {
 public:
  Status status_;
  SubscriptionID sub_id_;
  TenantID tenant_id_;
  NamespaceID namespace_id_;
  Topic topic_name_;
  SequenceNumber seqno_;

  explicit SubscriptionStatusImpl(SubscriptionID sub_id,
                                  TenantID tenant_id,
                                  NamespaceID namespace_id,
                                  Topic topic_name,
                                  SequenceNumber seqno)
  : sub_id_(sub_id)
  , tenant_id_(tenant_id)
  , namespace_id_(std::move(namespace_id))
  , topic_name_(std::move(topic_name))
  , seqno_(seqno) {}

  SubscriptionHandle GetSubscriptionHandle() const override { return sub_id_; }

  TenantID GetTenant() const override { return tenant_id_; }

  const NamespaceID& GetNamespace() const override { return namespace_id_; }

  const Topic& GetTopicName() const override { return topic_name_; }

  SequenceNumber GetSequenceNumber() const override { return seqno_; }

  const Status& GetStatus() const override { return status_; }
};

class StatusForHooks : public HookedSubscriptionStatus {
 public:
  StatusForHooks(StatusForHooks&& ) = delete;
  StatusForHooks& operator=(StatusForHooks&& ) = delete;
  StatusForHooks(const StatusForHooks& ) = delete;
  StatusForHooks& operator=(const StatusForHooks& ) = delete;

  StatusForHooks(const SubscriptionStatusImpl* status, const HostId* server)
    : status_(status), server_(server) {
    RS_ASSERT_DBG(status);
    RS_ASSERT_DBG(server);
  }

  SubscriptionHandle GetSubscriptionHandle() const final {
    return status_->GetSubscriptionHandle();
  }

  TenantID GetTenant() const final {
    return status_->GetTenant();
  }

  const NamespaceID& GetNamespace() const final {
    return status_->GetNamespace();
  }

  const Topic& GetTopicName() const final {
    return status_->GetTopicName();
  }

  SequenceNumber GetSequenceNumber() const final {
    return status_->GetSequenceNumber();
  }

  const Status& GetStatus() const final { return status_->GetStatus(); }

  const std::vector<HostId>& GetCurrentServers() const final {
    // There's negligible probability that the hook
    // is installed so allocate memory only when it's really needed.
    if (servers_.empty() && *server_ != HostId()) {
      servers_.emplace_back(*server_);
    }
    return servers_;
  }
 private:
  const SubscriptionStatusImpl* status_;
  const HostId* server_;
  mutable std::vector<HostId> servers_;
};

/// A subscriber that manages subscription on a single shard.
class Subscriber : public SubscriberIf, public ConnectionAwareReceiver {
 public:
  Subscriber(const ClientOptions& options,
             EventLoop* event_loop,
             std::shared_ptr<SubscriberStats> stats,
             size_t shard_id,
             size_t max_active_subscriptions,
             std::shared_ptr<size_t> num_active_subscriptions);

  void InstallHooks(const HooksParameters& parameters,
                    std::shared_ptr<SubscriberHooks> hooks) override;
  void UnInstallHooks(const HooksParameters& parameters) override;

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         std::unique_ptr<Observer> observer) override;

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno) override;

  void HasMessageSince(HasMessageSinceParams params) override;

  void TerminateSubscription(SubscriptionID sub_id) override;

  bool Empty() const override { return subscriptions_map_.Empty(); }

  Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                   size_t worker_id) override;

  bool Select(
      SubscriptionID sub_id, Info::Flags flags, Info* info) const override;

  void SetUserData(SubscriptionID sub_id, void* user_data) override;

  void RefreshRouting() override;

  void NotifyHealthy(bool isHealthy) override;

  bool CallInSubscriptionThread(SubscriptionParameters,
                                std::function<void()> job) override {
    job();
    return true;
  }

 private:
  ThreadCheck thread_check_;

  /// Options, whose lifetime must be managed by the owning client.
  const ClientOptions& options_;
  /// An event loop object this subscriber runs on.
  EventLoop* const event_loop_;
  /// A shared statistics.
  std::shared_ptr<SubscriberStats> stats_;

  SubscriptionsMap subscriptions_map_;
  ResilientStreamReceiver stream_supervisor_;
  bool currently_healthy_{true};

  std::unordered_map<SubscriptionID, SequenceNumber> last_acks_map_;

  /// Shard for this subscriber.
  const size_t shard_id_;

  /// Returns sequence number of last acknowledged message about
  /// the given subscription id.
  SequenceNumber GetLastAcknowledged(SubscriptionID sub_id) const;

  /// Checkt if destination host has been updated.
  void CheckRouterVersion();

  void ReceiveConnectionStatus(bool isHealthy);

  void ProcessUnsubscribe(SubscriptionID sub_id, Info& info, Status status);

  void ConnectionDropped() final override;
  void ConnectionCreated(
    std::unique_ptr<Sink<std::unique_ptr<Message>>> sink) final override;
  void ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe>) final override;
  void ReceiveDeliver(StreamReceiveArg<MessageDeliver>) final override;

  /// Max active subscriptions across the thread.
  const size_t max_active_subscriptions_;

  /// Number of active subscriptions in this thread
  std::shared_ptr<size_t> num_active_subscriptions_;

  SubscriberHooksContainer hooks_;
};

}  // namespace rocketspeed
