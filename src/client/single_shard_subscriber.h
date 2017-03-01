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
#include "src/util/topic_uuid.h"

namespace rocketspeed {

class BacklogQueryStore;
class Status;
class SubscriberStats;
class ApplicationMessage;
template <typename> class RetryLaterSink;

class SubscriptionStatusImpl : public SubscriptionStatus {
 public:
  Status status_;
  SubscriptionID sub_id_;
  TenantID tenant_id_;
  NamespaceID namespace_id_;
  Topic topic_name_;

  explicit SubscriptionStatusImpl(SubscriptionID sub_id,
                                  TenantID tenant_id,
                                  NamespaceID namespace_id,
                                  Topic topic_name)
  : sub_id_(sub_id)
  , tenant_id_(tenant_id)
  , namespace_id_(std::move(namespace_id))
  , topic_name_(std::move(topic_name)) {}

  SubscriptionHandle GetSubscriptionHandle() const override { return sub_id_; }

  TenantID GetTenant() const override { return tenant_id_; }

  const NamespaceID& GetNamespace() const override { return namespace_id_; }

  const Topic& GetTopicName() const override { return topic_name_; }

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
             std::shared_ptr<size_t> num_active_subscriptions,
             std::shared_ptr<const IntroParameters> intro_parameters);

  ~Subscriber();

  void InstallHooks(const HooksParameters& parameters,
                    std::shared_ptr<SubscriberHooks> hooks) override;
  void UnInstallHooks(const HooksParameters& parameters) override;

  void StartSubscription(SubscriptionID sub_id,
                         SubscriptionParameters parameters,
                         std::unique_ptr<Observer> observer) override;

  void Acknowledge(SubscriptionID sub_id, SequenceNumber seqno) override;

  void HasMessageSince(HasMessageSinceParams params) override;

  void TerminateSubscription(NamespaceID namespace_id,
                             Topic topic,
                             SubscriptionID sub_id) override;

  bool Empty() const override { return subscriptions_map_.Empty(); }

  Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                   size_t worker_id) override;

  bool Select(
      const TopicUUID& uuid, Info::Flags flags, Info* info) const override;

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
  /// A shared statistics.
  std::shared_ptr<SubscriberStats> stats_;

  SubscriptionsMap subscriptions_map_;
  ResilientStreamReceiver stream_supervisor_;
  bool currently_healthy_{true};

  std::unordered_map<SubscriptionID, SequenceNumber> last_acks_map_;

  std::unique_ptr<BacklogQueryStore> backlog_query_store_;

  /// Shard for this subscriber.
  const size_t shard_id_;

  /// Returns sequence number of last acknowledged message about
  /// the given subscription id.
  SequenceNumber GetLastAcknowledged(SubscriptionID sub_id) const;

  /// Checkt if destination host has been updated.
  void CheckRouterVersion();

  void ReceiveConnectionStatus(bool isHealthy);

  void ProcessUnsubscribe(SubscriptionID sub_id, Info& info, Status status);

  void ConnectionChanged() final override;
  void ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe>) final override;
  void ReceiveSubAck(StreamReceiveArg<MessageSubAck>) final override;
  void ReceiveDeliver(StreamReceiveArg<MessageDeliver>) final override;
  void ReceiveBacklogFill(StreamReceiveArg<MessageBacklogFill>) final override;

  BackPressure InvokeApplication(ApplicationMessage& msg);
  void SendMessage(Flow* flow, std::unique_ptr<Message> message);

  /// Max active subscriptions across the thread.
  const size_t max_active_subscriptions_;

  /// Number of active subscriptions in this thread
  std::shared_ptr<size_t> num_active_subscriptions_;

  SubscriberHooksContainer hooks_;

  /// This sink is used for invoking all application callbacks.
  std::unique_ptr<RetryLaterSink<ApplicationMessage>> app_sink_;
};

}  // namespace rocketspeed
