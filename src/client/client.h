// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "include/ApiHooks.h"
#include "include/BaseEnv.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/client/publisher.h"
#include "src/client/smart_wake_lock.h"
#include "src/messages/messages.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

class ClientEnv;
class Flow;
class Logger;
class MessageReceived;
class MsgLoop;
class MultiThreadedSubscriber;
class StatisticsExporter;
class WakeLock;


/** Implementation of the client interface. */
class ClientImpl : public Client {
 public:
  static Status Create(ClientOptions client_options,
                       std::unique_ptr<ClientImpl>* client,
                       bool is_internal = false);

  ClientImpl(ClientOptions options,
             std::unique_ptr<MsgLoop> msg_loop,
             bool is_internal);

  virtual ~ClientImpl();

  virtual PublishStatus Publish(const TenantID tenant_id,
                                const Topic& name,
                                const NamespaceID& namespaceId,
                                const TopicOptions& options,
                                const Slice& data,
                                PublishCallback callback,
                                const MsgId messageId) override;

  virtual void InstallHooks(const HooksParameters& params,
                            std::shared_ptr<ClientHooks> hooks) override;
  virtual void UnInstallHooks(const HooksParameters& params) override;

  SubscriptionHandle Subscribe(SubscriptionParameters parameters,
                               std::unique_ptr<Observer>& observer) override;

  // For the rvalue observer overload.
  using Client::Subscribe;

  SubscriptionHandle Subscribe(
      SubscriptionParameters parameters,
      DeliverCallback deliver_callback,
      SubscribeCallback subscription_callback,
      DataLossCallback data_loss_callback)
      override;

  // DEPRECATED
  SubscriptionHandle Subscribe(
      TenantID tenant_id,
      NamespaceID namespace_id,
      Topic topic_name,
      SequenceNumber start_seqno,
      DeliverCallback deliver_callback,
      SubscribeCallback subscription_callback = nullptr,
      DataLossCallback data_loss_callback = nullptr) override {
    return Subscribe({tenant_id,
                      std::move(namespace_id),
                      std::move(topic_name),
                      {{"", start_seqno}}},
                     std::move(deliver_callback),
                     std::move(subscription_callback),
                     std::move(data_loss_callback));
  }

  Status Unsubscribe(SubscriptionHandle sub_handle) override;

  Status Unsubscribe(NamespaceID namespace_id,
                     Topic topic,
                     SubscriptionHandle sub_handle) override;

  Status Acknowledge(const MessageReceived& message) override;

  Status HasMessageSince(
      SubscriptionHandle sub_handle,
      NamespaceID namespace_id,
      Topic topic,
      DataSource source,
      SequenceNumber seqno,
      std::function<void(HasMessageSinceResult, std::string)> callback)
      override;

  void SaveSubscriptions(SaveSubscriptionsCallback save_callback) override;

  Status RestoreSubscriptions(
      std::vector<SubscriptionParameters>* subscriptions) override;

  void ExportStatistics(StatisticsVisitor* visitor) const override;
  bool CallInSubscriptionThread(SubscriptionParameters params,
                                std::function<void()> job) override;

  Statistics GetStatisticsSync() const;

  /**
   * Stop the event loop processing, and wait for thread join.
   * Client callbacks will not be invoked after this point.
   * Stop() is idempotent.
   */
  void Stop();

 private:
  /** Options provided when creating the Client. */
  ClientOptions options_;

  /** A wake lock used on mobile devices. */
  SmartWakeLock wake_lock_;

  const std::shared_ptr<MsgLoop> msg_loop_;
  BaseEnv::ThreadId msg_loop_thread_;
  bool msg_loop_thread_spawned_;

  // If this is an internal client, then we will skip TenantId
  // checks and namespaceid checks.
  const bool is_internal_;

  /** The publisher object, which handles write path in the client. */
  PublisherImpl publisher_;

  /** The underlying subscriber, which handles read path in the client. */
  std::unique_ptr<MultiThreadedSubscriber> subscriber_;

  /** Statistics exporter. May be null if no visitor was provided in options. */
  std::unique_ptr<StatisticsExporter> stats_exporter_;

  /** Starts the client. */
  Status Start();
};

}  // namespace rocketspeed
