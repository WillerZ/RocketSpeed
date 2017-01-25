// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <unordered_map>
#include <mutex>

#include "RocketSpeed.h"
#include "EnvOptions.h"
#include "Slice.h"
#include "Status.h"
#include "SubscriptionStorage.h"
#include "Types.h"

namespace rocketspeed {

class ClientHooks;

/**
  * ShouldShadow is a function which decides if send a subscription
  * from the shadow client or not
*/
typedef std::function<bool(const SubscriptionParameters&)> ShouldShadow;

/**
 * Implementation of the client interface.
 * In this implementation we can add special second shadowed client
 * and send to him a shadow traffic.
*/
class ShadowedClient : public Client {
 public:
  /**
    * Create a new client with a second shadowed client.
    *
    * @param client_options Options for the normal client.
    * @param shadowed_client Options for the shadowed client.
    * @param is_internal Set if it is only internal client.
    * @param shadow_predicate Predicate which decides if send a subscription
    *   from the shadow client or not
    * @return the status of the created client.
  */
  static Status Create(
      ClientOptions client_options,
      ClientOptions shadowed_client_options,
      std::unique_ptr<Client>* client,
      bool is_internal = false,
      ShouldShadow shadow_predicate =
          [](const SubscriptionParameters& params) { return true; });

  ShadowedClient(std::unique_ptr<Client> client,
                 std::unique_ptr<Client> shadowed_client,
                 ShouldShadow shadow_predicate);

  virtual ~ShadowedClient();

  void SetDefaultCallbacks(
      SubscribeCallback subscription_callback,
      std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
      DataLossCallback data_loss_callback)
      override;

  void InstallHooks(const HooksParameters&,
                    std::shared_ptr<ClientHooks>) override;
  void UnInstallHooks(const HooksParameters&) override;

  virtual PublishStatus Publish(const TenantID tenant_id,
                                const Topic& name,
                                const NamespaceID& namespaceId,
                                const TopicOptions& options,
                                const Slice& data,
                                PublishCallback callback,
                                const MsgId messageId) override;

  SubscriptionHandle Subscribe(SubscriptionParameters parameters,
                               std::unique_ptr<Observer>& observer) override;

  // For the rvalue observer overload.
  using Client::Subscribe;

  SubscriptionHandle Subscribe(
      SubscriptionParameters parameters,
      std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
      SubscribeCallback subscription_callback,
      DataLossCallback data_loss_callback)
      override;

  // DEPRECATED
  SubscriptionHandle Subscribe(
      TenantID tenant_id,
      NamespaceID namespace_id,
      Topic topic_name,
      SequenceNumber start_seqno,
      std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback =
          nullptr,
      SubscribeCallback subscription_callback = nullptr,
      DataLossCallback data_loss_callback =
          nullptr) override {
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
                     SubscriptionHandle sub_handle = 0) override;

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

  /**
   * Stop the event loop processing, and wait for thread join.
   * Client callbacks will not be invoked after this point.
   * Stop() is idempotent.
   */
  void Stop();

 private:
  std::unique_ptr<Client> client_;
  std::unique_ptr<Client> shadowed_client_;

  // A map from subscriptionHandle for client_
  // to subscriptionHandle for shadowed_client_
  std::unordered_map<SubscriptionHandle, SubscriptionHandle>
      client_to_shadowed_subs_;

  // A mutex to protect client_to_shadowed_subs_ map
  std::mutex subs_mutex_;

  // Predicate if send subscription from shadow client
  ShouldShadow shadow_predicate_;
};

}  // namespace rocketspeed
