// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "include/ShadowedClient.h"

#include "include/RocketSpeed.h"
#include "src/client/client.h"

namespace rocketspeed {

Status ShadowedClient::Create(ClientOptions client_options,
                              ClientOptions client_proxy_options,
                              std::unique_ptr<Client>* out_client,
                              bool is_internal) {
  RS_ASSERT(out_client);

  std::unique_ptr<ClientImpl> client;
  auto st = ClientImpl::Create(std::move(client_options), &client);

  if (!st.ok()) {
    return st;
  }

  std::unique_ptr<ClientImpl> proxy_client;
  auto st_proxy = ClientImpl::Create(std::move(client_proxy_options),
                                     &proxy_client);

  if (!st_proxy.ok()) {
    return st_proxy;
  }

  std::unique_ptr<Client> result_client(
    new ShadowedClient(std::move(client), std::move(proxy_client)));

  *out_client = std::move(result_client);

  return Status::OK();
}

ShadowedClient::ShadowedClient(
    std::unique_ptr<Client> client,
    std::unique_ptr<Client> proxy_client)
: client_(std::move(client))
, proxy_client_(std::move(proxy_client))   {
}

void ShadowedClient::SetDefaultCallbacks(
    SubscribeCallback subscription_callback,
    std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
    DataLossCallback data_loss_callback) {
}

ShadowedClient::~ShadowedClient() {
}

PublishStatus ShadowedClient::Publish(const TenantID tenant_id,
                                       const Topic& name,
                                       const NamespaceID& namespace_id,
                                       const TopicOptions& options,
                                       const Slice& data,
                                       PublishCallback callback,
                                       const MsgId message_id) {
  return client_->Publish(tenant_id,
                          name,
                          namespace_id,
                          options,
                          data,
                          std::move(callback),
                          message_id);
}

SubscriptionHandle ShadowedClient::Subscribe(SubscriptionParameters parameters,
                                         std::unique_ptr<Observer>& observer) {
  auto subscription = client_->Subscribe(parameters, observer);

  if (subscription == SubscriptionHandle(0)) {
    // A null-handle does not correspond to any subscription.
    return subscription;
  }

  class EmptyObserver : public Observer {
  };

  std::unique_ptr<EmptyObserver> empty_observer(new EmptyObserver());
  auto proxy_subscription = proxy_client_->Subscribe(parameters,
                                                     std::move(empty_observer));

  if (proxy_subscription != SubscriptionHandle(0)) {
    std::lock_guard<std::mutex> lock(subs_mutex_);
    client_to_proxy_subs_[subscription] = proxy_subscription;
  }

  return subscription;
}

SubscriptionHandle ShadowedClient::Subscribe(
    SubscriptionParameters parameters,
    std::function<void(std::unique_ptr<MessageReceived>&)> deliver_callback,
    SubscribeCallback subscription_callback,
    DataLossCallback data_loss_callback) {

  auto subscription = client_->Subscribe(parameters,
                                         deliver_callback,
                                         subscription_callback,
                                         data_loss_callback);

  if (subscription == SubscriptionHandle(0)) {
    // A null-handle does not correspond to any subscription.
    return subscription;
  }

  class EmptyObserver : public Observer {
  };

  std::unique_ptr<EmptyObserver> empty_observer(new EmptyObserver());
  auto proxy_subscription = proxy_client_->Subscribe(parameters,
                                                     std::move(empty_observer));

  if (proxy_subscription != SubscriptionHandle(0)) {
    std::lock_guard<std::mutex> lock(subs_mutex_);
    client_to_proxy_subs_[subscription] = proxy_subscription;
  }

  return subscription;
}

Status ShadowedClient::Unsubscribe(SubscriptionHandle sub_handle) {
  auto st = client_->Unsubscribe(sub_handle);

  if (st.ok()) {
    std::lock_guard<std::mutex> lock(subs_mutex_);
    auto it = client_to_proxy_subs_.find(sub_handle);

    if(it != client_to_proxy_subs_.end()) {
      proxy_client_->Unsubscribe(it->second);
      client_to_proxy_subs_.erase(it);
    }
  }

  return st;
}

Status ShadowedClient::Acknowledge(const MessageReceived& message) {
  return client_->Acknowledge(message);
}

void ShadowedClient::SaveSubscriptions(
  SaveSubscriptionsCallback save_callback) {
  client_->SaveSubscriptions(std::move(save_callback));
}

Status ShadowedClient::RestoreSubscriptions(
    std::vector<SubscriptionParameters>* subscriptions) {
  return client_->RestoreSubscriptions(subscriptions);
}

void ShadowedClient::ExportStatistics(StatisticsVisitor* visitor) const {
  client_->ExportStatistics(visitor);
}

}  // namespace rocketspeed
