//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "client_wrapper.h"

#include <cassert>
#include <memory>
#include <limits>
#include <stdexcept>
#include <string>

#include "include/RocketSpeed.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/djinni/jvm_env.h"
#include "src/djinni/type_conversions.h"
#include "src/util/common/fixed_configuration.h"

#include "src-gen/djinni/cpp/LogLevel.hpp"
#include "src-gen/djinni/cpp/PublishCallback.hpp"
#include "src-gen/djinni/cpp/MessageReceivedCallback.hpp"
#include "src-gen/djinni/cpp/StorageType.hpp"
#include "src-gen/djinni/cpp/SnapshotCallback.hpp"
#include "src-gen/djinni/cpp/SubscribeCallback.hpp"
#include "src-gen/djinni/cpp/SubscriptionStorage.hpp"

namespace rocketspeed {
namespace djinni {

std::shared_ptr<ClientImpl> ClientImpl::Create(LogLevel log_level,
                                               HostId cockpit,
                                               SubscriptionStorage storage) {
  rocketspeed::Status st;

  auto cockpit_port = static_cast<uint16_t>(cockpit.port);
  if (cockpit_port != cockpit.port) {
    throw std::runtime_error("Invalid port.");
  }

  auto jvm_env = JvmEnv::Default();
  auto log_level1 = ToInfoLogLevel(log_level);
  auto info_log = jvm_env->CreatePlatformLogger(log_level1);
  LOG_VITAL(info_log,
            "Created JVM logger for Client, log level: %s",
            rocketspeed::LogLevelToString(log_level1));

  rocketspeed::HostId cockpit1;
  st = rocketspeed::HostId::Resolve(cockpit.host, cockpit_port, &cockpit1);
  if (!st.ok()) {
    throw std::runtime_error("Could not resolve cockpit host");
  }
  auto config = std::make_shared<FixedConfiguration>(cockpit1, cockpit1);

  rocketspeed::ClientOptions options;
  options.config = std::move(config);
  options.env = jvm_env;
  options.info_log = info_log;

  switch (storage.type) {
    case StorageType::NONE:
      break;
    case StorageType::FILE:
      if (storage.file_path.empty()) {
        throw std::runtime_error("Missing file path for file storage.");
      }
      st = rocketspeed::SubscriptionStorage::File(
          options.env, options.info_log, storage.file_path, &options.storage);
      if (!st.ok()) {
        throw std::runtime_error(st.ToString());
      }
      break;
      // No default, we will be warned about missing branch.
  }

  std::unique_ptr<rocketspeed::Client> client;
  st = rocketspeed::Client::Create(std::move(options), &client);
  if (!st.ok()) {
    throw std::runtime_error(st.ToString());
  }
  return std::make_shared<ClientWrapper>(std::move(client),
                                         std::move(info_log));
}

MsgId ClientWrapper::Publish(int32_t tenant_id,
                             std::string namespace_id,
                             std::string topic_name,
                             std::vector<uint8_t> data,
                             std::shared_ptr<PublishCallback> publish_cb,
                             MsgId message_id) {
  auto tenant_id1 = static_cast<rocketspeed::TenantID>(tenant_id);
  if (tenant_id1 != tenant_id) {
    throw std::runtime_error("TenantID out of range.");
  }

  rocketspeed::PublishCallback publish_cb1 = nullptr;
  if (publish_cb) {
    publish_cb1 = [this, publish_cb](std::unique_ptr<ResultStatus> status) {
      try {
        publish_cb->Call(FromMsgId(status->GetMessageId()),
                         status->GetNamespaceId().ToString(),
                         status->GetTopicName().ToString(),
                         FromSequenceNumber(status->GetSequenceNumber()),
                         FromStatus(status->GetStatus()));
      } catch (const std::exception& e) {
        LOG_WARN(info_log_, "PublishCallback caught exception: %s", e.what());
      };
    };
  }

  auto pub_st = client_->Publish(tenant_id1,
                                 topic_name,
                                 namespace_id,
                                 TopicOptions(),
                                 ToSlice(data),
                                 std::move(publish_cb1),
                                 ToMsgId(message_id));
  if (!pub_st.status.ok()) {
    throw std::runtime_error(pub_st.status.ToString());
  }
  return FromMsgId(pub_st.msgid);
}

int64_t ClientWrapper::Subscribe(
    int32_t tenant_id,
    std::string namespace_id,
    std::string topic_name,
    int64_t start_seqno,
    std::shared_ptr<MessageReceivedCallback> deliver_cb,
    std::shared_ptr<SubscribeCallback> subscribe_cb) {
  auto tenant_id1 = static_cast<rocketspeed::TenantID>(tenant_id);
  if (tenant_id != tenant_id1) {
    throw std::runtime_error("TenantID out of range.");
  }

  rocketspeed::MessageReceivedCallback deliver_cb1 = nullptr;
  if (deliver_cb) {
    deliver_cb1 =
        [this, deliver_cb](std::unique_ptr<MessageReceived>& message) {
          try {
            deliver_cb->Call(FromUint(message->GetSubscriptionHandle()),
                             FromSequenceNumber(message->GetSequenceNumber()),
                             FromSlice(message->GetContents()));
          } catch (const std::exception& e) {
            LOG_WARN(info_log_,
                     "MessageReceivedCallback caught exception: %s",
                     e.what());
          }
        };
  }

  rocketspeed::SubscribeCallback subscribe_cb1 = nullptr;
  if (subscribe_cb) {
    subscribe_cb1 = [this, subscribe_cb](const SubscriptionStatus& status) {
      try {
        subscribe_cb->Call(status.GetTenant(),
                           status.GetNamespace(),
                           status.GetTopicName(),
                           FromSequenceNumber(status.GetSequenceNumber()),
                           status.IsSubscribed(),
                           FromStatus(status.GetStatus()));
      } catch (const std::exception& e) {
        LOG_WARN(info_log_, "SubscribeCallback caught exception: %s", e.what());
      }
    };
  }

  auto sub_handle = client_->Subscribe(tenant_id1,
                                       std::move(namespace_id),
                                       std::move(topic_name),
                                       ToSequenceNumber(start_seqno),
                                       std::move(deliver_cb1),
                                       std::move(subscribe_cb1));
  if (!sub_handle) {
    throw std::runtime_error("Failed to create subscription.");
  }
  return FromUint(sub_handle);
}

int64_t ClientWrapper::Resubscribe(
    SubscriptionParameters params,
    std::shared_ptr<MessageReceivedCallback> deliver_cb,
    std::shared_ptr<SubscribeCallback> subscribe_cb) {
  return Subscribe(params.tenant_id,
                   std::move(params.namespace_id),
                   std::move(params.topic_name),
                   params.start_seqno,
                   std::move(deliver_cb),
                   std::move(subscribe_cb));
}

void ClientWrapper::Unsubscribe(int64_t sub_handle) {
  auto st = client_->Unsubscribe(ToUint(sub_handle));
  if (!st.ok()) {
    throw std::runtime_error(st.ToString());
  }
}

class MessageReceivedImpl : public rocketspeed::MessageReceived {
 public:
  MessageReceivedImpl(rocketspeed::SubscriptionHandle sub_handle,
                      rocketspeed::SequenceNumber seqno)
  : sub_handle_(sub_handle), seqno_(seqno) {}

  SubscriptionHandle GetSubscriptionHandle() const override {
    return sub_handle_;
  };

  SequenceNumber GetSequenceNumber() const override { return seqno_; };

  Slice GetContents() const override {
    assert(false);
    return Slice();
  }

 private:
  rocketspeed::SubscriptionHandle sub_handle_;
  rocketspeed::SequenceNumber seqno_;
};

void ClientWrapper::Acknowledge(int64_t sub_handle, int64_t seqno) {
  MessageReceivedImpl message(ToUint(sub_handle), ToSequenceNumber(seqno));
  client_->Acknowledge(message);
}

void ClientWrapper::SaveSubscriptions(
    std::shared_ptr<SnapshotCallback> subscriptions_cb) {
  rocketspeed::SaveSubscriptionsCallback subscriptions_cb1 = nullptr;
  if (subscriptions_cb) {
    subscriptions_cb1 = [this, subscriptions_cb](rocketspeed::Status status) {
      try {
        subscriptions_cb->Call(FromStatus(status));
      } catch (const std::exception& e) {
        LOG_WARN(info_log_, "SnapshotCallback caught exception: %s", e.what());
      };
    };
  }
  client_->SaveSubscriptions(std::move(subscriptions_cb1));
}

std::vector<SubscriptionParameters> ClientWrapper::RestoreSubscriptions() {
  std::vector<rocketspeed::SubscriptionParameters> subscriptions;
  auto st = client_->RestoreSubscriptions(&subscriptions);
  if (!st.ok()) {
    throw std::runtime_error(st.ToString());
  }
  std::vector<SubscriptionParameters> subscriptions1;
  subscriptions1.reserve(subscriptions.size());
  for (auto& sub : subscriptions) {
    subscriptions1.emplace_back(sub.tenant_id,
                                sub.namespace_id,
                                sub.topic_name,
                                FromSequenceNumber(sub.start_seqno));
  }
  return subscriptions1;
}

void ClientWrapper::Close() {
  client_.reset();
  info_log_.reset();
}

}  // namespace djinni
}  // namespace rocketspeed
