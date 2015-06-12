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

#include "include/Status.h"
#include "include/Types.h"
#include "include/RocketSpeed.h"
#include "src/djinni/jvm_env.h"
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

namespace {

int64_t FromSequenceNumber(uint64_t uval) {
  using Limits = std::numeric_limits<int64_t>;
  if (uval <= static_cast<uint64_t>(Limits::min()))
    return static_cast<int64_t>(uval) + Limits::min();
  if (uval >= static_cast<uint64_t>(Limits::min()))
    return static_cast<int64_t>(uval - Limits::min());
  assert(false);
  return 0;
}

uint64_t ToSequenceNumber(int64_t uval) {
  using Limits = std::numeric_limits<int64_t>;
  return static_cast<uint64_t>(uval) - Limits::min();
}

int64_t FromSubscriptionHandle(uint64_t uval) {
  using Limits = std::numeric_limits<int64_t>;
  if (uval <= static_cast<uint64_t>(Limits::min()))
    return static_cast<int64_t>(uval);
  if (uval >= static_cast<uint64_t>(Limits::min()))
    return static_cast<int64_t>(uval - Limits::min()) + Limits::min();
  assert(false);
  return 0;
}

uint64_t ToSubscriptionHandle(int64_t val) {
  return static_cast<uint64_t>(val);
}

rocketspeed::InfoLogLevel ToInfoLogLevel(LogLevel log_level) {
  using rocketspeed::InfoLogLevel;
  static_assert(InfoLogLevel::DEBUG_LEVEL ==
                    static_cast<InfoLogLevel>(LogLevel::DEBUG_LEVEL),
                "Enum representations do not match.");
  static_assert(InfoLogLevel::NUM_INFO_LOG_LEVELS ==
                    static_cast<InfoLogLevel>(LogLevel::NUM_INFO_LOG_LEVELS),
                "Enum representations do not match.");
  return static_cast<InfoLogLevel>(log_level);
}

Status FromStatus(rocketspeed::Status status) {
  StatusCode code = StatusCode::INTERNAL;
  if (status.ok()) {
    code = StatusCode::OK;
  } else if (status.IsNotFound()) {
    code = StatusCode::NOTFOUND;
  } else if (status.IsNotSupported()) {
    code = StatusCode::NOTSUPPORTED;
  } else if (status.IsInvalidArgument()) {
    code = StatusCode::INVALIDARGUMENT;
  } else if (status.IsIOError()) {
    code = StatusCode::IOERROR;
  } else if (status.IsNotInitialized()) {
    code = StatusCode::NOTINITIALIZED;
  } else if (status.IsUnauthorized()) {
    code = StatusCode::UNAUTHORIZED;
  } else if (status.IsTimedOut()) {
    code = StatusCode::TIMEDOUT;
  } else if (status.IsInternal()) {
    code = StatusCode::INTERNAL;
  } else {
    assert(false);
  }
  return Status(code, std::move(status.ToString()));
}

rocketspeed::MsgId ToMsgId(const MsgId& message_id) {
  union {
    char id[16];
    struct {
      int64_t hi, lo;
    };
  } value;
  value.hi = message_id.hi;
  value.lo = message_id.lo;
  return rocketspeed::MsgId(value.id);
}

MsgId FromMsgId(const rocketspeed::MsgId& message_id) {
  union {
    char id[16];
    struct {
      int64_t hi, lo;
    };
  } value;
  memcpy(value.id, message_id.id, 16);
  return MsgId(value.hi, value.lo);
}

rocketspeed::Slice ToSlice(const std::vector<uint8_t>& data) {
  auto first = reinterpret_cast<const char*>(data.data());
  return rocketspeed::Slice(first, data.size());
}

std::vector<uint8_t> FromSlice(Slice slice) {
  auto first = reinterpret_cast<const uint8_t*>(slice.data());
  return std::vector<uint8_t>(first, first + slice.size());
}

}  // namespace

std::shared_ptr<ClientImpl> ClientImpl::Create(LogLevel log_level,
                                               HostId cockpit,
                                               SubscriptionStorage storage) {
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

  rocketspeed::HostId cockpit1(std::move(cockpit.host), cockpit_port);
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
      auto st = rocketspeed::SubscriptionStorage::File(
          options.env, options.info_log, storage.file_path, &options.storage);
      if (!st.ok()) {
        throw std::runtime_error(st.ToString());
      }
      break;
      // No default, we will be warned about missing branch.
  }

  std::unique_ptr<rocketspeed::Client> client;
  auto st = rocketspeed::Client::Create(std::move(options), &client);
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
            deliver_cb->Call(
                FromSubscriptionHandle(message->GetSubscriptionHandle()),
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
  return FromSubscriptionHandle(sub_handle);
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
  auto st = client_->Unsubscribe(ToSubscriptionHandle(sub_handle));
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
  MessageReceivedImpl message(ToSubscriptionHandle(sub_handle),
                              ToSequenceNumber(seqno));
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
