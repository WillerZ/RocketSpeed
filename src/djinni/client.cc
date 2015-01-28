// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "client.h"

#include <cassert>
#include <stdexcept>
#include <memory>
#include <limits>
#include <string>

#include "include/Status.h"
#include "include/Types.h"
#include "include/RocketSpeed.h"
#include "src/djinni/jvm_env.h"
#include "src/djinni/wake_lock.h"

#include "src-gen/djinni/HostId.hpp"
#include "src-gen/djinni/ConfigurationImpl.hpp"
#include "src-gen/djinni/PublishCallbackImpl.hpp"
#include "src-gen/djinni/ReceiveCallbackImpl.hpp"
#include "src-gen/djinni/StorageType.hpp"
#include "src-gen/djinni/SubscribeCallbackImpl.hpp"
#include "src-gen/djinni/SubscriptionStorage.hpp"
#include "src-gen/djinni/WakeLockImpl.hpp"

namespace rocketspeed {
namespace djinni {

namespace {

std::unique_ptr<rocketspeed::Configuration> toConfiguration(
    ConfigurationImpl config,
    int32_t tenant_id) {
  std::vector<rocketspeed::HostId> pilots, copilots;
  for (auto& host_id : config.pilots) {
    pilots.emplace_back(std::move(host_id.hostname), host_id.port);
  }
  for (auto& host_id : config.copilots) {
    copilots.emplace_back(std::move(host_id.hostname), host_id.port);
  }
  auto tenant_id1 = static_cast<rocketspeed::TenantID>(tenant_id);
  assert(tenant_id == tenant_id1);
  std::unique_ptr<Configuration> config1(
      Configuration::Create(pilots, copilots, tenant_id1));
  return std::move(config1);
}

rocketspeed::MsgId toMsgId(const MsgIdImpl& message_id) {
  assert(MsgIdImpl::SIZE == sizeof(static_cast<rocketspeed::MsgId>(0).id));
  assert(MsgIdImpl::SIZE == message_id.guid.size());
  // Pointer decay guarded with asserts.
  auto guid = reinterpret_cast<const char*>(message_id.guid.data());
  return rocketspeed::MsgId(guid);
}

MsgIdImpl fromMsgId(const rocketspeed::MsgId& message_id) {
  assert(MsgIdImpl::SIZE == sizeof(static_cast<rocketspeed::MsgId>(0).id));
  auto first = reinterpret_cast<const uint8_t*>(message_id.id);
  std::vector<uint8_t> raw(first, first + MsgIdImpl::SIZE);
  return MsgIdImpl(std::move(raw));
}

int64_t fromSequenceNumber(rocketspeed::SequenceNumber seqno) {
  using Limits = std::numeric_limits<int64_t>;
  // I'm unaware of any better conversion that avoids undefined behaviours.
  if (seqno <= Limits::min())
    return static_cast<int64_t>(seqno);
  if (seqno >= Limits::min())
    return static_cast<int64_t>(seqno - Limits::min()) + Limits::min();
  assert(false);
  return 0;
}

rocketspeed::SequenceNumber toSequenceNumber(int64_t seqno) {
  using Limits = std::numeric_limits<int64_t>;
  // The min is promoted to uint64_t.
  return static_cast<uint64_t>(seqno - Limits::min());
}

int64_t fromNamespaceID(rocketspeed::NamespaceID namespace_id) {
  return namespace_id;
}

rocketspeed::NamespaceID toNamespaceID(int64_t namespace_id) {
  auto namespace_id1 = static_cast<rocketspeed::NamespaceID>(namespace_id);
  assert(namespace_id == namespace_id1);
  return namespace_id;
}

rocketspeed::Retention toRetention(RetentionBase retention) {
  switch (retention) {
    case RetentionBase::ONEHOUR:
      return rocketspeed::Retention::OneHour;
    case RetentionBase::ONEDAY:
      return rocketspeed::Retention::OneDay;
    case RetentionBase::ONEWEEK:
      return rocketspeed::Retention::OneWeek;
    default:
      assert(false);
      return rocketspeed::Retention::OneWeek;
  }
}

rocketspeed::SubscriptionRequest toSubscriptionRequest(
    SubscriptionRequestImpl request) {
  SubscriptionStart start = request.start
                                ? toSequenceNumber(request.start.value())
                                : SubscriptionStart();
  return SubscriptionRequest(toNamespaceID(request.namespace_id),
                             std::move(request.topic_name),
                             request.subscribe,
                             start);
}

std::vector<uint8_t> fromSlice(Slice slice) {
  auto first = reinterpret_cast<const uint8_t*>(slice.data());
  return std::vector<uint8_t>(first, first + slice.size());
}

Slice toSlice(const std::string& data) {
  return Slice(data.data(), data.size());
}

Slice toSlice(const std::vector<uint8_t>& data) {
  auto first = reinterpret_cast<const char*>(data.data());
  return Slice(first, data.size());
}

Status fromStatus(rocketspeed::Status status) {
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

PublishStatus fromPublishStatus(rocketspeed::PublishStatus status) {
  return PublishStatus(fromStatus(status.status), fromMsgId(status.msgid));
}

}  // namespace

std::shared_ptr<ClientImpl> ClientImpl::Open(
    ConfigurationImpl config,
    int32_t tenant_id,
    std::string client_id,
    std::shared_ptr<SubscribeCallbackImpl> subscribe_callback,
    std::shared_ptr<ReceiveCallbackImpl> receive_callback,
    SubscriptionStorage storage,
    std::shared_ptr<WakeLockImpl> wake_lock) {
  auto config1 = toConfiguration(config, tenant_id);

  rocketspeed::ClientOptions options(*config1, client_id);

  options.env = JvmEnv::Default();

  if (wake_lock) {
    options.wake_lock = std::make_shared<WakeLock>(wake_lock);
  }

  options.subscription_callback =
      [subscribe_callback](SubscriptionStatus status) {
    subscribe_callback->Call(fromStatus(status.status),
                             fromSequenceNumber(status.seqno),
                             status.subscribed);
  };

  options.receive_callback =
      [receive_callback](std::unique_ptr<MessageReceived> message) {
    receive_callback->Call(fromNamespaceID(message->GetNamespaceId()),
                           message->GetTopicName().ToString(),
                           fromSequenceNumber(message->GetSequenceNumber()),
                           fromSlice(message->GetContents()));
  };

  std::shared_ptr<ClientImpl> client;
  { // Create the RocketSpeed client and wrap it in Djinni handler.
    rocketspeed::Client* client_raw;
    auto status = rocketspeed::Client::Open(std::move(options), &client_raw);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    client.reset(new Client(client_raw));
  }

  return client;
}

PublishStatus Client::Publish(
    int32_t namespace_id,
    std::string topic_name,
    RetentionBase retention,
    std::vector<uint8_t> data,
    std::experimental::optional<MsgIdImpl> message_id,
    std::shared_ptr<PublishCallbackImpl> publish_callback) {
  PublishCallback publish_callback1 = nullptr;
  if (publish_callback) {
    publish_callback1 =
        [publish_callback](std::unique_ptr<ResultStatus> status) {
      publish_callback->Call(fromStatus(status->GetStatus()),
                             status->GetNamespaceId(),
                             status->GetTopicName().ToString(),
                             fromMsgId(status->GetMessageId()),
                             fromSequenceNumber(status->GetSequenceNumber()));
    };
  }

  TopicOptions topic_options(toRetention(retention));
  rocketspeed::PublishStatus status;
  if (message_id) {
    status = client_->Publish(topic_name,
                              namespace_id,
                              topic_options,
                              toSlice(data),
                              publish_callback1,
                              toMsgId(message_id.value()));
  } else {
    status = client_->Publish(topic_name,
                              namespace_id,
                              topic_options,
                              toSlice(data),
                              publish_callback1);
  }

  return fromPublishStatus(status);
}

void Client::ListenTopics(std::vector<SubscriptionRequestImpl> requests) {
  std::vector<SubscriptionRequest> requests1;
  for (auto& request : requests) {
    requests1.push_back(toSubscriptionRequest(request));
  }
  client_->ListenTopics(requests1);
}

namespace {

class MessageForAcknowledgement : public rocketspeed::MessageReceived {
 public:
  MessageForAcknowledgement(rocketspeed::NamespaceID namespace_id,
                            rocketspeed::Slice topic_name,
                            rocketspeed::SequenceNumber seqno)
      : namespace_id_(namespace_id), topic_name_(topic_name), seqno_(seqno) {}

  NamespaceID GetNamespaceId() const { return namespace_id_; }

  const Slice GetTopicName() const { return topic_name_; }

  SequenceNumber GetSequenceNumber() const { return seqno_; }

  const Slice GetContents() const {
    assert(false);
    return Slice();
  }

  ~MessageForAcknowledgement() {}

 private:
  rocketspeed::NamespaceID namespace_id_;
  rocketspeed::Slice topic_name_;
  rocketspeed::SequenceNumber seqno_;
};

}  // namespace

void Client::Acknowledge(int32_t namespace_id,
                         std::string topic_name,
                         int64_t sequence_number) {
  std::unique_ptr<MessageReceived> message(
      new MessageForAcknowledgement(fromNamespaceID(namespace_id),
                                    toSlice(topic_name),
                                    fromSequenceNumber(sequence_number)));
  // This call never looks at message content.
  client_->Acknowledge(*message);
}

void Client::Close() { client_.reset(); }

}  // namespace djinni
}  // namespace rocketspeed
