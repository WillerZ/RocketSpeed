//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "rollcall_impl.h"

#include "include/Slice.h"
#include "src/client/client.h"
#include "src/util/common/coding.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

Status RollcallStream::Open(ClientOptions client_options,
                            const TenantID tenant_id,
                            std::unique_ptr<RollcallStream>* stream) {
  std::unique_ptr<ClientImpl> client;
  Status status = ClientImpl::Create(std::move(client_options), &client, true);
  if (!status.ok()) {
    return status;
  }
  stream->reset(new RollcallImpl(std::move(client), tenant_id));
  return Status::OK();
}

const NamespaceID RollcallImpl::kRollcallNamespace = "_r";

RollcallImpl::RollcallImpl(std::unique_ptr<ClientImpl> client,
                           const TenantID tenant_id)
    : client_(std::move(client)), tenant_id_(tenant_id) {
}

RollcallShard RollcallImpl::GetNumShards(const NamespaceID& namespace_id) {
  if (namespace_id == GuestNamespace) {
    // Guest namespace is extensively used in tests.
    return 4;
  } else {
    return 16 * 1024; // 16K logs
  }
}

Status RollcallImpl::Subscribe(const NamespaceID& namespace_id,
                               RollcallShard shard_id,
                               const RollCallback callback) {
  if (shard_id >= GetNumShards(namespace_id)) {
    return Status::InvalidArgument("Shard ID out of range");
  }
  if (!callback) {
    return Status::InvalidArgument("Missing callback");
  }

  auto subscribe_callback = [callback](const SubscriptionStatus& ss) {
    if (!ss.GetStatus().ok()) {
      // Notify about failed subscription.
      callback(RollcallEntry());
    }
  };
  auto receive_callback = [callback](std::unique_ptr<MessageReceived>& msg) {
    RollcallEntry rmsg;
    rmsg.DeSerialize(msg->GetContents());
    callback(std::move(rmsg));
  };

  auto handle =
      client_->Client::Subscribe(tenant_id_,
                                 kRollcallNamespace,
                                 GetRollcallTopicName(namespace_id, shard_id),
                                 0,
                                 std::move(receive_callback),
                                 std::move(subscribe_callback));
  return handle ? Status::OK()
                : Status::InternalError("Failed to create subscription.");
}

Status RollcallImpl::WriteEntry(const TenantID tenant_id,
                                const Topic& topic_name,
                                const NamespaceID& nsid,
                                bool isSubscription,
                                PublishCallback publish_callback) {
  if (nsid == kRollcallNamespace) {
    // We do not write rollcall subscriptions into the rollcall.
    return Status::OK();
  }

  // Serialize the entry
  RollcallEntry impl(topic_name,
                     isSubscription
                         ? RollcallEntry::EntryType::SubscriptionRequest
                         : RollcallEntry::EntryType::UnSubscriptionRequest);
  std::string serial;
  impl.Serialize(&serial);

  // write it out to rollcall topic
  Topic rollcall_topic(
      GetRollcallTopicName(nsid, GetRollcallShard(nsid, topic_name)));
  return client_->Publish(tenant_id,
                          std::move(rollcall_topic),
                          kRollcallNamespace,
                          TopicOptions(),
                          Slice(serial),
                          std::move(publish_callback),
                          MsgId()).status;
}

RollcallShard RollcallImpl::GetRollcallShard(const NamespaceID& namespace_id,
                                             const Topic& topic) {
  return static_cast<RollcallShard>(MurmurHash2<Topic>()(topic) %
                                    GetNumShards(namespace_id));
}

Topic RollcallImpl::GetRollcallTopicName(const NamespaceID& nsid,
                                         RollcallShard shard_id) {
  std::string topic = nsid;
  topic.push_back('.');
  char buf[2];
  EncodeFixed16(buf, shard_id);
  Slice shard_slice(buf, sizeof(buf));
  topic.append(shard_slice.ToString(true));
  return topic;
}

void RollcallEntry::Serialize(std::string* buffer) {
  buffer->reserve(4 + topic_name_.size());
  buffer->resize(4);
  (*buffer)[0] = version_;
  (*buffer)[1] = (*buffer)[3] = '_';
  (*buffer)[2] = entry_type_;
  buffer->append(topic_name_);
}

Status RollcallEntry::DeSerialize(Slice in) {
  if (in.size() < 4) {
    return Status::InvalidArgument("Invalid rollcall entry");
  }
  version_ = in[0];
  entry_type_ = static_cast<EntryType>(in[2]);
  if (!ValidateEnum(entry_type_)) {
    return Status::InvalidArgument("Invalid rollcall entry type");
  }
  in.remove_prefix(4);
  topic_name_.clear();
  topic_name_.append(in.data(), in.size());
  return Status::OK();
}

}  // namespace rocketspeed
