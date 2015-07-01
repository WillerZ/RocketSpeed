//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "rollcall_impl.h"

#include "external/folly/move_wrapper.h"

#include "include/Slice.h"
#include "src/client/client.h"
#include "src/util/topic_uuid.h"
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

RollcallImpl::RollcallImpl(std::shared_ptr<ClientImpl> client,
                           const TenantID tenant_id,
                           std::string stats_prefix)
    : client_(std::move(client)),
      tenant_id_(tenant_id),
      stats_(std::move(stats_prefix)) {
}

RollcallShard RollcallImpl::GetNumShards(const NamespaceID& namespace_id) {
  if (namespace_id == GuestNamespace) {
    // Guest namespace is extensively used in tests.
    return 4;
  } else {
    return 400;
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
  auto receive_callback = [callback] (std::unique_ptr<MessageReceived>& msg) {
    Slice in(msg->GetContents());
    if (in.size() >= 2 && in[1] == '_') {
      auto version = in[0];
      if (version == RollcallEntry::ROLLCALL_ENTRY_VERSION_CURRENT) {
        in.remove_prefix(2);
        while (!in.empty()) {
          RollcallEntry rmsg(version);
          if (!rmsg.DeSerialize(&in).ok()) {
            break;
          }
          callback(std::move(rmsg));
        }
      }
    }
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
                                const TopicUUID& topic,
                                size_t shard_affinity,
                                bool isSubscription,
                                std::function<void(Status)> publish_callback,
                                size_t max_batch_size_bytes) {
  thread_check_.Check();

  Slice namespace_id;
  Slice topic_name;
  topic.GetTopicID(&namespace_id, &topic_name);

  NamespaceID nsid = namespace_id.ToString();
  if (nsid == kRollcallNamespace) {
    // We do not write rollcall subscriptions into the rollcall.
    return Status::OK();
  }

  // Serialize the entry
  RollcallEntry impl(topic_name.ToString(),
                     isSubscription
                         ? RollcallEntry::EntryType::SubscriptionRequest
                         : RollcallEntry::EntryType::UnSubscriptionRequest);

  const RollcallShard shard = GetRollcallShard(nsid, shard_affinity);
  const BatchKey batch_key(shard, std::move(nsid), tenant_id);

  Batch& batch = batches_[batch_key];
  if (batch.payload.empty()) {
    // First entry -- write batch header.
    batch.payload.push_back(RollcallEntry::ROLLCALL_ENTRY_VERSION_CURRENT);
    batch.payload.push_back('_');

    // Also add to batch timeout list.
    batch_timeouts_.Add(batch_key);
  }
  impl.Serialize(&batch.payload);
  batch.callbacks.emplace_back(std::move(publish_callback));

  if (batch.payload.size() >= max_batch_size_bytes) {
    FlushBatch(batch_key);
    batch_timeouts_.Erase(batch_key);
    stats_.batch_size_writes->Add(1);
  }
  return Status::OK();
}

void RollcallImpl::CheckBatchTimeouts(std::chrono::milliseconds timeout) {
  thread_check_.Check();
  batch_timeouts_.ProcessExpired(
    timeout,
    [this] (const BatchKey& key) {
      FlushBatch(key);
      stats_.batch_timeout_writes->Add(1);
    },
    -1 /* process all */);
}

Status RollcallImpl::FlushBatch(const BatchKey& key) {
  thread_check_.Check();
  Status st;
  Batch& batch = batches_[key];
  if (!batch.payload.empty()) {
    // write it out to rollcall topic
    const RollcallShard shard = std::get<0>(key);
    const NamespaceID& nsid = std::get<1>(key);
    const TenantID tenant_id = std::get<2>(key);

    const size_t num_entries = batch.callbacks.size();
    stats_.batch_size_bytes->Record(batch.payload.size());
    stats_.batch_size_entries->Record(num_entries);
    stats_.batch_writes->Add(1);
    stats_.entry_writes->Add(num_entries);

    auto moved_callbacks = folly::makeMoveWrapper(std::move(batch.callbacks));
    PublishCallback publish_callback =
      [moved_callbacks] (std::unique_ptr<ResultStatus> result) {
        for (auto& callback : *moved_callbacks) {
          // Invoke all callbacks from the batch.
          callback(result->GetStatus());
        }
      };
    st = client_->Publish(tenant_id,
                          GetRollcallTopicName(nsid, shard),
                          kRollcallNamespace,
                          TopicOptions(),
                          Slice(batch.payload),
                          std::move(publish_callback),
                          MsgId()).status;
    batch.payload.clear();
    batch.callbacks.clear();
  }
  return st;
}

RollcallShard RollcallImpl::GetRollcallShard(const NamespaceID& namespace_id,
                                             size_t shard_affinity) {
  return static_cast<RollcallShard>(shard_affinity %
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
  buffer->push_back(entry_type_);
  buffer->push_back('_');
  PutLengthPrefixedSlice(buffer, Slice(topic_name_));
}

Status RollcallEntry::DeSerialize(Slice* in) {
  if (!GetFixedEnum8(in, &entry_type_)) {
    return Status::InvalidArgument("Invalid rollcall entry type");
  }
  if (in->empty() || (*in)[0] != '_') {
    return Status::InvalidArgument("Invalid rollcall entry format");
  }
  in->remove_prefix(1);
  if (!GetLengthPrefixedSlice(in, &topic_name_)) {
    return Status::InvalidArgument("Invalid rollcall entry topic name");
  }
  return Status::OK();
}

RollcallImpl::Stats::Stats(std::string prefix) {
  batch_size_bytes =
    all.AddHistogram(prefix + ".batch_size_bytes", 0, 1 << 20, 1);
  batch_size_entries =
    all.AddHistogram(prefix + ".batch_size_entries", 0, 1 << 20, 1);
  batch_writes = all.AddCounter(prefix + ".batch_writes");
  entry_writes = all.AddCounter(prefix + ".entry_writes");
  batch_size_writes = all.AddCounter(prefix + ".batch_size_writes");
  batch_timeout_writes = all.AddCounter(prefix + ".batch_timeout_writes");
}

}  // namespace rocketspeed
