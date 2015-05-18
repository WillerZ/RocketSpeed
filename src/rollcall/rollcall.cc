//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "rollcall_impl.h"
#include "src/client/client.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

const NamespaceID RollcallImpl::rollcall_namespace_ = "_rollcall";

RollcallImpl::RollcallImpl(std::unique_ptr<ClientImpl> client,
  const TenantID tenant_id,
  const NamespaceID& nsid,
  const SequenceNumber start_point,
  RollCallback callback):
  rs_client_(std::move(client)),
  nsid_(nsid),
  start_point_(start_point),
  callback_(std::move(callback)),
  rollcall_topic_(GetRollcallTopicName(nsid)),
  msgid_(MsgId()) {

  // If the subscription request was successful, then mark
  // our state as SubscriptionConfirmed. Otherwise invoke
  // user-specified callback with error state.
  auto subscribe_callback = [this] (const SubscriptionStatus& ss) {
    if (!ss.GetStatus().ok()) {
      // invoke user-specified callback with error status
      callback_(RollcallEntry());
    }
  };

  if (callback_ != nullptr) {
    // If we are tailing the log, then set it up here.
    // Callback invoked on every message received from storage.
    // It converts the incoming record into a RollcallEntry record
    // and passes it back to the application.
    auto receive_callback = [this] (std::unique_ptr<MessageReceived> msg) {
      assert(msg->GetNamespaceId() == rollcall_namespace_);
      RollcallEntry rmsg;
      rmsg.DeSerialize(msg->GetContents());
      callback_(std::move(rmsg));
    };
    rs_client_->SetDefaultCallbacks(subscribe_callback, receive_callback);

    // send a subscription request for rollcall topic
    auto handle = rs_client_->Client::Subscribe(tenant_id,
                                                rollcall_namespace_,
                                                rollcall_topic_,
                                                start_point_);
    if (!handle) {
      // invoke user-specified callback with error status
      callback_(RollcallEntry());
    }
    // Handle will be lost, but the only way we unsubscribe is by shutting down
    // the client.
  }
}

Status
RollcallImpl::WriteEntry(const TenantID tenant_id,
                         const Topic& topic_name,
                         const NamespaceID& nsid,
                         bool isSubscription,
                         PublishCallback publish_callback) {

  // Serialize the entry
  RollcallEntry impl(topic_name, isSubscription ?
                       RollcallEntry::EntryType::SubscriptionRequest :
                       RollcallEntry::EntryType::UnSubscriptionRequest);
  std::string serial;
  impl.Serialize(&serial);
  Slice sl(serial);

  // write it out to rollcall topic
  Topic rtopic(GetRollcallTopicName(nsid));
  return rs_client_->Publish(tenant_id,
                             rtopic,
                             rollcall_namespace_,
                             rollcall_topic_options_,
                             sl,
                             std::move(publish_callback),
                             msgid_).status;
}

// write this object into a serialized string
void
RollcallEntry::Serialize(std::string* buffer) {
  PutFixed8(buffer, version_);
  PutFixed8(buffer, entry_type_);
  PutLengthPrefixedSlice(buffer, Slice(topic_name_));
}

// initialize this object from a serialized string
Status
RollcallEntry::DeSerialize(Slice in) {
  Slice sl;
  // extract the version
  if (!GetFixed8(&in, reinterpret_cast<uint8_t *>(&version_))) {
   return Status::InvalidArgument("Rollcall:Bad version");
  }

  // Is this a subscription or unsubscription request?
  if (!GetFixedEnum8(&in, &entry_type_)) {
   return Status::InvalidArgument("Rollcall:Bad subscription type");
  }

  // update topic name
  if (!GetLengthPrefixedSlice(&in, &sl)) {
   return Status::InvalidArgument("Rollcall:Bad topic name");
  }
  topic_name_.clear();
  topic_name_.append(sl.data(), sl.size());
  return Status::OK();
}

// Static method to open a Rollcall stream
Status
RollcallStream::Open(ClientOptions client_options,
  const TenantID tenant_id,
  const NamespaceID& nsid,
  SequenceNumber start_point,
  RollCallback callback,
  std::unique_ptr<RollcallStream>* stream) {
  std::unique_ptr<ClientImpl> client;

  // open the client
  Status status = ClientImpl::Create(
                     std::move(client_options), &client, true);
  if (!status.ok()) {
    return status;
  }

  // create our object
  stream->reset(new RollcallImpl(std::move(client), tenant_id, nsid,
                                 start_point, callback));
  return  Status::OK();
}

}  // namespace rocketspeed
