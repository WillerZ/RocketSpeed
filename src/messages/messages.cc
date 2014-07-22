// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/messages.h"

#include <string>
#include <vector>

#include "include/Slice.h"
#include "include/Status.h"
#include "src/util/coding.h"

/*
 * This file contains all the messages used by RocketSpeed. These messages are
 * the only means of communication between a client, pilot, copilot and
 * control tower. These are interal to RocketSpeed and can change from one
 * release to another. Applications should not use these messages to communicate
 * with RocketSpeed, instead applications should use the public api specified
 * in include/RocketSpeed.h to interact with RocketSpeed.
 * All messages have to implement the Serializer interface.
 */
namespace rocketspeed {

MessageData::MessageData(TenantID tenantID,
                         const Slice& topic_name,
                         const Slice& payload):
  type_(mData),
  tenantid_(tenantID),
  seqno_(0),
  topic_name_(topic_name),
  payload_(payload) {
  version__ =  ROCKETSPEED_CURRENT_MSG_VERSION;

  // TODO(dhruba) 1 : generate GUID here
  for (unsigned int i = 0; i < sizeof(msgid_); i++) {
    msgid_.messageId[i] = i;
  }
}

MessageData::MessageData() : MessageData(Tenant::Invalid, Slice(), Slice()) {}

MessageData::~MessageData() {
}

Slice MessageData::Serialize() {
  serialize_buffer__.clear();
  serialize_buffer__.append((const char *)&type_, sizeof(type_));
  serialize_buffer__.append((const char *)&version__, sizeof(version__));
  PutVarint32(&serialize_buffer__, tenantid_);
  PutVarint64(&serialize_buffer__, seqno_);
  PutLengthPrefixedSlice(&serialize_buffer__,
                         Slice((const char*)&msgid_, sizeof(msgid_)));
  PutLengthPrefixedSlice(&serialize_buffer__, topic_name_);
  PutLengthPrefixedSlice(&serialize_buffer__, payload_);
  return Slice(serialize_buffer__);
}

Status MessageData::DeSerialize(Slice* in) {
  const unsigned int len = in->size();
  if (len < sizeof(version__) + sizeof(type_)) {
    return Status::InvalidArgument("Bad Message Version/Type");
  }

  // extract type and version of message
  memcpy(&type_, in->data(), sizeof(type_));
  in->remove_prefix(sizeof(type_));
  memcpy(&version__, in->data(), sizeof(version__));
  in->remove_prefix(sizeof(version__));

  // If we do not support this version, then return error
  if (version__ > ROCKETSPEED_CURRENT_MSG_VERSION) {
    return Status::NotSupported("Bad Message Version");
  }

  if (!GetVarint32(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract sequence number of message
  if (!GetVarint64(in, &seqno_)) {
    return Status::InvalidArgument("Bad Sequence Number");
  }

  // extract message id
  Slice idSlice;
  if (!GetLengthPrefixedSlice(in, &idSlice) ||
      idSlice.size() < sizeof(msgid_)) {
    return Status::InvalidArgument("Bad Message Id");
  }
  memcpy(&msgid_, idSlice.data(), sizeof(msgid_));

  // extract message topic
  if (!GetLengthPrefixedSlice(in, &topic_name_)) {
    return Status::InvalidArgument("Bad Message Topic name");
  }

  // extract message payload
  if (!GetLengthPrefixedSlice(in, &payload_)) {
    return Status::InvalidArgument("Bad Message Payload");
  }
  return Status::OK();
}

MessageMetadata::MessageMetadata(TenantID tenantID,
  const SequenceNumber seqno,
  const HostId& hostid,
  const std::vector<TopicPair>& topics):
  type_(mMetadata),
  tenantid_(tenantID),
  seqno_(seqno),
  hostid_(hostid),
  topics_(topics) {
  version__ = ROCKETSPEED_CURRENT_MSG_VERSION;
}

MessageMetadata::MessageMetadata() :
  type_(mMetadata),
  tenantid_(Tenant::Invalid),
  seqno_(0) {
  version__ = ROCKETSPEED_CURRENT_MSG_VERSION;
}

MessageMetadata::~MessageMetadata() {
}

Slice MessageMetadata::Serialize() {
  serialize_buffer__.clear();

  // Type and Version
  serialize_buffer__.append((const char *)&type_, sizeof(type_));
  serialize_buffer__.append((const char *)&version__, sizeof(version__));
  PutVarint32(&serialize_buffer__, tenantid_);
  PutVarint64(&serialize_buffer__, seqno_);

  // HostId
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(hostid_.hostname));
  PutVarint64(&serialize_buffer__, hostid_.port);

  // Topics and metadata state
  PutVarint32(&serialize_buffer__, topics_.size());
  for (TopicPair p : topics_) {
    PutLengthPrefixedSlice(&serialize_buffer__, Slice(p.topic_name));
    serialize_buffer__.append((const char *)&p.topic_type,
                              sizeof(p.topic_type));
  }
  return Slice(serialize_buffer__);
}

Status MessageMetadata::DeSerialize(Slice* in) {
  if (in->size() < sizeof(version__) + sizeof(type_)) {
    return Status::InvalidArgument("Bad Message Version/Type");
  }

  // extract type and version of message
  memcpy(&type_, in->data(), sizeof(type_));
  in->remove_prefix(sizeof(type_));
  memcpy(&version__, in->data(), sizeof(version__));
  in->remove_prefix(sizeof(version__));

  // If we do not support this version, then return error
  if (version__ > ROCKETSPEED_CURRENT_MSG_VERSION) {
    return Status::NotSupported("Bad Message Version");
  }

  // extrant tenant ID
  if (!GetVarint32(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract sequence number of message
  if (!GetVarint64(in, &seqno_)) {
    return Status::InvalidArgument("Bad Sequence Number");
  }

  // extract host id
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  hostid_.hostname.clear();
  hostid_.hostname.append(sl.data(), sl.size());

  // extract port number
  if (!GetVarint64(in, &hostid_.port)) {
    return Status::InvalidArgument("Bad Port Number");
  }

  // extract number of topics
  uint32_t num_topics;
  if (!GetVarint32(in, &num_topics)) {
    return Status::InvalidArgument("Bad Number Of Topics");
  }

  // extract each topic
  for (unsigned i = 0; i < num_topics; i++) {
    TopicPair p;

    // extract one topic name
    if (!GetLengthPrefixedSlice(in, &sl)) {
      return Status::InvalidArgument("Bad Message Payload");
    }
    p.topic_name.append(sl.data(), sl.size());

    // extract one topic type
    memcpy(&p.topic_type, in->data(), sizeof(p.topic_type));
    in->remove_prefix(sizeof(p.topic_type));

    topics_.push_back(p);
  }
  return Status::OK();
}

}  // namespace rocketspeed
