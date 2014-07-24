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

/**
 * Given a serialized header, convert it to a real object
 */
MessageHeader::MessageHeader(Slice* in) {
  assert(in->size() >= GetSize());
  memcpy(&version_, in->data(), sizeof(version_));  // extract version
  in->remove_prefix(sizeof(version_));
  assert(version_ <= ROCKETSPEED_CURRENT_MSG_VERSION);
  GetFixed32(in, &msgsize_);                       // extract msg size
}

 /**
  * Creates a Message of the appropriate subtype by looking at the
  * MessageType. Returns nullptr on error. It is the responsibility
  * of the caller to own this memory object.
  **/
Message* Message::CreateNewInstance(Slice* in) {
  MessageData* msg1 = nullptr;
  MessageMetadata* msg2 = nullptr;
  MessageType mtype;

  // make a temporary copy of the input slice
  Slice tmp(*in);
  assert(in->size() >= MessageHeader::GetSize());

  // remove msg header
  tmp.remove_prefix(MessageHeader::GetSize());

  // extract msg type
  memcpy(&mtype, in->data(), sizeof(mtype));

  switch (mtype) {
    case mData:
      msg1 = new MessageData();
      msg1->DeSerialize(in);
      return msg1;
    case mMetadata:
      msg2 = new MessageMetadata();
      msg2->DeSerialize(in);
      return msg2;
    default:
      break;
  }
  return nullptr;
}

MessageData::MessageData(TenantID tenantID,
                         const Slice& topic_name,
                         const Slice& payload):
  topic_name_(topic_name),
  payload_(payload) {
  msghdr_.version_ =  ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mData;
  tenantid_ = tenantID;
  seqno_ = 0;

  // TODO(dhruba) 1 : generate GUID here
  for (unsigned int i = 0; i < sizeof(msgid_); i++) {
    msgid_.messageId[i] = i;
  }
}

MessageData::MessageData() : MessageData(Tenant::Invalid, Slice(), Slice()) {}

MessageData::~MessageData() {
}

Slice MessageData::Serialize() const {
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serialize_buffer__.append((const char *)&msghdr_.version_,
                            sizeof(msghdr_.version_));
  PutFixed32(&serialize_buffer__, msghdr_.msgsize_);
  serialize_buffer__.append((const char *)&type_, sizeof(type_));
  PutVarint32(&serialize_buffer__, tenantid_);
  PutVarint64(&serialize_buffer__, seqno_);
  PutLengthPrefixedSlice(&serialize_buffer__,
                         Slice((const char*)&msgid_, sizeof(msgid_)));
  PutLengthPrefixedSlice(&serialize_buffer__, topic_name_);
  PutLengthPrefixedSlice(&serialize_buffer__, payload_);

  // compute the size of this message
  msghdr_.msgsize_ = serialize_buffer__.size();
  std::string mlength;
  PutFixed32(&mlength, msghdr_.msgsize_);
  assert(mlength.size() == sizeof(msghdr_.msgsize_));

  // Update the 4byte-msg size starting from the 2nd byte
  serialize_buffer__.replace(1, sizeof(msghdr_.msgsize_), mlength);
  return Slice(serialize_buffer__);
}

Status MessageData::DeSerialize(Slice* in) {
  const unsigned int len = in->size();
  if (len < sizeof(msghdr_.msgsize_) + sizeof(msghdr_.version_) +
      sizeof(type_)) {
    return Status::InvalidArgument("Bad Message Version/Type");
  }
  // extract msg version
  memcpy(&msghdr_.version_, in->data(), sizeof(msghdr_.version_));
  in->remove_prefix(sizeof(msghdr_.version_));

  // If we do not support this version, then return error
  if (msghdr_.version_ > ROCKETSPEED_CURRENT_MSG_VERSION) {
    return Status::NotSupported("Bad Message Version");
  }
  // extract msg size
  if (!GetFixed32(in, &msghdr_.msgsize_)) {
    return Status::InvalidArgument("Bad msg size");
  }
  // extract type
  memcpy(&type_, in->data(), sizeof(type_));
  in->remove_prefix(sizeof(type_));

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
  hostid_(hostid),
  topics_(topics) {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mMetadata;
  tenantid_ = tenantID;
  seqno_ = seqno;
}

MessageMetadata::MessageMetadata() {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mMetadata;
  tenantid_ = Tenant::Invalid;
  seqno_ = 0;
}

MessageMetadata::~MessageMetadata() {
}

Slice MessageMetadata::Serialize() const {
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();

  // version and msg size
  serialize_buffer__.append((const char *)&msghdr_.version_,
                            sizeof(msghdr_.version_));
  PutFixed32(&serialize_buffer__, msghdr_.msgsize_);

  // Type, tenantId and seqno
  serialize_buffer__.append((const char *)&type_, sizeof(type_));
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
  // compute msg size
  msghdr_.msgsize_ = serialize_buffer__.size();
  std::string mlength;
  PutFixed32(&mlength, msghdr_.msgsize_);
  assert(mlength.size() == sizeof(msghdr_.msgsize_));

  // Update the 4-bye msg size starting from the 1 byte
  serialize_buffer__.replace(1, sizeof(msghdr_.msgsize_), mlength);
  return Slice(serialize_buffer__);
}

Status MessageMetadata::DeSerialize(Slice* in) {
  if (in->size() < sizeof(msghdr_.msgsize_) + sizeof(msghdr_.version_) +
      sizeof(type_)) {
    return Status::InvalidArgument("Bad Message Version/Type");
  }
  // extract version
  memcpy(&msghdr_.version_, in->data(), sizeof(msghdr_.version_));
  in->remove_prefix(sizeof(msghdr_.version_));

  // If we do not support this version, then return error
  if (msghdr_.version_ > ROCKETSPEED_CURRENT_MSG_VERSION) {
    return Status::NotSupported("Bad Message Version");
  }

  // extract msg size
  if (!GetFixed32(in, &msghdr_.msgsize_)) {
    return Status::InvalidArgument("Bad msg size");
  }

  // extract type
  memcpy(&type_, in->data(), sizeof(type_));
  in->remove_prefix(sizeof(type_));

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
