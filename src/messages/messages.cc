// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "messages.h"

#include <string>
#include <vector>

#include "include/Slice.h"
#include "include/Status.h"
#include "src/util/common/coding.h"
#include "src/util/common/guid_generator.h"

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
std::unique_ptr<Message>
Message::CreateNewInstance(Slice* in) {
  MessagePing* msg0 = nullptr;
  MessageData* msg1 = nullptr;
  MessageMetadata* msg2 = nullptr;
  MessageDataAck* msg3 = nullptr;
  MessageGap* msg4 = nullptr;
  MessageType mtype;

  // make a temporary copy of the input slice
  Slice tmp(*in);
  if (in->size() < MessageHeader::GetSize()) {
    return nullptr;
  }

  // remove msg header
  tmp.remove_prefix(MessageHeader::GetSize());

  // extract msg type
  memcpy(&mtype, tmp.data(), sizeof(mtype));

  Status st;
  switch (mtype) {
    case mPing:
      msg0 = new MessagePing();
      st = msg0->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg0);
      }
      break;

    case mPublish:
    case mDeliver:
      msg1 = new MessageData();
      st = msg1->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg1);
      }
      break;

    case mMetadata:
      msg2 = new MessageMetadata();
      st = msg2->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg2);
      }
      break;

    case mDataAck:
      msg3 = new MessageDataAck();
      st = msg3->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg3);
      }
      break;

    case mGap:
      msg4 = new MessageGap();
      st = msg4->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg4);
      }
      break;

    default:
      break;
  }
  return nullptr;
}

std::unique_ptr<Message> Message::CreateNewInstance(std::unique_ptr<char[]> in,
                                                    size_t size) {
  Slice slice(in.get(), size);
  std::unique_ptr<Message> msg = Message::CreateNewInstance(&slice);
  if (msg) {
    msg->buffer_ = std::move(in);
  }
  return std::move(msg);
}

void Message::SerializeToString(std::string* out) {
  Serialize();  // serialize into local buffer
  out->assign(std::move(serialize_buffer__));
}

/*
 * Fills the first bytes serialize_buffer__ with the messageHeader attributes
 */
void Message::serializeMessageHeader() const {
  if (serialize_buffer__.size() < MessageHeader::GetSize()) {
    serialize_buffer__.resize(MessageHeader::GetSize());
  }
  serialize_buffer__[0] = msghdr_.version_;
  serializeMessageSize();
}

/*
 * Inserts the message size in serialize_buffer__ at the appropriate position
 */
void Message::serializeMessageSize() const {
  msghdr_.msgsize_ = serialize_buffer__.size();
  serializeMessageSize(msghdr_.msgsize_);
}

void Message::serializeMessageSize(int msgsize) const {
  if (serialize_buffer__.size() < MessageHeader::GetSize()) {
    serialize_buffer__.resize(MessageHeader::GetSize());
  }
  std::string mlength;
  PutFixed32(&mlength, msgsize);
  assert(mlength.size() == sizeof(msghdr_.msgsize_));
  serialize_buffer__.replace(1, sizeof(msghdr_.msgsize_), mlength);
}

/*
 *  Extracts message header from first few bytes of in and returns
 *  Status::OK() on success
 */
Status Message::deserializeMessageHeader(Slice* in){
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
  return Status::OK();
}

Slice MessagePing::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serializeMessageHeader();

  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);

  // serialize message specific contents
  PutFixedEnum8(&serialize_buffer__, pingtype_);
  //  origin
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_));

  // compute the size of this message
  serializeMessageSize();
  return Slice(serialize_buffer__);
}

Status MessagePing::DeSerialize(Slice* in) {
  Status msghdrStatus = deserializeMessageHeader(in);
  if (!msghdrStatus.ok()) {
    return msghdrStatus;
  }

  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract ping type
  if (!GetFixedEnum8(in, &pingtype_)) {
    return Status::InvalidArgument("Bad ping type");
  }

  // extract origin
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  origin_.clear();
  origin_.append(sl.data(), sl.size());
  return Status::OK();
}

thread_local GUIDGenerator msgid_generator;

MessageData::MessageData(MessageType type,
                         TenantID tenantID,
                         const ClientID& origin,
                         const Slice& topic_name,
                         const NamespaceID namespace_id,
                         const Slice& payload,
                         Retention retention):
  Message(type, tenantID, origin),
  topic_name_(topic_name),
  payload_(payload),
  retention_(retention),
  namespaceid_(namespace_id) {
  assert(type == mPublish || type == mDeliver);
  seqno_ = 0;
  msgid_ = msgid_generator.Generate();
}

MessageData::MessageData(MessageType type):
  MessageData(type, Tenant::InvalidTenant, "", Slice(),
              Namespace::InvalidNamespace, Slice()) {
}

MessageData::MessageData():
  MessageData(mPublish) {
}

MessageData::~MessageData() {
}

Slice MessageData::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serializeMessageHeader();

  PutFixedEnum8(&serialize_buffer__, type_);

  // origin
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_));

  // seqno
  PutVarint64(&serialize_buffer__, seqno_);

  // The rest of the message is what goes into log storage.
  SerializeInternal();

  // compute the size of this message
  serializeMessageSize();
  return Slice(serialize_buffer__);
}

Status MessageData::DeSerialize(Slice* in) {
  Status msghdrStatus = deserializeMessageHeader(in);
  if (!msghdrStatus.ok()) {
    return msghdrStatus;
  }

  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extract origin
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  origin_.clear();
  origin_.append(sl.data(), sl.size());

  // extract sequence number of message
  if (!GetVarint64(in, &seqno_)) {
    return Status::InvalidArgument("Bad Sequence Number");
  }

  // The rest of the message is what goes into log storage.
  storage_slice_ = *in;
  return DeSerializeStorage(in);
}

Slice MessageData::GetStorageSlice() const {
  // Returns a Slice starting from tenant_id of the message.
  // storage_slice_ is constructed during deserialization.
  assert(storage_slice_.size() != 0);
  return storage_slice_;
}

void MessageData::SerializeInternal() const {
  PutFixed16(&serialize_buffer__, tenantid_);
  PutLengthPrefixedSlice(&serialize_buffer__, topic_name_);
  // miscellaneous flags
  uint16_t flags = 0;
  switch (retention_) {
    case Retention::OneHour: flags |= 0x0; break;
    case Retention::OneDay: flags |= 0x1; break;
    case Retention::OneWeek: flags |= 0x2; break;
  }
  PutFixed16(&serialize_buffer__, flags);
  PutFixed16(&serialize_buffer__, namespaceid_);

  PutLengthPrefixedSlice(&serialize_buffer__,
                         Slice((const char*)&msgid_, sizeof(msgid_)));

  PutLengthPrefixedSlice(&serialize_buffer__, payload_);
}

Status MessageData::DeSerializeStorage(Slice* in) {
  // extract tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract message topic
  if (!GetLengthPrefixedSlice(in, &topic_name_)) {
    return Status::InvalidArgument("Bad Message Topic name");
  }

  // miscellaneous flags
  uint16_t flags;
  if (!GetFixed16(in, &flags)) {
    return Status::InvalidArgument("Bad flags");
  }
  switch (flags & 0x3) {
    case 0x0: retention_ = Retention::OneHour; break;
    case 0x1: retention_ = Retention::OneDay; break;
    case 0x2: retention_ = Retention::OneWeek; break;
    default:
      return Status::InvalidArgument("Bad flags");
  }
  // namespace id
  if (!GetFixed16(in, &namespaceid_)) {
    return Status::InvalidArgument("Bad namespace id");
  }

  // extract message id
  Slice idSlice;
  if (!GetLengthPrefixedSlice(in, &idSlice) ||
      idSlice.size() < sizeof(msgid_)) {
    return Status::InvalidArgument("Bad Message Id");
  }
  memcpy(&msgid_, idSlice.data(), sizeof(msgid_));

  // extract payload (the rest of the message)
  if (!GetLengthPrefixedSlice(in, &payload_)) {
    return Status::InvalidArgument("Bad payload");
  }
  return Status::OK();
}


MessageMetadata::MessageMetadata(TenantID tenantID,
  const MetaType metatype,
  const ClientID& origin,
  const std::vector<TopicPair>& topics):
  metatype_(metatype),
  topics_(topics) {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mMetadata;
  tenantid_ = tenantID;
  origin_ = origin;
}

MessageMetadata::MessageMetadata() {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mMetadata;
  tenantid_ = Tenant::InvalidTenant;
  metatype_ = MetaType::NotInitialized;
}

MessageMetadata::~MessageMetadata() {
}

Slice MessageMetadata::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serializeMessageHeader();

  // Type, tenantId and origin
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_));

  // Now serialize message specific data
  PutFixedEnum8(&serialize_buffer__, metatype_);
  //  origin

  // Topics and metadata state
  PutVarint32(&serialize_buffer__, topics_.size());
  for (TopicPair p : topics_) {
    PutVarint64(&serialize_buffer__, p.seqno);
    PutLengthPrefixedSlice(&serialize_buffer__, Slice(p.topic_name));
    PutFixed16(&serialize_buffer__, p.namespace_id);
    PutFixedEnum8(&serialize_buffer__, p.topic_type);
  }
  // compute msg size
  serializeMessageSize();
  return Slice(serialize_buffer__);
}

Status MessageMetadata::DeSerialize(Slice* in) {
  Status msghdrStatus = deserializeMessageHeader(in);
  if (!msghdrStatus.ok()) {
    return msghdrStatus;
  }

  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extrant tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract host id
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  origin_.clear();
  origin_.append(sl.data(), sl.size());

  // extract metadata type
  if (!GetFixedEnum8(in, &metatype_)) {
    return Status::InvalidArgument("Bad metadata type");
  }

  // extract number of topics
  uint32_t num_topics;
  if (!GetVarint32(in, &num_topics)) {
    return Status::InvalidArgument("Bad Number Of Topics");
  }

  // extract each topic
  for (uint32_t i = 0; i < num_topics; i++) {
    TopicPair p;

    // extract start seqno for this topic subscription
    if (!GetVarint64(in, &p.seqno)) {
      return Status::InvalidArgument("Bad Message Payload: seqno");
    }

    // extract one topic name
    if (!GetLengthPrefixedSlice(in, &sl)) {
      return Status::InvalidArgument("Bad Message Payload");
    }
    p.topic_name.append(sl.data(), sl.size());

    // extract namespaceid
    if (!GetFixed16(in, &p.namespace_id)) {
      return Status::InvalidArgument("Bad Namespace id");
    }

    // extract one topic type
    if (!GetFixedEnum8(in, &p.topic_type)) {
      return Status::InvalidArgument("Bad topic type");
    }

    topics_.push_back(p);
  }
  return Status::OK();
}

MessageDataAck::MessageDataAck(TenantID tenantID,
                               const ClientID& origin,
                               AckVector acks)
: acks_(std::move(acks)) {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mDataAck;
  tenantid_ = tenantID;
  origin_ = origin;
}

MessageDataAck::~MessageDataAck() {
}

const MessageDataAck::AckVector& MessageDataAck::GetAcks() const {
  return acks_;
}

Slice MessageDataAck::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serializeMessageHeader();

  // Type, tenantId and origin
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_));

  // serialize message specific contents
  PutVarint32(&serialize_buffer__, acks_.size());
  for (const Ack& ack : acks_) {
    PutFixedEnum8(&serialize_buffer__, ack.status);
    PutBytes(&serialize_buffer__, ack.msgid.id, sizeof(ack.msgid.id));
    PutVarint64(&serialize_buffer__, ack.seqno);
  }

  // compute the size of this message
  serializeMessageSize();
  return Slice(serialize_buffer__);
}

Status MessageDataAck::DeSerialize(Slice* in) {
  // extract message header from in
  Status msghdrStatus = deserializeMessageHeader(in);
  if (!msghdrStatus.ok()) {
    return msghdrStatus;
  }

  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extrant tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract host id
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  origin_.clear();
  origin_.append(sl.data(), sl.size());

  // extract number of acks
  uint32_t num_acks;
  if (!GetVarint32(in, &num_acks)) {
    return Status::InvalidArgument("Bad Number Of Acks");
  }

  // extract each ack
  for (unsigned i = 0; i < num_acks; i++) {
    Ack ack;

    // extract status
    if (!GetFixedEnum8(in, &ack.status)) {
      return Status::InvalidArgument("Bad Ack Status");
    }

    // extract msgid
    if (!GetBytes(in, ack.msgid.id, sizeof(ack.msgid.id))) {
      return Status::InvalidArgument("Bad Ack MsgId");
    }

    if (!GetVarint64(in, &ack.seqno)) {
      return Status::InvalidArgument("Bad Ack Sequence number");
    }

    acks_.push_back(ack);
  }

  return Status::OK();
}

MessageGap::MessageGap(TenantID tenantID,
                      const ClientID& origin,
                      GapType gap_type,
                      SequenceNumber gap_from,
                      SequenceNumber gap_to)
: gap_type_(gap_type)
, gap_from_(gap_from)
, gap_to_(gap_to) {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mGap;
  tenantid_ = tenantID;
  origin_ = origin;
}

MessageGap::~MessageGap() {
}

Slice MessageGap::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serializeMessageHeader();

  // Type, tenantId and origin
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_));

  // Write the gap information.
  PutFixedEnum8(&serialize_buffer__, gap_type_);
  PutVarint64(&serialize_buffer__, gap_from_);
  PutVarint64(&serialize_buffer__, gap_to_);

  // compute the size of this message
  serializeMessageSize();
  return Slice(serialize_buffer__);
}

Status MessageGap::DeSerialize(Slice* in) {
  Status msghdrStatus = deserializeMessageHeader(in);
  if (!msghdrStatus.ok()) {
    return msghdrStatus;
  }
  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extrant tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract host id
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  origin_.clear();
  origin_.append(sl.data(), sl.size());

  // Read gap type
  if (!GetFixedEnum8(in, &gap_type_)) {
    return Status::InvalidArgument("Missing gap type");
  }

  // Read gap start seqno
  if (!GetVarint64(in, &gap_from_)) {
    return Status::InvalidArgument("Bad gap log ID");
  }

  // Read gap end seqno
  if (!GetVarint64(in, &gap_to_)) {
    return Status::InvalidArgument("Bad gap log ID");
  }
  return Status::OK();
}


}  // namespace rocketspeed
