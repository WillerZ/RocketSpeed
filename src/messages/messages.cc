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
  MessageGoodbye* msg5 = nullptr;
  MessageType mtype;

  // extract msg type
  if (in->size() < sizeof(mtype)) {
    return nullptr;
  }
  memcpy(&mtype, in->data(), sizeof(mtype));

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

    case mGoodbye:
      msg5 = new MessageGoodbye();
      st = msg5->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg5);
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
  return CreateNewInstance(std::move(in), slice);
}

std::unique_ptr<Message> Message::CreateNewInstance(std::unique_ptr<char[]> in,
                                                    Slice slice) {
  std::unique_ptr<Message> msg = Message::CreateNewInstance(&slice);
  if (msg) {
    msg->buffer_ = std::move(in);
  }
  return std::move(msg);
}

void Message::SerializeToString(std::string* out) const {
  Serialize();  // serialize into local buffer
  out->assign(std::move(serialize_buffer__));
}

Slice MessagePing::Serialize() const {
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);

  // serialize message specific contents
  // pingtype
  PutFixedEnum8(&serialize_buffer__, pingtype_);
  // cookie
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(cookie_));

  return Slice(serialize_buffer__);
}

Status MessagePing::DeSerialize(Slice* in) {
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

  // extract cookie
  if (!GetLengthPrefixedSlice(in, &cookie_)) {
    return Status::InvalidArgument("Bad cookie");
  }
  return Status::OK();
}

MessageData::MessageData(MessageType type,
                         TenantID tenantID,
                         const Slice& topic_name,
                         const Slice& namespace_id,
                         const Slice& payload) :
  Message(type, tenantID),
  topic_name_(topic_name),
  payload_(payload),
  namespaceid_(namespace_id) {
  assert(type == mPublish || type == mDeliver);
  seqno_ = 0;
  seqno_prev_ = 0;
  msgid_ = GUIDGenerator::ThreadLocalGUIDGenerator()->Generate();
}

MessageData::MessageData(MessageType type):
  MessageData(type, Tenant::InvalidTenant, Slice(),
              InvalidNamespace, Slice()) {
}

MessageData::MessageData():
  MessageData(mPublish) {
}

MessageData::~MessageData() {
}

Slice MessageData::Serialize() const {
  PutFixedEnum8(&serialize_buffer__, type_);

  // seqno
  PutVarint64(&serialize_buffer__, seqno_prev_);
  PutVarint64(&serialize_buffer__, seqno_);

  // The rest of the message is what goes into log storage.
  SerializeInternal();
  return Slice(serialize_buffer__);
}

Status MessageData::DeSerialize(Slice* in) {
  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extract sequence numbers of message
  if (!GetVarint64(in, &seqno_prev_)) {
    return Status::InvalidArgument("Bad Previous Sequence Number");
  }

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
  PutTopicID(&serialize_buffer__, namespaceid_, topic_name_);
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
  if (!GetTopicID(in, &namespaceid_, &topic_name_)) {
    return Status::InvalidArgument("Bad Message Topic ID");
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
  const std::vector<TopicPair>& topics):
  metatype_(metatype),
  topics_(topics) {
  type_ = mMetadata;
  tenantid_ = tenantID;
}

MessageMetadata::MessageMetadata() {
  type_ = mMetadata;
  tenantid_ = Tenant::InvalidTenant;
  metatype_ = MetaType::NotInitialized;
}

MessageMetadata::~MessageMetadata() {
}

Slice MessageMetadata::Serialize() const {
  // Type, tenantId and origin
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);

  // Now serialize message specific data
  PutFixedEnum8(&serialize_buffer__, metatype_);
  //  origin

  // Topics and metadata state
  PutVarint32(&serialize_buffer__, static_cast<uint32_t>(topics_.size()));
  for (TopicPair p : topics_) {
    PutVarint64(&serialize_buffer__, p.seqno);
    PutTopicID(&serialize_buffer__, p.namespace_id, p.topic_name);
    PutFixedEnum8(&serialize_buffer__, p.topic_type);
  }
  return Slice(serialize_buffer__);
}

Status MessageMetadata::DeSerialize(Slice* in) {
  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extrant tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

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
    if (!GetTopicID(in, &p.namespace_id, &p.topic_name)) {
      return Status::InvalidArgument("Bad Namespace/Topic");
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
                               AckVector acks)
: acks_(std::move(acks)) {
  type_ = mDataAck;
  tenantid_ = tenantID;
}

MessageDataAck::~MessageDataAck() {
}

const MessageDataAck::AckVector& MessageDataAck::GetAcks() const {
  return acks_;
}

Slice MessageDataAck::Serialize() const {
  // Type, tenantId and origin
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);

  // serialize message specific contents
  PutVarint32(&serialize_buffer__, static_cast<uint32_t>(acks_.size()));
  for (const Ack& ack : acks_) {
    PutFixedEnum8(&serialize_buffer__, ack.status);
    PutBytes(&serialize_buffer__, ack.msgid.id, sizeof(ack.msgid.id));
    PutVarint64(&serialize_buffer__, ack.seqno);
  }

  return Slice(serialize_buffer__);
}

Status MessageDataAck::DeSerialize(Slice* in) {
  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extrant tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

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
                       Slice namespace_id,
                       Slice topic_name,
                       GapType gap_type,
                       SequenceNumber gap_from,
                       SequenceNumber gap_to)
: namespace_id_(namespace_id)
, topic_name_(topic_name)
, gap_type_(gap_type)
, gap_from_(gap_from)
, gap_to_(gap_to) {
  type_ = mGap;
  tenantid_ = tenantID;
}

MessageGap::~MessageGap() {
}

Slice MessageGap::Serialize() const {
  // Type, tenantId and origin
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);

  // Write the gap information.
  PutTopicID(&serialize_buffer__, namespace_id_, topic_name_);
  PutFixedEnum8(&serialize_buffer__, gap_type_);
  PutVarint64(&serialize_buffer__, gap_from_);
  PutVarint64(&serialize_buffer__, gap_to_);

  return Slice(serialize_buffer__);
}

Status MessageGap::DeSerialize(Slice* in) {
  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extrant tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // Read topic ID.
  if (!GetTopicID(in, &namespace_id_, &topic_name_)) {
    return Status::InvalidArgument("Invalid topic ID");
  }

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

MessageGoodbye::MessageGoodbye(TenantID tenant_id,
                               Code code,
                               OriginType origin_type)
: code_(code)
, origin_type_(origin_type) {
  type_ = mGoodbye;
  tenantid_ = tenant_id;
}

Slice MessageGoodbye::Serialize() const {
  // Type, tenantId and origin
  PutFixedEnum8(&serialize_buffer__, type_);
  PutFixed16(&serialize_buffer__, tenantid_);

  // MessageGoodbye specifics
  PutFixedEnum8(&serialize_buffer__, code_);
  PutFixedEnum8(&serialize_buffer__, origin_type_);

  return Slice(serialize_buffer__);
}

Status MessageGoodbye::DeSerialize(Slice* in) {
  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  // extrant tenant ID
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract code
  if (!GetFixedEnum8(in, &code_)) {
    return Status::InvalidArgument("Bad code");
  }

  // extract origin type
  if (!GetFixedEnum8(in, &origin_type_)) {
    return Status::InvalidArgument("Bad origin type");
  }

  return Status::OK();
}


}  // namespace rocketspeed
