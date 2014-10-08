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
#include "src/util/xxhash.h"

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
  MessageType mtype;

  // make a temporary copy of the input slice
  Slice tmp(*in);
  assert(in->size() >= MessageHeader::GetSize());

  // remove msg header
  tmp.remove_prefix(MessageHeader::GetSize());

  // extract msg type
  memcpy(&mtype, tmp.data(), sizeof(mtype));

  switch (mtype) {
    case mPing:
      msg0 = new MessagePing();
      msg0->DeSerialize(in);
      return std::unique_ptr<Message>(msg0);
    case mData:
      msg1 = new MessageData();
      msg1->DeSerialize(in);
      return std::unique_ptr<Message>(msg1);
    case mMetadata:
      msg2 = new MessageMetadata();
      msg2->DeSerialize(in);
      return std::unique_ptr<Message>(msg2);
    case mDataAck:
      msg3 = new MessageDataAck();
      msg3->DeSerialize(in);
      return std::unique_ptr<Message>(msg3);
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

Slice MessagePing::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serialize_buffer__.append((const char *)&msghdr_.version_,
                            sizeof(msghdr_.version_));
  PutFixed32(&serialize_buffer__, msghdr_.msgsize_);
  serialize_buffer__.append((const char *)&type_, sizeof(type_));
  PutFixed16(&serialize_buffer__, tenantid_);

  // serialize message specific contents
  serialize_buffer__.append((const char *)&pingtype_, sizeof(type_));
  //  origin
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_.hostname));
  PutVarint64(&serialize_buffer__, origin_.port);

  // compute the size of this message
  msghdr_.msgsize_ = serialize_buffer__.size();
  std::string mlength;
  PutFixed32(&mlength, msghdr_.msgsize_);
  assert(mlength.size() == sizeof(msghdr_.msgsize_));

  // Update the 4byte-msg size starting from the 2nd byte
  serialize_buffer__.replace(1, sizeof(msghdr_.msgsize_), mlength);
  return Slice(serialize_buffer__);
}

Status MessagePing::DeSerialize(Slice* in) {
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

  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract ping type
  void* p = static_cast<void *>(&pingtype_);
  memcpy(p, in->data(), sizeof(pingtype_));
  in->remove_prefix(sizeof(pingtype_));

  // extract origin
  HostId* host = static_cast<HostId*>(&origin_);
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  host->hostname.clear();
  host->hostname.append(sl.data(), sl.size());

  // extract port number
  if (!GetVarint64(in, &host->port)) {
    return Status::InvalidArgument("Bad Port Number");
  }
  return Status::OK();
}

static std::atomic<uint32_t> msgid_seed(static_cast<uint32_t>(time(NULL)));
static std::atomic<uint32_t> msgid_counter(0);

MessageData::MessageData(TenantID tenantID,
                         const HostId& origin,
                         const Slice& topic_name,
                         const NamespaceID namespace_id,
                         const Slice& payload,
                         Retention retention):
  origin_(origin),
  topic_name_(topic_name),
  payload_(payload),
  retention_(retention),
  namespaceid_(namespace_id) {
  msghdr_.version_ =  ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mData;
  tenantid_ = tenantID;
  seqno_ = 0;

  // TODO(dhruba) 1 : generate better/faster GUID here
  union {
    char msgid[16];
    struct {
      uint32_t a;
      uint32_t b;
      uint32_t c;
      uint32_t d;
    };
  } u;

  u.a = XXH32(static_cast<const void*>(origin.hostname.c_str()),
              static_cast<int>(origin.hostname.size()), msgid_seed);
  u.b = XXH32(static_cast<const void*>(topic_name.data()),
              static_cast<int>(topic_name.size()), msgid_seed);
  u.c = XXH32(static_cast<const void*>(payload_.data()),
              static_cast<int>(payload_.size()), msgid_seed);
  u.d = msgid_counter++;
  if (u.d == uint32_t(-1)) {
    ++msgid_seed;
  }
  msgid_ = MsgId(u.msgid);
}

MessageData::MessageData():
  MessageData(Tenant::InvalidTenant, HostId(), Slice(),
              Namespace::InvalidNamespace, Slice()) {
}

MessageData::~MessageData() {
}

Slice MessageData::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serialize_buffer__.append((const char *)&msghdr_.version_,
                            sizeof(msghdr_.version_));
  PutFixed32(&serialize_buffer__, msghdr_.msgsize_);
  serialize_buffer__.append((const char *)&type_, sizeof(type_));
  PutVarint64(&serialize_buffer__, seqno_);

  // origin
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_.hostname));
  PutVarint64(&serialize_buffer__, origin_.port);

  // The rest of the message is what goes into log storage.
  SerializeInternal();

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

  // extract sequence number of message
  if (!GetVarint64(in, &seqno_)) {
    return Status::InvalidArgument("Bad Sequence Number");
  }

  // extract origin
  HostId* host = static_cast<HostId*>(&origin_);
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  host->hostname.clear();
  host->hostname.append(sl.data(), sl.size());

    // extract port number
  if (!GetVarint64(in, &origin_.port)) {
    return Status::InvalidArgument("Bad Port Number");
  }

  // The rest of the message is what goes into log storage.
  return DeSerializeStorage(in);
}

Slice MessageData::SerializeStorage() const {
  // If we are serializing as part of Serialize then don't restart buffer.
  serialize_buffer__.clear();
  SerializeInternal();
  return Slice(serialize_buffer__);
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

  serialize_buffer__.append(payload_.data(), payload_.size());
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
  payload_ = *in;
  return Status::OK();
}


MessageMetadata::MessageMetadata(TenantID tenantID,
  const MetaType metatype,
  const HostId& origin,
  const std::vector<TopicPair>& topics):
  metatype_(metatype),
  origin_(origin),
  topics_(topics) {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mMetadata;
  tenantid_ = tenantID;
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

  // version and msg size
  serialize_buffer__.append((const char *)&msghdr_.version_,
                            sizeof(msghdr_.version_));
  PutFixed32(&serialize_buffer__, msghdr_.msgsize_);

  // Type, tenantId and seqno
  serialize_buffer__.append((const char *)&type_, sizeof(type_));
  PutFixed16(&serialize_buffer__, tenantid_);

  // Now serialize message specific data
  serialize_buffer__.append((const char *)&metatype_, sizeof(metatype_));
  //  origin
  PutLengthPrefixedSlice(&serialize_buffer__, Slice(origin_.hostname));
  PutVarint64(&serialize_buffer__, origin_.port);

  // Topics and metadata state
  PutVarint32(&serialize_buffer__, topics_.size());
  for (TopicPair p : topics_) {
    PutVarint64(&serialize_buffer__, p.seqno);
    PutLengthPrefixedSlice(&serialize_buffer__, Slice(p.topic_name));
    PutFixed16(&serialize_buffer__, p.namespace_id);
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
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  // extract metadata type
  void* p = static_cast<void *>(&metatype_);
  memcpy(p, in->data(), sizeof(metatype_));
  in->remove_prefix(sizeof(metatype_));

  // extract host id
  Slice sl;
  if (!GetLengthPrefixedSlice(in, &sl)) {
    return Status::InvalidArgument("Bad HostName");
  }
  origin_.hostname.clear();
  origin_.hostname.append(sl.data(), sl.size());

  // extract port number
  if (!GetVarint64(in, &origin_.port)) {
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
    memcpy(&p.topic_type, in->data(), sizeof(p.topic_type));
    in->remove_prefix(sizeof(p.topic_type));

    topics_.push_back(p);
  }
  return Status::OK();
}

MessageDataAck::MessageDataAck(const std::vector<Ack>& acks)
: acks_(acks) {
  msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  type_ = mDataAck;
}

MessageDataAck::MessageDataAck()
: MessageDataAck(std::vector<Ack>()) {
}

MessageDataAck::~MessageDataAck() {
}

const std::vector<MessageDataAck::Ack>& MessageDataAck::GetAcks() const {
  return acks_;
}

Slice MessageDataAck::Serialize() const {
  // serialize common header
  msghdr_.msgsize_ = 0;
  serialize_buffer__.clear();
  serialize_buffer__.append((const char *)&msghdr_.version_,
                            sizeof(msghdr_.version_));
  PutFixed32(&serialize_buffer__, msghdr_.msgsize_);
  serialize_buffer__.append((const char *)&type_, sizeof(type_));

  // serialize message specific contents
  PutVarint32(&serialize_buffer__, acks_.size());
  for (const Ack& ack : acks_) {
    serialize_buffer__.append((const char*)&ack.status, sizeof(ack.status));
    serialize_buffer__.append((const char*)&ack.msgid, sizeof(ack.msgid));
  }

  // compute the size of this message
  msghdr_.msgsize_ = serialize_buffer__.size();
  std::string mlength;
  PutFixed32(&mlength, msghdr_.msgsize_);
  assert(mlength.size() == sizeof(msghdr_.msgsize_));

  // Update the 4byte-msg size starting from the 2nd byte
  serialize_buffer__.replace(1, sizeof(msghdr_.msgsize_), mlength);
  return Slice(serialize_buffer__);
}

Status MessageDataAck::DeSerialize(Slice* in) {
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

  // extract number of acks
  uint32_t num_acks;
  if (!GetVarint32(in, &num_acks)) {
    return Status::InvalidArgument("Bad Number Of Acks");
  }

  // extract each ack
  for (unsigned i = 0; i < num_acks; i++) {
    Ack ack;

    // extract status
    if (in->empty()) {
      return Status::InvalidArgument("Bad Ack Status");
    }
    ack.status = static_cast<AckStatus>((*in)[0]);
    in->remove_prefix(1);

    // extract msgid
    if (in->size() < sizeof(ack.msgid)) {
      return Status::InvalidArgument("Bad Ack MsgId");
    }
    memcpy(&ack.msgid, in->data(), sizeof(ack.msgid));
    in->remove_prefix(sizeof(ack.msgid));

    acks_.push_back(ack);
  }

  return Status::OK();
}


}  // namespace rocketspeed
