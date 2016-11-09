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

const char* const kMessageTypeNames[size_t(MessageType::max) + 1] = {
  "invalid",
  "ping",
  "publish",
  "metadata (DEPRECATED)",
  "data_ack",
  "gap",
  "deliver",
  "goodbye",
  "subscribe",
  "unsubscribe",
  "deliver_gap",
  "deliver_data",
  "find_tail_seqno",
  "tail_seqno",
  "deliver_batch",
  "heartbeat",
};

MessageType Message::ReadMessageType(Slice slice) {
  MessageType mtype;
  if (slice.size() < sizeof(mtype)) {
    return MessageType::NotInitialized;
  }
  memcpy(&mtype, slice.data(), sizeof(mtype));
  return mtype;
}

 /**
  * Creates a Message of the appropriate subtype by looking at the
  * MessageType. Returns nullptr on error. It is the responsibility
  * of the caller to own this memory object.
  **/
std::unique_ptr<Message>
Message::CreateNewInstance(Slice* in) {
  MessageType mtype = ReadMessageType(*in);
  if (mtype == MessageType::NotInitialized) {
    return nullptr;
  }

  Status st;
  switch (mtype) {
    case MessageType::mPing: {
      std::unique_ptr<MessagePing> msg(new MessagePing());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mPublish:
    case MessageType::mDeliver: {
      std::unique_ptr<MessageData> msg(new MessageData());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mDataAck: {
      std::unique_ptr<MessageDataAck> msg(new MessageDataAck());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mGap: {
      std::unique_ptr<MessageGap> msg(new MessageGap());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mGoodbye: {
      std::unique_ptr<MessageGoodbye> msg(new MessageGoodbye());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mSubscribe: {
      std::unique_ptr<MessageSubscribe> msg(new MessageSubscribe());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mUnsubscribe: {
      std::unique_ptr<MessageUnsubscribe> msg(new MessageUnsubscribe());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mDeliverGap: {
      std::unique_ptr<MessageDeliverGap> msg(new MessageDeliverGap());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mDeliverData: {
      std::unique_ptr<MessageDeliverData> msg(new MessageDeliverData());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mFindTailSeqno: {
      std::unique_ptr<MessageFindTailSeqno> msg(new MessageFindTailSeqno());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mTailSeqno: {
      std::unique_ptr<MessageTailSeqno> msg(new MessageTailSeqno());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mDeliverBatch: {
      std::unique_ptr<MessageDeliverBatch> msg(new MessageDeliverBatch());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    case MessageType::mHeartbeat: {
      std::unique_ptr<MessageHeartbeat> msg(new MessageHeartbeat());
      st = msg->DeSerialize(in);
      if (st.ok()) {
        return std::unique_ptr<Message>(msg.release());
      }
      break;
    }

    default:
      break;
  }
  return nullptr;
}

std::unique_ptr<Message> Message::CreateNewInstance(Slice in) {
  return CreateNewInstance(&in);
}

std::unique_ptr<Message> Message::Copy(const Message& msg) {
  // Not efficient, but not used often, and should be efficient once we
  // have shared serialized buffers.
  std::string serial;
  msg.SerializeToString(&serial);
  Slice slice(serial);
  return CreateNewInstance(&slice);
}

Status Message::Serialize(std::string* out) const {
  PutFixedEnum8(out, type_);
  PutFixed16(out, tenantid_);
  return Status::OK();
}

Status Message::DeSerialize(Slice* in) {
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad MessageType");
  }
  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad TenantID");
  }
  return Status::OK();
}

void Message::SerializeToString(std::string* out) const {
  Serialize(out);
}

Status MessagePing::Serialize(std::string* out) const {
  PutFixedEnum8(out, type_);
  PutFixed16(out, tenantid_);

  // serialize message specific contents
  // pingtype
  PutFixedEnum8(out, pingtype_);
  // cookie
  PutLengthPrefixedSlice(out, Slice(cookie_));
  return Status::OK();
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
                         Topic topic_name,
                         NamespaceID namespace_id,
                         std::string payload) :
  Message(type, tenantID),
  topic_name_(std::move(topic_name)),
  payload_(std::move(payload)),
  namespaceid_(std::move(namespace_id)) {
  RS_ASSERT(type == MessageType::mPublish || type == MessageType::mDeliver);
  seqno_ = 0;
  seqno_prev_ = 0;
}

MessageData::MessageData(MessageType type):
  MessageData(type, Tenant::InvalidTenant, "",
              InvalidNamespace, "") {
}

MessageData::MessageData():
  MessageData(MessageType::mPublish) {
}

MessageData::~MessageData() {
}

Status MessageData::Serialize(std::string* out) const {
  PutFixedEnum8(out, type_);

  // seqno
  PutVarint64(out, seqno_prev_);
  PutVarint64(out, seqno_);

  // The rest of the message is what goes into log storage.
  SerializeInternal(out);
  return Status::OK();
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
  return DeSerializeStorage(in);
}

std::string MessageData::GetStorage() const {
  std::string storage;
  SerializeInternal(&storage);
  return storage;
}

size_t MessageData::GetTotalSize() const {
  return sizeof(MessageData) + topic_name_.size() +
         payload_.size() + namespaceid_.size() + payload_.size();
}

void MessageData::SerializeInternal(std::string* out) const {
  PutFixed16(out, tenantid_);
  PutTopicID(out, namespaceid_, topic_name_);
  PutLengthPrefixedSlice(out,
                         Slice((const char*)&msgid_, sizeof(msgid_)));

  PutLengthPrefixedSlice(out, payload_);
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

MessageDataAck::MessageDataAck(TenantID tenantID,
                               AckVector acks)
: acks_(std::move(acks)) {
  type_ = MessageType::mDataAck;
  tenantid_ = tenantID;
}

MessageDataAck::~MessageDataAck() {
}

const MessageDataAck::AckVector& MessageDataAck::GetAcks() const {
  return acks_;
}

Status MessageDataAck::Serialize(std::string* out) const {
  // Type, tenantId and origin
  PutFixedEnum8(out, type_);
  PutFixed16(out, tenantid_);

  // serialize message specific contents
  PutVarint32(out, static_cast<uint32_t>(acks_.size()));
  for (const Ack& ack : acks_) {
    PutFixedEnum8(out, ack.status);
    PutBytes(out, ack.msgid.id, sizeof(ack.msgid.id));
    PutVarint64(out, ack.seqno);
  }
  return Status::OK();
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
                       NamespaceID namespace_id,
                       Topic topic_name,
                       GapType gap_type,
                       SequenceNumber gap_from,
                       SequenceNumber gap_to)
: namespace_id_(std::move(namespace_id))
, topic_name_(std::move(topic_name))
, gap_type_(gap_type)
, gap_from_(gap_from)
, gap_to_(gap_to) {
  type_ = MessageType::mGap;
  tenantid_ = tenantID;
}

MessageGap::~MessageGap() {
}

Status MessageGap::Serialize(std::string* out) const {
  // Type, tenantId and origin
  PutFixedEnum8(out, type_);
  PutFixed16(out, tenantid_);

  // Write the gap information.
  PutTopicID(out, Slice(namespace_id_), Slice(topic_name_));
  PutFixedEnum8(out, gap_type_);
  PutVarint64(out, gap_from_);
  PutVarint64(out, gap_to_);
  return Status::OK();
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
  type_ = MessageType::mGoodbye;
  tenantid_ = tenant_id;
}

Status MessageGoodbye::Serialize(std::string* out) const {
  // Type, tenantId and origin
  PutFixedEnum8(out, type_);
  PutFixed16(out, tenantid_);

  // MessageGoodbye specifics
  PutFixedEnum8(out, code_);
  PutFixedEnum8(out, origin_type_);
  return Status::OK();
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

Status MessageFindTailSeqno::Serialize(std::string* out) const {
  Message::Serialize(out);
  PutTopicID(out, namespace_id_, topic_name_);
  return Status::OK();
}

Status MessageFindTailSeqno::DeSerialize(Slice* in) {
  Status st = Message::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  if (!GetTopicID(in, &namespace_id_, &topic_name_)) {
    return Status::InvalidArgument("Bad NamespaceID and/or TopicName");
  }
  return Status::OK();
}

Status MessageTailSeqno::Serialize(std::string* out) const {
  Message::Serialize(out);
  PutTopicID(out, namespace_id_, topic_name_);
  PutVarint64(out, seqno_);
  return Status::OK();
}

Status MessageTailSeqno::DeSerialize(Slice* in) {
  Status st = Message::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  if (!GetTopicID(in, &namespace_id_, &topic_name_)) {
    return Status::InvalidArgument("Bad NamespaceID and/or TopicName");
  }
  if (!GetVarint64(in, &seqno_)) {
    return Status::InvalidArgument("Bad sequence number");
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
Status MessageSubscribe::Serialize(std::string* out) const {
  Message::Serialize(out);
  PutTopicID(out, namespace_id_, topic_name_);
  PutVarint64(out, start_seqno_);
  EncodeSubscriptionID(out, sub_id_);
  return Status::OK();
}

Status MessageSubscribe::DeSerialize(Slice* in) {
  Status st = Message::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  if (!GetTopicID(in, &namespace_id_, &topic_name_)) {
    return Status::InvalidArgument("Bad NamespaceID and/or TopicName");
  }
  if (!GetVarint64(in, &start_seqno_)) {
    return Status::InvalidArgument("Bad SequenceNumber");
  }
  if (!DecodeSubscriptionID(in, &sub_id_)) {
    return Status::InvalidArgument("Bad SubscriptionID");
  }
  return Status::OK();
}

Status MessageUnsubscribe::Serialize(std::string* out) const {
  Message::Serialize(out);
  EncodeSubscriptionID(out, sub_id_);
  PutFixedEnum8(out, reason_);
  return Status::OK();
}

Status MessageUnsubscribe::DeSerialize(Slice* in) {
  Status st = Message::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  if (!DecodeSubscriptionID(in, &sub_id_)) {
    return Status::InvalidArgument("Bad SubscriptionID");
  }
  if (!GetFixedEnum8(in, &reason_)) {
    return Status::InvalidArgument("Bad Reason");
  }
  return Status::OK();
}

Status MessageDeliver::Serialize(std::string* out) const {
  Message::Serialize(out);
  EncodeSubscriptionID(out, sub_id_);
  PutVarint64(out, seqno_prev_);
  RS_ASSERT(seqno_ >= seqno_prev_);
  uint64_t seqno_diff = seqno_ - seqno_prev_;
  PutVarint64(out, seqno_diff);
  return Status::OK();
}

Status MessageDeliver::DeSerialize(Slice* in) {
  Status st = Message::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  if (!DecodeSubscriptionID(in, &sub_id_)) {
    return Status::InvalidArgument("Bad SubscriptionID");
  }
  if (!GetVarint64(in, &seqno_prev_)) {
    return Status::InvalidArgument("Bad previous SequenceNumber");
  }
  uint64_t seqno_diff;
  if (!GetVarint64(in, &seqno_diff)) {
    return Status::InvalidArgument("Bad difference between SequenceNumbers");
  }
  seqno_ = seqno_prev_ + seqno_diff;
  RS_ASSERT(seqno_ >= seqno_prev_);
  return Status::OK();
}

Status MessageDeliverGap::Serialize(std::string* out) const {
  MessageDeliver::Serialize(out);
  PutFixedEnum8(out, gap_type_);
  return Status::OK();
}

Status MessageDeliverGap::DeSerialize(Slice* in) {
  Status st = MessageDeliver::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  if (!GetFixedEnum8(in, &gap_type_)) {
    return Status::InvalidArgument("Bad GapType");
  }
  return Status::OK();
}

Status MessageDeliverData::Serialize(std::string* out) const {
  MessageDeliver::Serialize(out);
  PutLengthPrefixedSlice(out,
                         Slice((const char*)&message_id_, sizeof(message_id_)));
  PutLengthPrefixedSlice(out, payload_);
  return Status::OK();
}

Status MessageDeliverData::DeSerialize(Slice* in) {
  Status st = MessageDeliver::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  Slice id_slice;
  if (!GetLengthPrefixedSlice(in, &id_slice) ||
      id_slice.size() < sizeof(message_id_)) {
    return Status::InvalidArgument("Bad Message ID");
  }
  memcpy(&message_id_, id_slice.data(), sizeof(message_id_));
  if (!GetLengthPrefixedSlice(in, &payload_)) {
    return Status::InvalidArgument("Bad payload");
  }
  return Status::OK();
}

Status MessageDeliverBatch::Serialize(std::string* out) const {
  Message::Serialize(out);
  PutVarint64(out, messages_.size());
  for (const auto& msg : messages_) {
    Status st = msg->Serialize(out);
    if (!st.ok()) {
      return Status::InvalidArgument("Bad MessageDeliverData");
    }
  }
  return Status::OK();
}

Status MessageDeliverBatch::DeSerialize(Slice* in) {
  Status st = Message::DeSerialize(in);
  if (!st.ok()) {
    return st;
  }
  uint64_t len;
  if (!GetVarint64(in, &len)) {
    return Status::InvalidArgument("Bad Messages count");
  }
  messages_.clear();
  messages_.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    messages_.emplace_back(new MessageDeliverData());
    st = messages_.back()->DeSerialize(in);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

Status MessageHeartbeat::Serialize(std::string* out) const {
  using namespace std::chrono;
  PutFixedEnum8(out, type_);
  PutFixed16(out, tenantid_);

  uint64_t epoch_ms = duration_cast<milliseconds>(
    timestamp_.time_since_epoch())
    .count();
  PutFixed64(out, epoch_ms);

  // Check that heartbeats are strictly sorted.
  RS_ASSERT_DBG(std::is_sorted(
      healthy_streams_.begin(),
      healthy_streams_.end(),
      std::less_equal<uint32_t>()));
  for (uint32_t shard : healthy_streams_) {
    PutVarint32(out, shard);
  }

  return Status::OK();
}

Status MessageHeartbeat::DeSerialize(Slice* in) {
  // extract type
  if (!GetFixedEnum8(in, &type_)) {
    return Status::InvalidArgument("Bad type");
  }

  if (!GetFixed16(in, &tenantid_)) {
    return Status::InvalidArgument("Bad tenant ID");
  }

  if (in->size() == 0) {
    return Status::OK();        // for backwards compatibility
  }

  uint64_t epoch_ms;
  if (!GetFixed64(in, &epoch_ms)) {
    return Status::InvalidArgument("Bad timestamp");
  }
  Clock::duration since_epoch = std::chrono::milliseconds(epoch_ms);
  timestamp_ = Clock::time_point(since_epoch);

  uint32_t shard;
  while (GetVarint32(in, &shard)) {
    healthy_streams_.push_back(shard);
  }

  return Status::OK();
}

}  // namespace rocketspeed
