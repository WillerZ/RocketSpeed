// Copyright (c) 2014, Facebook, Inc.  All rights reserved.

// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/util/common/subscription_id.h"
#include "src/messages/serializer.h"
#include "src/util/common/autovector.h"
#include "src/util/storage.h"

/*
 * This file contains all the messages used by RocketSpeed. These messages are
 * the ONLY means of communication between a client, pilot, copilot and
 * control tower. These are interal to RocketSpeed and can change from one
 * release to another. Applications should not use these messages to communicate
 * with RocketSpeed, instead applications should use the public api specified
 * in include/RocketSpeed.h to interact with RocketSpeed.
 * All messages have to implement the Serializer interface.
 */

namespace rocketspeed {

/// Current version of protocol being emitted by this client/server.
/// Receiving a later versioned message will cause connection to close with
/// an error.
constexpr uint8_t kCurrentMsgVersion = 1;

/// Minimum supported version of protocol of this client/server.
/// Anything below will cause connection to close with error.
constexpr uint8_t kMinAcceptedVersion = 1;

/** The message types. */
enum class MessageType : uint8_t {
  NotInitialized = 0,  // not initialized yet
  mPing = 0x01,        // ping data
  mPublish = 0x02,     // data publish
  // mMetadata = 0x03,       // subscription information (DEPRECATED)
  mDataAck = 0x04,         // ack for user data
  mGap = 0x05,             // gap in the log
  mDeliver = 0x06,         // data delivery
  mGoodbye = 0x07,         // MessageGoodbye
  mSubscribe = 0x08,       // MessageSubscribe
  mUnsubscribe = 0x09,     // MessageUnsubscribe
  mDeliverGap = 0x0A,      // MessageDeliverGap
  mDeliverData = 0x0B,     // MessageDeliverData
  mFindTailSeqno = 0x0C,   // MessageFindTailSeqno
  mTailSeqno = 0x0D,       // MessageTailSeqno
  mDeliverBatch = 0x0E,    // MessageDeliverBatch
  mHeartbeat = 0x0F,       // MessageHeartbeat
  mHeartbeatDelta = 0x10,  // MessageHeartbeatDelta
  mBacklogQuery = 0x11,    // MessageBacklogQuery
  mBacklogFill = 0x12,     // MessageBacklogFill
  mIntroduction = 0x13,    // MessageIntroduction

  min = mPing,
  max = mIntroduction,
};

inline bool ValidateEnum(MessageType e) {
  return e >= MessageType::min && e <= MessageType::max;
}

extern const char* const kMessageTypeNames[size_t(MessageType::max) + 1];

inline const char* MessageTypeName(MessageType type) {
  return ValidateEnum(type) ? kMessageTypeNames[size_t(type)] : "invalid";
}

/*
 * The metadata messages can be of two subtypes
 */
enum MetadataType : uint8_t {
  mNotinitialized = 0x00,          // message is not yet initialized
  mSubscribe = 0x01,               // subscribe
  mUnSubscribe = 0x02              // unsubscribe
};

inline bool ValidateEnum(MetadataType e) {
  return e >= mSubscribe && e <= mUnSubscribe;
}

inline bool ValidateEnum(GapType e) {
  return e >= kBenign && e <= kRetention;
}

inline bool ValidateEnum(HasMessageSinceResult e) {
  return e >= HasMessageSinceResult::kMin && e <= HasMessageSinceResult::kMax;
}

/*
 * A topic:bool pair to indicate whether to subscribe or unsubscribe
 * from the specified topic.
 * If topic_type == mSubscribe, then seqno indicates the starting
 * sequence number from which (inclusive) this subscriber is
 * requesting data from. A seqno of 0 indicates that the subscriber
 * is interested in receiving data starting now.
 * A seqno of 1 indicates that the subscriber is interested in
 * receiving all the data associated with this topic.
 * If topic_type == mUnSubscribe, then seqno is not defined.
 */
class TopicPair {
 public:
  SequenceNumber seqno;  // the starting sequence number for this subscription
  Topic topic_name;
  NamespaceID namespace_id;
  MetadataType topic_type;

  TopicPair(SequenceNumber s, std::string name, MetadataType type,
            NamespaceID namespaceId) :
    seqno(s),
    topic_name(std::move(name)),
    namespace_id(std::move(namespaceId)),
    topic_type(type) {
  }
  TopicPair() {}
};

/**
 * This is a superclass of all RocketSpeed messages.
 * All RocketSpeed messages have a type, tenant-id and Origin.
 * The type identifies the functionality of the message,
 * The TenantId is used to implement fair sharing of resources
 * among multiple users of the system.
 * The Origin identifies the entity that produced this message.
 */
class Message : private Serializer {
 public:
  /**
   * @return The message Tupe
   */
  MessageType GetMessageType() const { return type_; }

  /**
   * @return The tenant ID.
   */
  TenantID GetTenantID() const { return tenantid_; }

  /**
   * Reads the type of a serialised message.
   * Does not validate message itself.
   *
   * @param slice A serialised message to inspect.
   * @return The type of provided message.
   */
  static MessageType ReadMessageType(Slice slice);

  /**
   * Creates a Message of the appropriate subtype by looking at the
   * MessageType. Returns nullptr on error.  The memory ownership is passed
   * to the message itself, and will be discarded once the message is destroyed.
   */
  static std::unique_ptr<Message> CreateNewInstance(Slice in);

  /*
   * Inherited from Serializer
   */
  Status DeSerialize(Slice* in) override;
  void SerializeToString(std::string* out) const;

  /**
   * Creates a deep copy of a message.
   */
  static std::unique_ptr<Message> Copy(const Message& msg);

  template <typename TMsg>
  static std::unique_ptr<TMsg> Copy(TMsg msg) {
    return std::make_unique<TMsg>(std::move(msg));
  }

 protected:
  Message(MessageType type, TenantID tenantid) :
          type_(type), tenantid_(tenantid) {
  }

  explicit Message(MessageType type) : type_(type) {
  }

  Message() : type_(MessageType::NotInitialized) {
  }

  Status Serialize(std::string* out) const override;

  MessageType type_;                // type of this message
  TenantID tenantid_;               // unique id for tenant

 private:
  static std::unique_ptr<Message> CreateNewInstance(Slice* in);
};


/*********************************************************/
/**    Message definitions start here                  ***/
/*********************************************************/
/*
 * This is a ping message
 */
class MessagePing : public Message {
 public:
  // Two types of ping messages
  enum PingType : char {
    NotInitialized = 0x00,        // message not yet initialized
    Request = 0x01,               // ping request
    Response = 0x02               // ping response
  };

  MessagePing() : pingtype_(PingType::NotInitialized) {}

  MessagePing(TenantID tenantid, PingType pingtype, std::string cookie = "")
      : Message(MessageType::mPing, tenantid)
      , pingtype_(pingtype)
      , cookie_(std::move(cookie)) {}

  PingType GetPingType() const {
    return pingtype_;
  }

  void SetPingType(PingType type) {
    pingtype_ = type;
  }

  const std::string& GetCookie() const {
    return cookie_;
  }

  void SetCookie(std::string cookie) {
    cookie_ = std::move(cookie);
  }

  /*
   * Inherited from Serializer
   */
  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 protected:
  PingType pingtype_;
  std::string cookie_;
};

inline bool ValidateEnum(MessagePing::PingType e) {
  return e >= MessagePing::Request && e <= MessagePing::Response;
}

/*
 * This is a data message.
 * The payload is the user-data in the message.
 */
class MessageData : public Message {
 public:
  /**
   * Creates a message by specifying its contents. It is the responsibility of
   * the creator to ensure that the lifetime of the payload remains valid
   * till the lifetime of this object.
   *
   * @param topic_name name of the topic
   * @param payload The user defined message contents
   */
  MessageData(MessageType type,
              TenantID tenantID,
              Topic topic_name,
              NamespaceID namespace_id,
              std::string payload);

  /*
   * constructor with type
   */
  explicit MessageData(MessageType type);

  /*
   * default constructor
   */
  MessageData();

  /**
   * This message is not needed any more
   */
  virtual ~MessageData();

  /**
   * @return The message Sequence Number.
   */
  SequenceNumber GetSequenceNumber() const { return seqno_; }

  /**
   * @return The message previous Sequence Number. For subscriptions to seqno
   *         0, this will be 0.
   */
  SequenceNumber GetPrevSequenceNumber() const { return seqno_prev_; }

  /**
   * @Sets the Sequence Numbers.
   */
  void SetSequenceNumbers(SequenceNumber prev, SequenceNumber seqno) {
    seqno_prev_ = prev;
    seqno_ = seqno;
  }

  /**
   * @Sets the previous Sequence Number.
   */
  void SetPreviousSequenceNumber(SequenceNumber prev) {
    seqno_prev_ = prev;
  }

  /**
   * @return The Message ID.
   */
  const MsgId& GetMessageId() const { return msgid_; }

  /**
   * @Sets the Message ID
   */
  void SetMessageId(MsgId m) { msgid_ = m; }

  /**
   * @return The Topic Name
   */
  Slice GetTopicName() const { return Slice(topic_name_); }

  /**
   * @return The namespace of this topic
   */
  Slice GetNamespaceId() const { return Slice(namespaceid_); }

  /**
   * @return The Message payload
   */
  Slice GetPayload() const { return Slice(payload_); }

  /**
   * @return the slice containing tenant ID, topic_name and paylodad from
   * buffer_
   */
  std::string GetStorage() const;

  /**
   * @return Deserializes the message from the log storage format.
   *         Only the tenant ID, topic_name, and payload are deserialized.
   *         The seqno_ is not deserialized. Sequence number is
   *         stored by the log storage.
   */
  Status DeSerializeStorage(Slice* in);

  /**
   * @return an approximate size in bytes of the message
   */
  size_t GetTotalSize() const;

  /*
   * Inherited from Serializer
   */
  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  void SerializeInternal(std::string* out) const;

  // type of this message: mPublish or mDeliver
  SequenceNumber seqno_prev_; // previous sequence number on topic
  SequenceNumber seqno_;      // sequence number of message
  MsgId msgid_;               // globally unique id for message
  Topic topic_name_;          // name of topic
  std::string payload_;       // user data of message
  NamespaceID namespaceid_;   // message namespace
};

/*
 * This is a data ack message.
 * The ack is sent back to the producer after a publish.
 */
class MessageDataAck : public Message {
 public:
  /**
   * Result of the publish.
   */
  enum AckStatus : char {
    Success = 0x00,
    Failure = 0x01,
  };

  /**
   * Individual message ack.
   */
  struct Ack {
    // If the message was successfully stored in the log storage.
    AckStatus status;

    // Message ID of the message that was successfully stored.
    MsgId msgid;

    // Sequence number of the message in the log storage.
    // This can be used after a publish to read all messages from the point
    // of the published message.
    SequenceNumber seqno;

    bool operator==(const Ack& rhs) const {
      return status == rhs.status &&
             msgid == rhs.msgid &&
             seqno == rhs.seqno;
    }
  };

  /**
   * Vector of Acks, with one Ack allocated in place (this is the common case).
   */
  typedef autovector<Ack, 1> AckVector;

  /**
   * Creates a message by specifying the message acks.
   *
   * @param msgid The MsgIds of the messages that have been ack'd
   */
  explicit MessageDataAck(TenantID tenantID,
                          AckVector acks);
  /**
   * default constructor
   */
  MessageDataAck() {}

  /**
   * This message is not needed any more
   */
  virtual ~MessageDataAck();

  /**
   * @return The message IDs
   */
  const AckVector& GetAcks() const;

  /*
   * Inherited from Serializer
   */
  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  // type of this message: mDataAck
  AckVector acks_;  // message acks
};

inline bool ValidateEnum(MessageDataAck::AckStatus e) {
  return e >= MessageDataAck::Success && e <= MessageDataAck::Failure;
}

/*
 * This message indicates a gap in the logs.
 */
class MessageGap : public Message {
 public:
  /**
   * Creates a message from a GapRecord.
   *
   * @param msgid The MsgIds of the messages that have been ack'd
   */
  explicit MessageGap(TenantID tenantID,
                      NamespaceID namespace_id,
                      Topic topic_name,
                      GapType gap_type,
                      SequenceNumber gap_from,
                      SequenceNumber gap_to);
  /**
   * default constructor
   */
  MessageGap() {}

  /**
   * This message is not needed any more
   */
  virtual ~MessageGap();

  /**
   * Get the gap information.
   */
  const NamespaceID& GetNamespaceId() const {
    return namespace_id_;
  }

  const Topic& GetTopicName() const {
    return topic_name_;
  }

  GapType GetType() const {
    return gap_type_;
  }

  /**
   * @return First sequence number (inclusive) of the gap. For subscriptions to
   *         0, this will be 0.
   */
  SequenceNumber GetStartSequenceNumber() const {
    return gap_from_;
  }

  /**
   * @return Last sequence number (inclusive) of the gap.
   */
  SequenceNumber GetEndSequenceNumber() const {
    return gap_to_;
  }
  /*
   * Inherited from Serializer
   */
  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  // type of this message: mGap
  NamespaceID namespace_id_;
  Topic topic_name_;
  GapType gap_type_;
  SequenceNumber gap_from_;
  SequenceNumber gap_to_;
};

/**
 * This message indicating a client will no longer accept messages.
 * These will be generated automatically when a socket disconnects, but can
 * also be sent explicitly by clients if they no longer wish to communicate.
 */
class MessageGoodbye : public Message {
 public:
  /**
   * Extra context for the goodbye message, i.e. what caused the goodbye.
   */
  enum Code : char {
    SocketError = 0x00,
    Graceful = 0x01,
  };

  /**
   * Type of the origin sending the message. From the perspective of the
   * recipient, a Client origin is a node that has initiated communication with
   * the recipient. A Server origin is a node that we initiated communication
   * with.
   *
   * If a goodbye message is received from a Client, then all state for that
   * client should be discarded. If a goodbye message is received from a Server,
   * then it should be assumed that the server has lost all state about this
   * client.
   */
  enum OriginType : char {
    Client = 0x00,
    Server = 0x01,
  };

  /**
   * Creates a goodbye message.
   *
   * @param tenant_id The tenant ID of the origin.
   * @param code The error code.
   */
  explicit MessageGoodbye(TenantID tenant_id,
                          Code code,
                          OriginType origin_type);

  MessageGoodbye() {}
  virtual ~MessageGoodbye() {}

  Code GetCode() const {
    return code_;
  }

  OriginType GetOriginType() const {
    return origin_type_;
  }

  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  // type of this message: mGoodbye
  Code code_;
  OriginType origin_type_;
};

inline bool ValidateEnum(MessageGoodbye::Code e) {
  return e >= MessageGoodbye::SocketError && e <= MessageGoodbye::Graceful;
}

inline bool ValidateEnum(MessageGoodbye::OriginType e) {
  return e >= MessageGoodbye::Client && e <= MessageGoodbye::Server;
}

/**
 * A request for the tail sequence number.
 * Servers respond by sending a TailSeqno message.
 * The tail sequence number is computed with best effort.
 */
class MessageFindTailSeqno final : public Message {
 public:
  MessageFindTailSeqno(TenantID tenant_id,
                       NamespaceID namespace_id,
                       Topic topic_name)
      : Message(MessageType::mFindTailSeqno, tenant_id)
      , namespace_id_(std::move(namespace_id))
      , topic_name_(std::move(topic_name)) {}

  MessageFindTailSeqno() : Message(MessageType::mFindTailSeqno) {}

  const NamespaceID& GetNamespace() const { return namespace_id_; }

  const Topic& GetTopicName() const { return topic_name_; }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  NamespaceID namespace_id_;
  Topic topic_name_;
};

/**
 * Best effort estimate of tail sequence number on a topic.
 * If TailSeqno reports sequence number N, then it is guaranteed that a
 * subsequent publish will be to a sequence number >= N. There is no
 * guaranteed lower bound on N.
 */
class MessageTailSeqno final : public Message {
 public:
  MessageTailSeqno(TenantID tenant_id,
                   NamespaceID namespace_id,
                   Topic topic_name,
                   SequenceNumber seqno)
      : Message(MessageType::mTailSeqno, tenant_id)
      , namespace_id_(std::move(namespace_id))
      , topic_name_(std::move(topic_name))
      , seqno_(seqno) {}

  MessageTailSeqno() : Message(MessageType::mTailSeqno) {}

  const NamespaceID& GetNamespace() const { return namespace_id_; }

  const Topic& GetTopicName() const { return topic_name_; }

  SequenceNumber GetSequenceNumber() const { return seqno_; }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  NamespaceID namespace_id_;
  Topic topic_name_;
  SequenceNumber seqno_;
};

/**
 * Messages exchanged on a subscription.
 * @{
 */

/** A request to subscribe to provided topic with given parameters. */
class MessageSubscribe final : public Message {
 public:
  MessageSubscribe(TenantID tenant_id,
                   NamespaceID namespace_id,
                   Topic topic_name,
                   SequenceNumber start_seqno,
                   SubscriptionID sub_id)
      : Message(MessageType::mSubscribe, tenant_id),
        namespace_id_(std::move(namespace_id)),
        topic_name_(std::move(topic_name)),
        start_seqno_(start_seqno),
        sub_id_(sub_id) {}

  MessageSubscribe() : Message(MessageType::mSubscribe) {}

  Slice GetNamespace() const { return Slice(namespace_id_); }

  Slice GetTopicName() const { return Slice(topic_name_); }

  SequenceNumber GetStartSequenceNumber() const { return start_seqno_; }

  SubscriptionID GetSubID() const { return sub_id_; }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  /** Parameters of the subscription. */
  NamespaceID namespace_id_;
  Topic topic_name_;
  SequenceNumber start_seqno_;
  /** ID of the requested subscription assigned by the subscriber. */
  SubscriptionID sub_id_;
};

/**
 * A request or response which notifies that subscription was terminated by
 * either side.
 */
class MessageUnsubscribe final : public Message {
 public:
  enum class Reason : uint8_t {
    /** Unsubscription was requested by the subscriber. */
    kRequested = 0x00,
    /**
     * Subscriber shouldn't be subscribing to this subscribee with such
     * subscription parameters.
     */
    kInvalid = 0x02,
  };

  MessageUnsubscribe(TenantID tenant_id,
                     NamespaceID namespace_id,
                     Topic topic_name,
                     SubscriptionID sub_id, Reason reason)
      : Message(MessageType::mUnsubscribe, tenant_id),
        namespace_id_(std::move(namespace_id)),
        topic_name_(std::move(topic_name)),
        sub_id_(sub_id),
        reason_(reason) {}

  MessageUnsubscribe() : Message(MessageType::mUnsubscribe) {}

  SubscriptionID GetSubID() const { return sub_id_; }

  Slice GetNamespace() const { return namespace_id_; }

  Slice GetTopicName() const { return topic_name_; }

  void SetSubID(SubscriptionID sub_id) { sub_id_ = sub_id; }

  Reason GetReason() const { return reason_; }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  NamespaceID namespace_id_;
  Topic topic_name_;
  /** ID of the subscription this response refers to. */
  SubscriptionID sub_id_;
  /** A feedback to the other party, why this subscription was terminated. */
  Reason reason_;
};

inline bool ValidateEnum(MessageUnsubscribe::Reason e) {
  return e >= MessageUnsubscribe::Reason::kRequested &&
         e <= MessageUnsubscribe::Reason::kInvalid;
}

/**
 * An abstract message delivered on particular subscription.
 * Carries a pair of sequence numbers and advances subscription state according
 * to their values. Depending on concrete type may or may not carry extra data.
 */
class MessageDeliver : public Message {
 public:
  MessageDeliver(MessageType type,
                 TenantID tenant_id,
                 NamespaceID namespace_id,
                 Topic topic,
                 SubscriptionID sub_id)
      : Message(type, tenant_id),
        namespace_id_(std::move(namespace_id)),
        topic_(std::move(topic)),
        sub_id_(sub_id),
        seqno_prev_(0),
        seqno_(0) {}

  explicit MessageDeliver(MessageType type) : Message(type) {}
  virtual ~MessageDeliver() = 0;

  Slice GetNamespace() const { return namespace_id_; }

  Slice GetTopicName() const { return topic_; }

  SubscriptionID GetSubID() const { return sub_id_; }

  void SetSubID(SubscriptionID sub_id) { sub_id_ = sub_id; }

  Slice GetDataSource() const { return source_; }

  SequenceNumber GetPrevSequenceNumber() const { return seqno_prev_; }

  SequenceNumber GetSequenceNumber() const { return seqno_; }

  void SetSequenceNumbers(DataSource source,
                          SequenceNumber seqno_prev,
                          SequenceNumber seqno) {
    RS_ASSERT(seqno_prev <= seqno);
    source_ = std::move(source);
    seqno_prev_ = seqno_prev;
    seqno_ = seqno;
  }

  void SetSequenceNumbers(SequenceNumber seqno_prev, SequenceNumber seqno) {
    RS_ASSERT(seqno_prev <= seqno);
    seqno_prev_ = seqno_prev;
    seqno_ = seqno;
  }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 protected:
  NamespaceID namespace_id_;
  Topic topic_;
  DataSource source_;

 private:
  /** ID of the subscription this response refers to. */
  SubscriptionID sub_id_;
  /** Sequence number of the previous message on this subscription. */
  SequenceNumber seqno_prev_;
  /** Sequence number of this message. */
  SequenceNumber seqno_;
};

inline MessageDeliver::~MessageDeliver() {}

/** A message delivered on particular subscription, which carries no data. */
class MessageDeliverGap final : public MessageDeliver {
 public:
  MessageDeliverGap(TenantID tenant_id,
                    NamespaceID namespace_id,
                    Topic topic,
                    SubscriptionID sub_id,
                    GapType gap_type)
      : MessageDeliver(MessageType::mDeliverGap,
                       tenant_id,
                       std::move(namespace_id),
                       std::move(topic),
                       sub_id),
        gap_type_(gap_type) {}

  MessageDeliverGap() : MessageDeliver(MessageType::mDeliverGap) {}

  GapType GetGapType() const { return gap_type_; }

  SequenceNumber GetFirstSequenceNumber() const {
    return GetPrevSequenceNumber();
  }

  SequenceNumber GetLastSequenceNumber() const { return GetSequenceNumber(); }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  GapType gap_type_;
};

/** A message delivered on particular subscription. */
class MessageDeliverData final : public MessageDeliver {
 public:
  MessageDeliverData(TenantID tenant_id,
                     NamespaceID namespace_id,
                     Topic topic,
                     SubscriptionID sub_id,
                     MsgId message_id,
                     std::string payload)
      : MessageDeliver(MessageType::mDeliverData,
                       tenant_id,
                       std::move(namespace_id),
                       std::move(topic),
                       sub_id)
      , message_id_(message_id)
      , payload_(std::move(payload)) {}

  MessageDeliverData() : MessageDeliver(MessageType::mDeliverData) {}

  const MsgId& GetMessageID() const { return message_id_; };

  Slice GetPayload() const { return Slice(payload_); }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  /** ID of the message assigned by the publisher. */
  MsgId message_id_;
  /** Payload delivered with the message. */
  std::string payload_;
};

/**
 * A message which contains multiple MessageDeliver on different subscriptions.
 */
class MessageDeliverBatch : public Message {
 public:
  typedef std::vector<std::unique_ptr<MessageDeliverData>> MessagesVector;

  MessageDeliverBatch(TenantID tenant_id, MessagesVector messages)
      : Message(MessageType::mDeliverBatch, tenant_id)
      , messages_(std::move(messages)) {}

  explicit MessageDeliverBatch() : Message(MessageType::mDeliverBatch) {}

  const MessagesVector& GetMessages() const {
    return messages_;
  }

  /*
  * Inherited from Serializer
  */
  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;
 private:
  MessagesVector messages_;
};

/*
 * This is a heartbeat message
 */
class MessageHeartbeat : public Message {
 public:
  using StreamSet = std::vector<uint32_t>;
  using Clock = std::chrono::system_clock;

  MessageHeartbeat() {}
  explicit MessageHeartbeat(TenantID tenantid,
                            Clock::time_point timestamp = Clock::now(),
                            StreamSet healthy_streams = StreamSet())
    : Message(MessageType::mHeartbeat, tenantid),
      timestamp_(timestamp),
      healthy_streams_(std::move(healthy_streams)) {}

  /*
   * Inherited from Serializer
   */
  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

  /**
   * Time at the origin of this heartbeat. Recipients of heartbeats
   * can use this to determine the end-to-end latency of propagation
   * and ensure this is below a threshold.
   */
  Clock::time_point GetTimestamp() const {
    return timestamp_;
  }

  const StreamSet& GetHealthyStreams() const {
    return healthy_streams_;
  }

 private:
  Clock::time_point timestamp_;
  StreamSet healthy_streams_;
};

/*
 * This is a heartbeat delta message. It is used as an optimisation of
 * regular heartbeats over stateful connections, allowing us to send only
 * those streams that have changed in healthiness.
 */
class MessageHeartbeatDelta : public Message {
 public:
  using StreamSet = MessageHeartbeat::StreamSet;
  using Clock = MessageHeartbeat::Clock;

  MessageHeartbeatDelta() {}
  explicit MessageHeartbeatDelta(TenantID tenantid,
                                 Clock::time_point timestamp,
                                 StreamSet added_healthy,
                                 StreamSet removed_healthy)
    : Message(MessageType::mHeartbeatDelta, tenantid),
      timestamp_(timestamp),
      added_healthy_(std::move(added_healthy)),
      removed_healthy_(std::move(removed_healthy)) {}

  /*
   * Inherited from Serializer
   */
  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

  /**
   * Time at the origin of this heartbeat. Recipients of heartbeats
   * can use this to determine the end-to-end latency of propagation
   * and ensure this is below a threshold.
   */
  Clock::time_point GetTimestamp() const {
    return timestamp_;
  }

  const StreamSet& GetAddedHealthyStreams() const {
    return added_healthy_;
  }

  const StreamSet& GetRemovedHealthyStreams() const {
    return removed_healthy_;
  }

 private:
  Clock::time_point timestamp_;
  StreamSet added_healthy_;
  StreamSet removed_healthy_;
};

/**
 * Message that requests a backlog fill from the server.
 * See HasMessageSince in Client API.
 */
class MessageBacklogQuery : public Message {
 public:
  MessageBacklogQuery() {}
  explicit MessageBacklogQuery(TenantID tenantid,
                               SubscriptionID sub_id,
                               NamespaceID namespace_id,
                               Topic topic,
                               DataSource source,
                               SequenceNumber seqno)
  : Message(MessageType::mBacklogQuery, tenantid)
  , sub_id_(sub_id)
  , namespace_id_(std::move(namespace_id))
  , topic_(std::move(topic))
  , source_(std::move(source))
  , seqno_(seqno) {}

  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

  SubscriptionID GetSubID() const { return sub_id_; }
  const NamespaceID& GetNamespace() const { return namespace_id_; }
  const Topic& GetTopicName() const { return topic_; }
  const DataSource& GetDataSource() const { return source_; }
  SequenceNumber GetSequenceNumber() const { return seqno_; }

 private:
  SubscriptionID sub_id_;
  NamespaceID namespace_id_;
  Topic topic_;
  DataSource source_;
  SequenceNumber seqno_;
};

/**
 * Message that informs on the status of messages on a topic between two
 * sequence numbers. Usually sent as a response to a backlog query, but
 * information can be used for efficient caching, or advancing subscriptions.
 */
class MessageBacklogFill : public Message {
 public:
  MessageBacklogFill() {}
  explicit MessageBacklogFill(TenantID tenantid,
                              NamespaceID namespace_id,
                              Topic topic,
                              DataSource source,
                              SequenceNumber prev_seqno,
                              SequenceNumber next_seqno,
                              HasMessageSinceResult result,
                              std::string info)
  : Message(MessageType::mBacklogFill, tenantid)
  , namespace_id_(std::move(namespace_id))
  , topic_(std::move(topic))
  , source_(std::move(source))
  , prev_seqno_(prev_seqno)
  , next_seqno_(next_seqno)
  , result_(result)
  , info_(std::move(info)) {}

  Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

  const NamespaceID& GetNamespace() const { return namespace_id_; }
  const Topic& GetTopicName() const { return topic_; }
  const DataSource& GetDataSource() const { return source_; }

  /**
   * The prev/next sequence number describe the range of sequence numbers that
   * the result applies to. The range is open-open. For example, if prev=2,
   * and next=5, and the result is kNo, then neither messages at seqno 3 or 4
   * are for the requested topic.
   */
  SequenceNumber GetPrevSequenceNumber() const { return prev_seqno_; }
  SequenceNumber GetNextSequenceNumber() const { return next_seqno_; }

  /** see HasMessageSinceResult. */
  HasMessageSinceResult GetResult() const { return result_; }

  const std::string& GetInfo() const { return info_; }

 private:
  NamespaceID namespace_id_;
  Topic topic_;
  DataSource source_;
  SequenceNumber prev_seqno_;
  SequenceNumber next_seqno_;
  HasMessageSinceResult result_;
  std::string info_;
};

/**
 * Message used by the clients to introduce themselves to the server providing
 * certain key:value properties. Client can add any number of key value pairs
 * Both Key and Value should be strings.
 */
class MessageIntroduction final : public Message {
 public:
  MessageIntroduction(TenantID tenant_id,
                      IntroProperties stream_properties,
                      IntroProperties client_properties)
  : Message(MessageType::mIntroduction, tenant_id)
  , stream_properties_(std::move(stream_properties))
  , client_properties_(std::move(client_properties)) {}

  MessageIntroduction() : Message(MessageType::mIntroduction) {}

  const IntroProperties& GetStreamProperties() const {
    return stream_properties_;
  }
  const IntroProperties& GetClientProperties() const {
    return client_properties_;
  }

  virtual Status Serialize(std::string* out) const override;
  Status DeSerialize(Slice* in) override;

 private:
  IntroProperties stream_properties_;
  IntroProperties client_properties_;
};

/** @} */

}  // namespace rocketspeed
