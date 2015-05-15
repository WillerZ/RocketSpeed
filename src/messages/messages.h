// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/serializer.h"
#include "src/util/common/autovector.h"

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

/** The message types. */
enum class MessageType : uint8_t {
  NotInitialized = 0,   // not initialized yet
  mPing = 0x01,         // ping data
  mPublish = 0x02,      // data publish
  mMetadata = 0x03,     // subscription information
  mDataAck = 0x04,      // ack for user data
  mGap = 0x05,          // gap in the log
  mDeliver = 0x06,      // data delivery
  mGoodbye = 0x07,      // MessageGoodbye
  mSubscribe = 0x08,    // MessageSubscribe
  mUnsubscribe = 0x09,  // MessageUnsubscribe
  mDeliverGap = 0x0A,   // MessageDeliverGap
  mDeliverData = 0x0B,  // MessageDeliverData
};

inline std::ostream& operator<<(std::ostream& os, MessageType type) {
  return os << static_cast<std::underlying_type<MessageType>::type>(type);
}

inline bool ValidateEnum(MessageType e) {
  return e >= MessageType::mPing && e <= MessageType::mDeliverData;
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

/**
 * Type of gaps that may appear in the logs.
 */
enum GapType : uint8_t {
  kBenign = 0x00,     // Gap due to operational issue, no data lost.
  kDataLoss = 0x01,   // Catastrophic failure, acknowledged data was lost.
  kRetention = 0x02,  // Gap due to data falling out of retention period.
};

inline bool ValidateEnum(GapType e) {
  return e >= kBenign && e <= kRetention;
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
class Message : public Serializer {
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
   * Creates a Message of the appropriate subtype by looking at the
   * MessageType. Returns nullptr on error.  The memory ownership is passed
   * to the message itself, and will be discarded once the message is destroyed.
   */
  static std::unique_ptr<Message> CreateNewInstance(std::unique_ptr<char[]> in,
                                                    size_t size);

  /**
   * Creates a Message of the appropriate subtype by looking at the
   * MessageType. Returns nullptr on error.  The memory ownership is passed
   * to the message itself, and will be discarded once the message is destroyed.
   * Uses a subslice of `in` to parse the message.
   */
  static std::unique_ptr<Message> CreateNewInstance(std::unique_ptr<char[]> in,
                                                    Slice slice);

  /*
   * Inherited from Serializer
   */
  Status DeSerialize(Slice* in) override;
  void SerializeToString(std::string* out) const;

  /**
   * Creates a deep copy of a message.
   */
  static std::unique_ptr<Message> Copy(const Message& msg);

 protected:
  Message(MessageType type, TenantID tenantid) :
          type_(type), tenantid_(tenantid) {
  }

  explicit Message(MessageType type) : type_(type) {
  }

  Message() : type_(MessageType::NotInitialized) {
  }

  Slice Serialize() const override;

  MessageType type_;                // type of this message
  TenantID tenantid_;               // unique id for tenant
  ClientID origin_;                 // origin stream
  std::unique_ptr<char[]> buffer_;  // owned memory for slices

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
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

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
              const Slice& topic_name,
              const Slice& namespace_id,
              const Slice& payload);

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
  Slice GetTopicName() const { return topic_name_; }

  /**
   * @return The namespace of this topic
   */
  Slice GetNamespaceId() const { return namespaceid_; }

  /**
   * @return The Message payload
   */
  Slice GetPayload() const { return payload_; }

  /**
   * @return the slice containing tenant ID, topic_name and paylodad from
   * buffer_
   */
  Slice GetStorageSlice() const;

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

  /**
   * @return Deserializes the message from the log storage format.
   *         Only the tenant ID, topic_name, and payload are deserialized.
   *         The seqno_ is not deserialized. Sequence number is
   *         stored by the log storage.
   */
  Status DeSerializeStorage(Slice* in);

 private:
  void SerializeInternal() const;

  // type of this message: mPublish or mDeliver
  SequenceNumber seqno_prev_; // previous sequence number on topic
  SequenceNumber seqno_;      // sequence number of message
  MsgId msgid_;               // globally unique id for message
  Slice topic_name_;          // name of topic
  Slice payload_;             // user data of message
  Slice namespaceid_;         // message namespace
  Slice storage_slice_;       // slice starting from tenantid from buffer_
};

/*
 * This is a subscribe/unsubscribe message.
 */
class MessageMetadata : public Message {
 public:
  // Two types of metadata messages
  enum MetaType : char {
    NotInitialized = 0x00,        // message not yet initialized
    Request = 0x01,               // request
    Response = 0x02               // request processed ack
  };
  /**
   * Creates a message by specifying its contents.
   * @param seqno The client-supplied sequence number of this metadata msg
   * @param metatype Request or response.
   * @param topics The list of topics to subscribe-to/unsubscribe-from
   */
  MessageMetadata(TenantID tenantID,
                  const MetaType metatype,
                  const std::vector<TopicPair>& topics);

  /*
   * default constructor
   */
  MessageMetadata();

  /*
   * copy constructor
   */
  explicit MessageMetadata(MessageMetadata* in) :
    Message(MessageType::mMetadata, in->GetTenantID()),
    metatype_(in->GetMetaType()),
    topics_(in->GetTopicInfo()) {
  }

  /**
   * This message is not needed any more
   */
  virtual ~MessageMetadata();

  /**
   * Is it a request or a response?
   */
  MetaType GetMetaType() const {
    return metatype_;
  }

  /**
   * Set the metatype response/request
   */
  void SetMetaType(MetaType metatype) {
    metatype_ = metatype;
  }

  /**
   * @return Information about all topics
   */
  const std::vector<TopicPair>& GetTopicInfo() const { return topics_; }
  std::vector<TopicPair>& GetTopicInfo() { return topics_; }

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

 private:
  MetaType metatype_;         // request or response

  // The List of topics to subscribe-to/unsubscribe-from
  std::vector<TopicPair> topics_;
};

inline bool ValidateEnum(MessageMetadata::MetaType e) {
  return e >= MessageMetadata::Request && e <= MessageMetadata::Response;
}

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
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

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
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

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

  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

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
 * Messages exchanged on a subscription.
 * @{
 */

/**
 * ID of the subscription the message belongs to. IDs are uniquely identify
 * subscription within a stream.
 */
typedef uint32_t SubscriptionID;

/** A request to subscribe to provided topic with given parameters. */
class MessageSubscribe final : public Message {
 public:
  MessageSubscribe(TenantID tenant_id,
                   const NamespaceID& namespace_id,
                   const Topic& topic_name,
                   SequenceNumber start_seqno,
                   SubscriptionID sub_id)
      : Message(MessageType::mSubscribe, tenant_id),
        namespace_id_(namespace_id),
        topic_name_(topic_name),
        start_seqno_(start_seqno),
        sub_id_(sub_id) {}

  MessageSubscribe() : Message(MessageType::mSubscribe) {}

  const NamespaceID& GetNamespace() const { return namespace_id_; }

  const Topic& GetTopicName() const { return topic_name_; }

  SequenceNumber GetStartSequenceNumber() const { return start_seqno_; }

  SubscriptionID GetSubID() const { return sub_id_; }

  Slice Serialize() const override;
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
    kRequested = 0x00,
    kBackOff = 0x01,
    kInvalid = 0x02,
  };

  MessageUnsubscribe(TenantID tenant_id, SubscriptionID sub_id, Reason reason)
      : Message(MessageType::mUnsubscribe, tenant_id),
        sub_id_(sub_id),
        reason_(reason) {}

  MessageUnsubscribe() : Message(MessageType::mUnsubscribe) {}

  SubscriptionID GetSubID() const { return sub_id_; }

  Reason GetReason() const { return reason_; }

  Slice Serialize() const override;
  Status DeSerialize(Slice* in) override;

 private:
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
  MessageDeliver(MessageType type, TenantID tenant_id, SubscriptionID sub_id)
      : Message(type, tenant_id), sub_id_(sub_id), seqno_prev_(0), seqno_(0) {}

  explicit MessageDeliver(MessageType type) : Message(type) {}
  virtual ~MessageDeliver() = 0;

  SubscriptionID GetSubID() const { return sub_id_; }

  SequenceNumber GetPrevSequenceNumber() const { return seqno_prev_; }

  SequenceNumber GetSequenceNumber() const { return seqno_; }

  void SetSequenceNumbers(SequenceNumber seqno_prev, SequenceNumber seqno) {
    assert(seqno_prev <= seqno);
    seqno_prev_ = seqno_prev;
    seqno_ = seqno;
  }

  Slice Serialize() const override;
  Status DeSerialize(Slice* in) override;

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
  MessageDeliverGap(TenantID tenant_id, SubscriptionID sub_id, GapType gap_type)
      : MessageDeliver(MessageType::mDeliverGap, tenant_id, sub_id),
        gap_type_(gap_type) {}

  MessageDeliverGap() : MessageDeliver(MessageType::mDeliverGap) {}

  GapType GetGapType() const { return gap_type_; }

  Slice Serialize() const override;
  Status DeSerialize(Slice* in) override;

 private:
  GapType gap_type_;
};

/** A message delivered on particular subscription. */
class MessageDeliverData final : public MessageDeliver {
 public:
  MessageDeliverData(TenantID tenant_id, SubscriptionID sub_id, Slice payload)
      : MessageDeliver(MessageType::mDeliverData, tenant_id, sub_id),
        payload_(payload) {}

  MessageDeliverData() : MessageDeliver(MessageType::mDeliverData) {}

  Slice GetPayload() const { return payload_; }

  Slice Serialize() const override;
  Status DeSerialize(Slice* in) override;

 private:
  /** Payload delivered with the message. */
  Slice payload_;
};
/** @} */

}  // namespace rocketspeed
