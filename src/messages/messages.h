// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>
#include <vector>
#include <memory>

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
#define ROCKETSPEED_CURRENT_MSG_VERSION 1

namespace rocketspeed {

/*
 * The message types. The first byte of a message indicates the message type.
 */
enum MessageType : uint8_t {
  NotInitialized = 0,         // not initialized yet
  mPing = 0x01,               // ping data
  mPublish = 0x02,            // data publish
  mMetadata = 0x03,           // subscription information
  mDataAck = 0x04,            // ack for user data
  mGap = 0x05,                // gap in the log
  mDeliver = 0x06,            // data delivery
  mGoodbye = 0x07,            // goodbye
};

/*
 * The metadata messages can be of two subtypes
 */
enum MetadataType : uint8_t {
  mNotinitialized = 0x00,          // message is not yet initialized
  mSubscribe = 0x01,               // subscribe
  mUnSubscribe = 0x02              // unsubscribe
};

/**
 * Type of gaps that may appear in the logs.
 */
enum GapType : uint8_t {
  kBenign = 0x00,     // Gap due to operational issue, no data lost.
  kDataLoss = 0x01,   // Catastrophic failure, acknowledged data was lost.
  kRetention = 0x02,  // Gap due to data falling out of retention period.
};

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
    namespace_id(namespaceId),
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
   * The origin is the client that originated a request.
   * @return The Origin client ID.
   */
  const ClientID& GetOrigin() const { return origin_; }

  /**
   * Set the origin client ID.
   */

  void SetOrigin(const ClientID& client_id) {
    origin_ = client_id;
  }

  /**
   * Creates a Message of the appropriate subtype by looking at the
   * MessageType. Returns nullptr on error.  The memory ownership is passed
   * to the message itself, and will be discarded once the message is destroyed.
   */
  static std::unique_ptr<Message> CreateNewInstance(std::unique_ptr<char[]> in,
                                                    size_t size);

  /*
   * Inherited from Serializer
   */
  virtual Status DeSerialize(Slice* in) = 0;
  void SerializeToString(std::string* out) const;

 protected:
  Message(MessageType type, TenantID tenantid, const ClientID& origin) :
          type_(type), tenantid_(tenantid), origin_(origin) {
    msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  }
  Message() : type_(MessageType::NotInitialized) {
    msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  }
  void serializeMessageHeader() const;
  void serializeMessageSize() const;
  void serializeMessageSize(int msgsize) const;
  Status deserializeMessageHeader(Slice* in);


  MessageType type_;                // type of this message
  TenantID tenantid_;               // unique id for tenant
  ClientID origin_;                 // sender's id
  std::unique_ptr<char[]> buffer_;  // owned memory for slices

 private:
  static std::unique_ptr<Message> CreateNewInstance(Slice* in);
  virtual Slice Serialize() const = 0;
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

  MessagePing(TenantID tenantid,
              PingType pingtype,
              const ClientID& origin) :
              Message(MessageType::mPing, tenantid, origin),
              pingtype_(pingtype) {}

  PingType GetPingType() const {
    return pingtype_;
  }

  void SetPingType(PingType type) {
    pingtype_ = type;
  }

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

 protected:
  PingType pingtype_;
};

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
              const ClientID& origin,
              const Slice& topic_name,
              const NamespaceID namespace_id,
              const Slice& payload,
              Retention retention = Retention::OneWeek);

  /*
   * copy constructor
   */
  explicit MessageData(MessageData* in) :
    Message(in->GetMessageType(), in->GetTenantID(), in->GetOrigin()),
    seqno_(in->GetSequenceNumber()),
    msgid_(in->GetMessageId()),
    retention_(in->GetRetention()),
    namespaceid_(in->GetNamespaceId()) {

    // allocate buffer to copy the slices to owned buffer
    ssize_t size = in->GetTopicName().size() + in->GetPayload().size();
    buffer_.reset(new char[size]);

    // copy topic name
    memcpy(buffer_.get(), in->GetTopicName().data(), in->GetTopicName().size());
    topic_name_ = Slice(buffer_.get(), in->GetTopicName().size());
    ssize_t offset = in->GetTopicName().size();

    // copy payload
    memcpy(buffer_.get() + offset, in->GetPayload().data(),
           in->GetPayload().size());
    payload_ = Slice(buffer_.get() + offset, in->GetPayload().size());
  }

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
   * @Sets the Sequence Number.
   */
  void SetSequenceNumber(SequenceNumber n) { seqno_ = n; }

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
  NamespaceID GetNamespaceId() const { return namespaceid_; }

  /**
   * @return The Message payload
   */
  Slice GetPayload() const { return payload_; }

  /**
   * @return The retention of this message.
   */
  Retention GetRetention() const { return retention_; }

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
   *         The seqno_ and origin_ are not deserialized. Sequence number is
   *         stored by the log storage, and origin_ is not stored at all.
   */
  Status DeSerializeStorage(Slice* in);

 private:
  void SerializeInternal() const;

  // type of this message: mPublish or mDeliver
  SequenceNumber seqno_;     // sequence number of message
  MsgId msgid_;              // globally unique id for message
  Slice topic_name_;         // name of topic
  Slice payload_;            // user data of message
  Retention retention_;      // message retention
  NamespaceID namespaceid_;  // message namespace
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
   * @param hostid The identifier of the client
   * @param topics The list of topics to subscribe-to/unsubscribe-from
   */
  MessageMetadata(TenantID tenantID,
                  const MetaType metatype,
                  const ClientID& origin,
                  const std::vector<TopicPair>& topics);

  /*
   * default constructor
   */
  MessageMetadata();

  /*
   * copy constructor
   */
  explicit MessageMetadata(MessageMetadata* in) :
    Message(MessageType::mMetadata, in->GetTenantID(), in->GetOrigin()),
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
                          const ClientID& origin,
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
                      const ClientID& origin,
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
  GapType GetType() const {
    return gap_type_;
  }

  SequenceNumber GetStartSequenceNumber() const {
    return gap_from_;
  }

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
  enum Code : char {
    SocketError = 0x00,
    Graceful = 0x01,
  };

  enum OriginType : char {
    Client = 0x00,
    Server = 0x01,
  };

  /**
   * Creates a goodbye message.
   *
   * @param tenant_id The tenant ID of the origin.
   * @param origin The originator of the communication.
   * @param code The error code.
   */
  explicit MessageGoodbye(TenantID tenant_id,
                          ClientID origin,
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


}  // namespace rocketspeed
