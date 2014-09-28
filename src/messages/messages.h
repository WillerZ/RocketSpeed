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
enum MessageType : char {
  NotInitialized = 0,         // not initialized yet
  mPing = 0x01,               // ping data
  mData = 0x02,               // user data
  mMetadata = 0x03,           // subscription information
  mDataAck = 0x04,            // ack for user data
};

/*
 * The metadata messages can be of two subtypes
 */
enum MetadataType : char {
  mNotinitialized = 0x00,          // message is not yet initialized
  mSubscribe = 0x01,               // subscribe
  mUnSubscribe = 0x02              // unsubscribe
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
    topic_name(name),
    namespace_id(namespaceId),
    topic_type(type) {
  }
  TopicPair() {}
};

/**
 * This is a superclass of all RocketSpeed messages.
 * All RocketSpeed messages have a type, a sequence number,
 * a tenant-id and a msgid.
 * The type identifies the functionality of the message,
 * the TenantId is used to implement fair sharing of resources
 * among multiple users of the system.
 * The sequence number is used to delivery messages in order.
 * The msgid is used to remove duplicates messages.
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

  /*
   * Creates a Message of the appropriate subtype by looking at the
   * MessageType. Returns nullptr on error. It is the responsibility
   * of the caller to own this memory object.
   */
  static std::unique_ptr<Message> CreateNewInstance(Slice* in);

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const = 0;
  virtual Status DeSerialize(Slice* in) = 0;

 protected:
  Message(MessageType type, TenantID tenantid) :
          type_(type), tenantid_(tenantid) {
    msghdr_.version_ = ROCKETSPEED_CURRENT_MSG_VERSION;
  }
  Message() : Message(MessageType::NotInitialized, 0) {}

  MessageType type_;         // type of this message
  TenantID tenantid_;        // unique id for tenant
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
              const HostId& origin) :
              Message(MessageType::mPing, tenantid),
              pingtype_(pingtype), origin_(origin) {}

  PingType GetPingType() const {
    return pingtype_;
  }

  void SetPingType(PingType type) {
    pingtype_ = type;
  }

  const HostId& GetOrigin() const {
    return origin_;
  }
  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

 protected:
  PingType pingtype_;
  HostId origin_;
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
  MessageData(TenantID tenantID,
              const HostId& origin,
              const Slice& topic_name,
              const NamespaceID namespace_id,
              const Slice& payload,
              Retention retention = Retention::OneWeek);

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
   * @return The Origin host.
   */
  const HostId& GetOrigin() const { return origin_; }

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

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

  /**
   * @param clear_buffer Set to true to serialize to the start of the buffer.
   * @return Serializes the message into the log storage format.
   */
  Slice SerializeStorage() const;

  /**
   * @return Deserializes the message from the log storage format.
   *         Only the tenant ID, topic_name, and payload are deserialized.
   *         The seqno_ and origin_ are not deserialized. Sequence number is
   *         stored by the log storage, and origin_ is not stored at all.
   */
  Status DeSerializeStorage(Slice* in);

 private:
  void SerializeInternal() const;

  // type of this message: mData
  SequenceNumber seqno_;     // sequence number of message
  MsgId msgid_;              // globally unique id for message
  HostId origin_;            // host that sent the data
  Slice topic_name_;         // name of topic
  Slice payload_;            // user data of message
  Retention retention_;      // message retention
  NamespaceID namespaceid_;  // message namespace
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
                  const HostId& origin,
                  const std::vector<TopicPair>& topics);

  /*
   * default constructor
   */
  MessageMetadata();

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

  /**
   * @return The Hostid of the origininator of this request
   */
  const HostId& GetOrigin() const { return origin_; }

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

 private:
  MetaType metatype_;         // request or response
  HostId origin_;             // unique identifier for a client

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
    AckStatus status;
    MsgId msgid;

    bool operator==(const Ack& rhs) const {
      return status == rhs.status && msgid == rhs.msgid;
    }
  };

  /**
   * Creates a message by specifying the message acks.
   *
   * @param msgid The MsgIds of the messages that have been ack'd
   */
  explicit MessageDataAck(const std::vector<Ack>& acks);

  /**
   * default constructor
   */
  MessageDataAck();

  /**
   * This message is not needed any more
   */
  virtual ~MessageDataAck();

  /**
   * @return The message IDs
   */
  const std::vector<Ack>& GetAcks() const;

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

 private:
  // type of this message: mDataAck
  std::vector<Ack> acks_;  // message acks
};

}  // namespace rocketspeed
