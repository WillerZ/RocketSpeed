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
  mData = 0x01,               // user data
  mMetadata = 0x02            // subscription information
};

/*
 * The unique identifier of a message. This is globally unique in the entire
 * RocketSpeed ecosystem. A producer typically generates this id by calculating
 * a MD5/SHA signature of the message payload.
 */
class MsgId {
 public:
  char messageId[16];
};

/*
 * A host:port pair that uniquely identifies a machine.
 */
class HostId {
 public:
  std::string hostname;      // name of a machine
  uint64_t    port;          // name of port to connect to

  HostId(std::string s, uint64_t p) :
    hostname(s),
    port(p) {
  }
  HostId() {}
};

/*
 * The metadata messages can be of two subtypes
 */
enum MetadataType : char {
  mSubscribe = 0x01,               // subscribe
  mUnSubscribe = 0x02              // unsubscribe
};

/*
 * A topic:bool pair to indicate whether to subscribe or unsubscribe
 * from the specified topic
 */
class TopicPair {
 public:
  std::string topic_name;
  MetadataType topic_type;

  TopicPair(std::string name, MetadataType type) :
    topic_name(name),
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
  MessageType GetMessageType() { return type_; }
  /**
   * @return The tenant ID.
   */
  TenantID GetTenantID() const { return tenantid_; }
  /**
   * @return The message Sequence Number.
   */
  SequenceNumber GetSequenceNumber() { return seqno_; }

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
  MessageType type_;         // type of this message
  TenantID tenantid_;        // unique id for tenant
  SequenceNumber seqno_;     // sequence number of message
};


/*********************************************************/
/**    Message definitions start here                  ***/
/*********************************************************/

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
  MessageData(TenantID tenantID, const Slice& topic_name, const Slice& payload);

  /*
   * default constructor
   */
  MessageData();

  /**
   * This message is not needed any more
   */
  virtual ~MessageData();

  /**
   * @return The Topic Name
   */
  Slice GetTopicName() { return topic_name_; }

  /**
   * @return The Message payload
   */
  Slice GetPayload() { return payload_; }


  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

 private:
  // type of this message: mData
  MsgId msgid_;              // globally unique id for message
  Slice topic_name_;         // name of topic
  Slice payload_;            // user data of message
};

/*
 * This is a subscribe/unsubscribe message.
 */
class MessageMetadata : public Message {
 public:
  /**
   * Creates a message by specifying its contents.
   * @param seqno The client-supplied sequence number of this metadata msg
   * @param hostid The identifier of the client
   * @param topics The list of topics to subscribe-to/unsubscribe-from
   */
  MessageMetadata(TenantID tenantID,
                  const SequenceNumber seqno,
                  const HostId& hostid,
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
   * @return Information about all topics
   */
  const std::vector<TopicPair>& GetTopicInfo() { return topics_; }

  /**
   * @return The Hostid
   */
  const HostId& GetHostId() { return hostid_; }

  /*
   * Inherited from Serializer
   */
  virtual Slice Serialize() const;
  virtual Status DeSerialize(Slice* in);

 private:
  HostId hostid_;             // unique identifier for a client

  // The List of topics to subscribe-to/unsubscribe-from
  std::vector<TopicPair> topics_;
};

}  // namespace rocketspeed
