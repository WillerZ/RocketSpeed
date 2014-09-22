/**
 * @file
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright Facebook 2014
 *
 * @section DESCRIPTION
 *
 * This file defines the various data structures needed to access
 * the RocketSpeed pub-sub service.
 */
#pragma once

#include <cstring>
#include <string>
#include <vector>

#include "include/Status.h"

namespace rocketspeed {

/**
 * Each Topic is a string
 */
typedef std::string Topic;

/**
 * Each message has a sequence number associated with it.
 * A newly produced message has a higher sequence number than
 * a message produced earlier.
 */
typedef uint64_t SequenceNumber;

/**
 * The unique identifier of a message. This is globally unique in the entire
 * RocketSpeed ecosystem. A producer typically generates this id by calculating
 * a MD5/SHA signature of the message payload.
 */
class MsgId {
 public:
  char messageId[16] {};

  MsgId() {}

  explicit MsgId(char msgid[16]) {
    memcpy(messageId, msgid, 16);
  }

  bool operator<(const MsgId& rhs) const {
    return memcmp(messageId, rhs.messageId, sizeof(messageId)) < 0;
  }

  bool operator==(const MsgId& rhs) const {
    return memcmp(messageId, rhs.messageId, sizeof(messageId)) == 0;
  }
};

/**
 * This is the status returned when a new message is published.
 */
class PublishStatus {
 public:
  Status status;
  MsgId msgid;

  PublishStatus(Status status, MsgId msgid)
  : status(status)
  , msgid(msgid) {
  }
};

/**
 * This is the status returned when a published message is acknowledged.
 */
class ResultStatus {
 public:
  Status status;
  MsgId msgid;
  SequenceNumber seqno;
};

/**
 *  Type of callback that is called when a message arrives
 *
 *  @param status OK if this message is received successfully
 *  @param seqno Sequence Number of message
 *  @param name Name of topic
 *  @param payload  The message data itself.
 */
class MessageReceived {
 public:
  Status status;
  SequenceNumber seqno;
  Topic name;
  std::string payload;
};

/**
 * An object of the form scheme://host:port/path
 */
typedef std::string URL;

/**
 * A unique ID for this RocketSpeed tenant. Each tenant will have a Service
 * Level Agreement with the RocketSpeed system used to limit the impact any one
 * tenant can have on the system as a whole. If a tenant exceeds their agreed
 * resource usage then their experience may be throttled.
 */
typedef uint16_t TenantID;

enum Tenant : TenantID {
  /**
   * The invalid tenant ID should never be used. It is here to catch cases
   * when the client fails to set the tenant ID.
   */
  Invalid = 0,

  /**
   * The Guest Tenant ID may be used by applications during development. The
   * guest tenant has a small amount of resources allocated to it, which should
   * be suitable for lightweight development. This should not be used in
   * production.
   */
  Guest = 1,

  /**
   * TenantIds 2-100 are reserved for system usage. Real users should be
   * assigned ids larger than 100
   */
};

/**
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

  bool operator<(const HostId& rhs) const {
    if (port < rhs.port) {
      return true;
    } else if (port > rhs.port) {
      return false;
    } else {
      return hostname < rhs.hostname;
    }
  }

  bool operator==(const HostId& rhs) const {
    return port == rhs.port && hostname == rhs.hostname;
  }
};

/**
 * A Configuration that specifies how a client can describe a RocketSpeed Cloud.
 *
 * @param url The name of the RocketSpeed Cloud Service
 * @param tenancyID A unique ID for this service.
 */
class Configuration {
 public:
  /**
   * Creates a Configuration object for a set of pilots and tenant ID.
   *
   * @param pilots Pilot hostnames.
   * @param tenant_id Client tenant ID.
   * @return A new Configuration with the specified options.
   */
  static Configuration* Create(const std::vector<HostId>& pilots,
                               TenantID tenant_id,
                               int local_port);

  virtual ~Configuration() {}

  /**
   * The list of Pilot endpoints
   */
  virtual const std::vector<HostId>& GetPilotHostIds() const = 0;

  /**
   * The Tenant associated with this configuration
   */
  virtual TenantID GetTenantID() const = 0;

  /**
   * The port on the client on incoming messages are received
   */
  virtual int GetLocalPort() const = 0;
};

enum Retention : char {
  OneHour = 0x01,         // keep messages for 1 hour
  OneDay = 0x02,          // keep messages for 1 day
  OneWeek = 0x03,         // keep messages for 1 week
  Total = 3,              // number of retention classes
};

/**
 * These are the options associated with a Topic
 *
 *  @param retention The amount of time a message would remain in RocketSpeed
 */
class TopicOptions {
 public:
  Retention retention;
};

}
