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
#include "include/Slice.h"

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
 * A globally unique identifier.
 */
struct GUID {
  union {
    char id[16];
    struct { uint64_t hi, lo; };
  };

  GUID() = default;

  explicit GUID(char guid[16]) {
    memcpy(id, guid, 16);
  }

  bool operator<(const GUID& rhs) const {
    return hi < rhs.hi || (hi == rhs.hi && lo < rhs.lo);
  }

  bool operator==(const GUID& rhs) const {
    return lo == rhs.lo && hi == rhs.hi;
  }

  struct Hash {
    size_t operator()(const GUID& guid) const {
      return guid.lo ^ guid.hi;
    }
  };
};

/**
 * The unique identifier of a message. This is globally unique in the entire
 * RocketSpeed ecosystem.
 */
typedef GUID MsgId;

/**
 * This is the status returned when a new message is published.
 */
class PublishStatus {
 public:
  Status status;
  MsgId msgid;

  PublishStatus(Status pstatus, MsgId messageid)
  : status(pstatus)
  , msgid(messageid) {
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
 * This is the status returned when a subscription/unsubscription
 * message is acknowledged and confirmed by the Cloud Service.
 */
class SubscriptionStatus {
 public:
  SubscriptionStatus() : seqno(0), subscribed(false) {}
  Status status;
  SequenceNumber seqno;  // the start seqno of a subscription
  bool   subscribed;     // true for subscription, false for unsubscription
};

/**
 *  Message received on subscribed topics
 */
class MessageReceived {
 public:
  // The sequence number of this message
  virtual SequenceNumber GetSequenceNumber() = 0;

  // The Topic name
  virtual const Slice GetTopicName() = 0;

  // The contents of the message
  virtual const Slice GetContents() = 0;

  virtual ~MessageReceived()  {}
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
  InvalidTenant = 0,

  /**
   * The Guest Tenant ID may be used by applications during development. The
   * guest tenant has a small amount of resources allocated to it, which should
   * be suitable for lightweight development. This should not be used in
   * production.
   */
  GuestTenant = 1,

  /**
   * TenantIds 2-100 are reserved for system usage. Real users should be
   * assigned ids larger than 100
   */
};

/**
 * A unique ID for this RocketSpeed namespace. Each namespace can have its own
 * set of topic names. Namespaces are a way to partition the set of topics in
 * a single instance of RocketSpeed.
 * A Tenant can access topics from any number of namespaces.
 */
typedef uint16_t NamespaceID;

enum Namespace : NamespaceID {
  /**
   * The invalid namespaceID should never be used. It is here to catch cases
   * when the client fails to set the tenant ID.
   */
  InvalidNamespace = 0,

  /**
   * The Guest Namespace ID may be used by applications during development.
   */
  GuestNamespace = 1,

  /**
   * NamespaceIds 2-100 are reserved for system usage. Real users should be
   * using namespaces larger than 100
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

  std::string ToString() const {
    return hostname + ":" + std::to_string(port);
  }
};

/*
 *  A topic:bool pair to indicate whether to subscribe or unsubscribe
 *  from the specified topic.
 *  Messages after the specified sequence number will be delivered to
 *  the client.
 *  A seqno of 0 indicates that the subscriber is interested in
 *  receiving whatever data is available via this topic.
 *  A seqno of 1 indicates that the subscriber is interested in
 *  receiving all the data that was published to this topic.
 **/
class SubscriptionPair {
 public:
  SequenceNumber seqno;  // the starting sequence number for this subscription
  Topic topic_name;
  NamespaceID namespace_id;

  SubscriptionPair(SequenceNumber s, std::string name,
                   NamespaceID namespaceId) :
    seqno(s),
    topic_name(name),
    namespace_id(namespaceId) {
  }
  SubscriptionPair() {}
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
   * @param copilots Copilot hostnames.
   * @param tenant_id Client tenant ID.
   * @param client_port The port on the client for receiving
   *                    incoming messages
   * @return A new Configuration with the specified options.
   */
  static Configuration* Create(const std::vector<HostId>& pilots,
                               const std::vector<HostId>& copilots,
                               TenantID tenant_id,
                               int client_port);

  virtual ~Configuration() {}

  /**
   * The list of Pilot endpoints
   */
  virtual const std::vector<HostId>& GetPilotHostIds() const = 0;

  /**
   * The list of CoPilot endpoints
   */
  virtual const std::vector<HostId>& GetCopilotHostIds() const = 0;

  /**
   * The Tenant associated with this configuration
   */
  virtual TenantID GetTenantID() const = 0;

  /**
   * The port on the client on incoming messages are received
   */
  virtual int GetClientPort() const = 0;
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
  TopicOptions()
  : retention(Retention::OneWeek) {}

  explicit TopicOptions(Retention retentionp)
  : retention(retentionp) {}

  Retention retention;
};

}
