// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>

#include "include/Status.h"
#include "include/Slice.h"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif

namespace rocketspeed {

/**
 * Identifies a stream, which is a pair of unidirectional channels, one in each
 * direction. Messages flowing in one direction within given stream are linearly
 * ordered. Two messages flowing in opposite directions have no ordering
 * guarantees.
 * The ID uniquely identifies a stream within a single physical connection only,
 * that means if streams are multiplexed on the same connection and have the
 * same IDs, the IDs need to be remapped. The IDs do not need to be unique
 * system-wide.
 */
// TODO(stupaq) move to stream sockets, once we drop dependency on stream
// allocator
typedef std::string StreamID;

/**
 * A unique ID for this RocketSpeed namespace. Each namespace can have its own
 * set of topic names. Namespaces are a way to partition the set of topics in
 * a single instance of RocketSpeed.
 *
 * A Tenant can access topics from any number of namespaces.
 *
 * Namespace beginning with an underscore (_) are reserved for system usage.
 */
typedef std::string NamespaceID;

/**
 * Tests if a namespace is reserved.
 *
 * @param ns The namespace to test.
 * @return true iff the namespace is reserved for system usage.
 */
bool IsReserved(const NamespaceID& ns);

/**
 * The invalid namespaceID should never be used. It is here to catch cases
 * when the client fails to set the tenant ID.
 */
extern const NamespaceID InvalidNamespace;

/**
 * The Guest Namespace ID may be used by applications during development.
 */
extern const NamespaceID GuestNamespace;

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

  explicit GUID(const char guid[16]) {
    memcpy(id, guid, 16);
  }

  bool operator<(const GUID& rhs) const {
    return hi < rhs.hi || (hi == rhs.hi && lo < rhs.lo);
  }

  bool operator==(const GUID& rhs) const {
    return lo == rhs.lo && hi == rhs.hi;
  }

  // Return a string that contains a copy of the GUID.
  std::string ToString() const {
    return std::string(id, 16);
  }
  // if hex == true, it returns a Hex format of the GUID
  std::string ToHexString() const {
    char buf[33];
    memset(buf, 0, 33);
    for (int i = 0; i < 16; i++) {
      snprintf(buf + (i * 2), 3, "%02X", (unsigned char)id[i]);
    }
    return std::string(buf);
  }
  // Sets the GUID to the string, only if str is at least 16 chars long
  // returns true if successfully set or false otherwise.
  bool FromString(const std::string& str) {
    if (str.length() == 16) {
      memcpy(id, str.c_str(), 16);
      return true;
    }
    // clear and return false
    Clear();
    return false;
  }

  // set the hi and lo = 0 to mark it as empty
  void Clear() {
    hi = lo = 0;
  }
  // return true iff both hi and lo are 0
  bool Empty() const {
    return (hi == 0 && lo == 0);
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

  PublishStatus() {}
};

/**
 * This is the status returned when a published message is acknowledged.
 */
class ResultStatus {
 public:
  /**
   * The status of the Publish call.
   */
  virtual Status GetStatus() const = 0;

  /**
   * The message ID of the published message. This will match the message ID
   * given in the PublishStatus.
   */
  virtual MsgId GetMessageId() const = 0;

  /**
   * The sequence number where the message was published. A subscription to
   * this sequence number + topic + namespace will receive this message first
   * (assuming it is still within the retention period).
   */
  virtual SequenceNumber GetSequenceNumber() const = 0;

  /**
   * Topic name of the published message.
   * This Slice is only valid as long as this ResultStatus.
   */
  virtual Slice GetTopicName() const = 0;

  /**
   * Namespace of the published message.
   */
  virtual Slice GetNamespaceId() const = 0;

  /**
   * Payload of the published message.
   * This Slice is only valid as long as this ResultStatus.
   */
  virtual Slice GetContents() const = 0;

  virtual ~ResultStatus() {}
};

/**
 * This is the status returned when a subscription/unsubscription
 * message is acknowledged and confirmed by the Cloud Service.
 */
class SubscriptionStatus {
 public:
  SubscriptionStatus() : seqno(0),
                         subscribed(false),
                         topic_name(""),
                         namespace_id("") {}

  SubscriptionStatus(NamespaceID _namespace_id,
                     Topic _topic_name,
                     SequenceNumber _seqno,
                     bool _subscribe,
                     Status _status)
      : status(std::move(_status))
      , seqno(_seqno)
      , subscribed(_subscribe)
      , topic_name(std::move(_topic_name))
      , namespace_id(std::move(_namespace_id)) {}

  Status status;
  SequenceNumber seqno;  // the start seqno of a subscription
  bool subscribed;       // true for subscription, false for unsubscription
  Topic topic_name;
  NamespaceID namespace_id;
};

/**
 *  Message received on subscribed topics
 */
class MessageReceived {
 public:
  // The sequence number of this message
  virtual SequenceNumber GetSequenceNumber() const = 0;

  // The namespace id of this message
  virtual Slice GetNamespaceId() const = 0;

  // The Topic name
  virtual Slice GetTopicName() const = 0;

  // The contents of the message
  virtual Slice GetContents() const = 0;

  virtual ~MessageReceived()  {}
};

/**
 * An object of the form scheme://host:port/path
 */
typedef std::string URL;

/**
 * A application generated identifier to represent a
 * RocketSpeed client. The RocketSpeed system uses this to
 * manage client's subscriptions and publications.
 */
typedef std::string ClientID;

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
  /**
   * The System Tenant ID is used for work that is done to keep the entire
   * RocketSpeed system up, alive and running well.
   */
  SystemTenant = 2,
};

/**
 * Indicates a sequence number which marks a begnning of subscription.
 * If present, the subscription will be started/resumed from given number.
 * If absent, the subscription will be resumed from last known sequence number,
 * according to specified storage strategy.
 */
class SubscriptionStart {
 public:
  /* implicit */ SubscriptionStart(SequenceNumber seqno)
      : seqno_(seqno), present_(true) {}

  SubscriptionStart() : seqno_(0), present_(false) {}

  explicit operator bool() const {
    return present_;
  }

  SequenceNumber get() const {
    assert(*this);
    return seqno_;
  }

 private:
  SequenceNumber seqno_;
  bool present_;
};

/**
 * Indicates which topic to subscribe, messages after the specified sequence
 * number will be delivered to the client.
 * A seqno of 0 indicates that the subscriber is interested in receiving
 * whatever data is available via this topic.
 * A seqno of 1 indicates that the subscriber is interested in receiving
 * all the data that was published to this topic.
 * An absent seqno indicates that subscription should be resumed based on
 * information from state storage.
 */
class SubscriptionRequest {
 public:
  NamespaceID namespace_id;
  Topic topic_name;
  bool subscribe;
  SubscriptionStart start;

  SubscriptionRequest(NamespaceID _namespace_id,
                      Topic _topic_name,
                      bool _subscribe,
                      SubscriptionStart _start)
      : namespace_id(std::move(_namespace_id)),
        topic_name(std::move(_topic_name)),
        subscribe(_subscribe),
        start(_start) {}

  SubscriptionRequest() {}
};

/**
 * A host:port pair that uniquely identifies a machine.
 */
class HostId {
 public:
  std::string hostname;      // name of a machine
  uint64_t    port;          // name of port to connect to

  HostId(std::string s, uint64_t p);
  HostId();

  bool operator<(const HostId& rhs) const;
  bool operator==(const HostId& rhs) const;

  // converts a HostId to a name:port string
  std::string ToString() const;

  // Converts an internal client ID to a HostId
  // The client ID must be formatted as <host>:<port><worker_id-byte>
  // e.g. "localhost:1234\x01"
  static HostId ToHostId(ClientID str);

  // converts a HostId to a ClientID
  ClientID ToClientId(int worker_id = 0) const;
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
   * @param num_workers Number of client worker threads.
   * @return A new Configuration with the specified options.
   */
  static Configuration* Create(const std::vector<HostId>& pilots,
                               const std::vector<HostId>& copilots,
                               TenantID tenant_id,
                               int num_workers = 4);

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
   * The number of client worker threads.
   */
  virtual int GetNumWorkers() const = 0;
};

enum Retention : char {
  OneHour = 0x01,         // keep messages for 1 hour
  OneDay = 0x02,          // keep messages for 1 day
  OneWeek = 0x03,         // keep messages for 1 week
  Total = 3,              // number of retention classes
};

/**
 * These are the options associated with publishing to a Topic.
 * These parameters can be message-specific compression type,
 * message-specific checksum, or could be hints (e.g. is this
 * message a json blob, etc.) that makes the system handle
 * messages more efficiently.
 */
class TopicOptions {
 public:
  TopicOptions() {}
};

}  // namespace rocketspeed
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
