// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstring>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

#include "include/Status.h"
#include "include/Slice.h"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif

namespace rocketspeed {

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
 * The System Namespace ID is used by the RocketSpeed system internally
 * to store various transient pieces of metadata. It is possible that
 * data stored in this namespace automatically get purged out after
 * some configured period of time.
 */
extern const NamespaceID SytemNamespaceTransient;

/**
 * The System Namespace ID is used by the RocketSpeed system internally
 * to store various permanent pieces of metadata.
 */
extern const NamespaceID SytemNamespacePermanent;

/**
 * Each Topic is a string
 */
typedef std::string Topic;

/**
 * Each message has a sequence number associated with it.
 * A newly produced message has a higher sequence number than
 * a message produced earlier for the same topic.
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

  GUID() : hi(0), lo(0) {}

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
 * A host:port pair that uniquely identifies a machine.
 */
class HostId {
 public:
  std::string hostname;      // name of a machine
  uint64_t    port;          // port number to connect to

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
 */
class Configuration {
 public:
  virtual ~Configuration() {}

  /**
   * Get a pilot host ID to use for publishes.
   *
   * @param pilot_out If ok(), will be set to pilot host ID to use.
   * @return ok() if successful, otherwise error.
   */
  virtual Status GetPilot(HostId* pilot_out) const = 0;

  /**
   * Get a copilot host ID to use for subscriptions.
   *
   * @param copilot_out If ok(), will be set to copilot host ID to use.
   * @return ok() if successful, otherwise error.
   */
  virtual Status GetCopilot(HostId* copilot_out) const = 0;
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

/**
 * Identifies a single subscription. A null-handle does not correspond to any
 * subscription.
 */
typedef uint64_t SubscriptionHandle;

/**
 * Describes parameters of a subscription persisted by the client.
 * After receiving a list of restored subscriptions, the application can reissue
 * corresponding subscription requests by providing subscription parameters back
 * to the client together with appropriate callbacks.
 */
class SubscriptionParameters {
 public:
  TenantID tenant_id;
  NamespaceID namespace_id;
  Topic topic_name;
  SequenceNumber start_seqno;

  SubscriptionParameters() = default;

  SubscriptionParameters(TenantID _tenant_id,
                         NamespaceID _namespace_id,
                         Topic _topic_name,
                         SequenceNumber _start_seqno)
      : tenant_id(_tenant_id)
      , namespace_id(std::move(_namespace_id))
      , topic_name(std::move(_topic_name))
      , start_seqno(_start_seqno) {}

  bool operator==(const SubscriptionParameters& other) const {
    return tenant_id == other.tenant_id && start_seqno == other.start_seqno &&
           namespace_id == other.namespace_id && topic_name == other.topic_name;
  }

  bool operator!=(const SubscriptionParameters& other) const {
    return !(*this == other);
  }
};

/** Status of a subscription requested by the application. */
class SubscriptionStatus {
 public:
  /** The tenant this subscription was created for. */
  virtual TenantID GetTenant() const = 0;

  /** The namespace of the topic. */
  virtual const NamespaceID& GetNamespace() const = 0;

  /** The topic name this message arrived on. */
  virtual const Topic& GetTopicName() const = 0;

  /** Current sequence number of the subscription. */
  virtual SequenceNumber GetSequenceNumber() const = 0;

  /** True iff the subscription is active after the callback. */
  virtual bool IsSubscribed() const = 0;

  /** The reason of this notification. */
  virtual const Status& GetStatus() const = 0;

  virtual ~SubscriptionStatus() {}
};

/** Message received on a subscription. */
class MessageReceived {
 public:
  /** Returns handle identifying subscription that this message arrived on. */
  virtual SubscriptionHandle GetSubscriptionHandle() const = 0;

  // The sequence number of this message
  virtual SequenceNumber GetSequenceNumber() const = 0;

  // The namespace id of this message
  virtual Slice GetNamespaceId() const = 0;

  // The Topic name
  virtual Slice GetTopicName() const = 0;

  // The contents of the message
  virtual Slice GetContents() const = 0;

  virtual ~MessageReceived() {}
};

}  // namespace rocketspeed

namespace std {
  template <>
  struct hash<rocketspeed::HostId> {
    size_t operator()(const rocketspeed::HostId& host_id) const;
  };
} // namespace std

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
