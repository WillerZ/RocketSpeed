// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "Assert.h"
#include "Status.h"
#include "Slice.h"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif

namespace rocketspeed {

class HostId;
class Logger;

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
bool IsReserved(const Slice& ns);

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
 * Each message delivered to a client belongs to a single data source. Messages
 * are uniquely identified by a (data source, sequence number) pair. Messages
 * on a particular topic may arrive from more than one data source. In this
 * case, the topic is partially ordered, though messages from a particular
 * data source will be received in-order. Totally ordered topics will receive
 * messages only from a single data source.
 */
using DataSource = std::string;

// DEPRECATED
using Epoch = std::string;

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
 * An interface that encapsulates sharding logic.
 */
class ShardingStrategy {
 public:
  static Status Create(const std::shared_ptr<Logger>& info_log,
                       const std::string& config_str,
                       std::unique_ptr<ShardingStrategy>* out);

  static Status Create(const std::shared_ptr<Logger>& info_log,
                       const std::string& config_str,
                       std::shared_ptr<ShardingStrategy>* out) {
    std::unique_ptr<ShardingStrategy> sharding;
    Status st = Create(info_log, config_str, &sharding);
    if (st.ok()) {
      *out = std::move(sharding);
    }
    return st;
  }

  virtual ~ShardingStrategy() = default;

  /**
   * Returns a shard ID for given namespace and topic.
   * The total number of shards can grow over time, and the Client should make
   * no assumptions about it.
   */
  virtual size_t GetShard(Slice namespace_id, Slice topic_name) const = 0;

  /**
   * Returns a version of the strategy, which can spontaneously increase.
   * A version change could mean that the host selected by the strategy on one
   * or more shards may have changed.
   * Calling this method should be cheap, uncontended atomic is the heaviest
   * acceptable implementation.
   */
  virtual size_t GetVersion() = 0;

  /**
   * Returns the currently selected host for a particular shard.
   * This method can acquire a mutex or perform other heavy synchronisation, but
   * no IO.
   */
  virtual HostId GetHost(size_t shard) = 0;

  /**
   * Tell the strategy that we could not connect to provided host.
   * This method can acquire a mutex or perform other heavy synchronisation, but
   * no IO.
   */
  virtual void MarkHostDown(const HostId& host_id) = 0;
};

/**
 * Encapsulates thread selection logic.
 */
using ThreadSelectionStrategy = std::function<size_t(size_t, Slice, Slice)>;

/**
 * A PublisherRouter that specifies how a Client can connect to RocketSpeed.
 */
class PublisherRouter {
 public:
  /**
   * Factory method for creating PublisherRouter from human-readable string.
   */
  static Status Create(const std::shared_ptr<Logger>& info_log,
                       const std::string& config_str,
                       std::unique_ptr<PublisherRouter>* out);

  static Status Create(const std::shared_ptr<Logger>& info_log,
                       const std::string& config_str,
                       std::shared_ptr<PublisherRouter>* out) {
    std::unique_ptr<PublisherRouter> publisher;
    Status st = Create(info_log, config_str, &publisher);
    if (st.ok()) {
      *out = std::move(publisher);
    }
    return st;
  }

  virtual ~PublisherRouter() {}

  /**
   * Get a pilot host ID to use for publishes.
   *
   * @param pilot_out If ok(), will be set to pilot host ID to use.
   * @return ok() if successful, otherwise error.
   */
  virtual Status GetPilot(HostId* pilot_out) const = 0;
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

/**
 * Describes parameters of a subscription to be traced by hooks.
 * Similar to SubscriptionParameters but doesn't require SequenceNumber.
 */
struct HooksParameters {
  TenantID tenant_id;
  NamespaceID namespace_id;
  Topic topic_name;

  explicit HooksParameters(TenantID tid, NamespaceID nid, Topic topic)
      : tenant_id(tid), namespace_id(nid), topic_name(topic) {}

  explicit HooksParameters(const SubscriptionParameters& p)
      : tenant_id(p.tenant_id),
        namespace_id(p.namespace_id),
        topic_name(p.topic_name) {}

  bool operator==(const HooksParameters& other) const {
    return tenant_id == other.tenant_id && namespace_id == other.namespace_id &&
           topic_name == other.topic_name;
  }

  bool operator!=(const HooksParameters& params) const {
    return !operator==(params);
  }
};

}  // namespace rocketspeed

namespace std {
template <>
struct hash<rocketspeed::HooksParameters> {
  size_t operator()(const rocketspeed::HooksParameters& params) const {
    return std::hash<rocketspeed::Topic>()(params.topic_name);
  }
};

}  // namespace std

namespace rocketspeed {

/** Status of a subscription requested by the application. */
class SubscriptionStatus {
 public:
  /** A handle that identifies the subscription in question. */
  virtual SubscriptionHandle GetSubscriptionHandle() const = 0;

  /** The tenant this subscription was created for. */
  virtual TenantID GetTenant() const = 0;

  /** The namespace of the topic. */
  virtual const NamespaceID& GetNamespace() const = 0;

  /** The topic name this message arrived on. */
  virtual const Topic& GetTopicName() const = 0;

  /** The reason of this notification. */
  virtual const Status& GetStatus() const = 0;

  virtual ~SubscriptionStatus() {}
};

/**
 * Extended version of SubscriptionStatus
 * required by API hooks (see ApiHooks.h).
 */
class HookedSubscriptionStatus : public SubscriptionStatus {
 public:
  /** List of servers that handle or are about to handle the subscription. */
  virtual const std::vector<HostId>& GetCurrentServers() const = 0;
};


/** Message received on a subscription. */
class MessageReceived {
 public:
  /** The handle identifying subscription that this message arrived on. */
  virtual SubscriptionHandle GetSubscriptionHandle() const = 0;

  /** The sequence number of this message. */
  virtual SequenceNumber GetSequenceNumber() const = 0;

  /** The contents of the message. */
  virtual Slice GetContents() const = 0;

  virtual ~MessageReceived() {}
};

enum class DataLossType : char {
  kDataLoss,  // Catastrophic failure, acknowledged data was lost.
  kRetention  // Retention period expired.
};

/** Contains information about data loss. */
class DataLossInfo {
 public:
  /** The handle identifying subscription that this message arrived on. */
  virtual SubscriptionHandle GetSubscriptionHandle() const = 0;

  /** The type of data loss that occurred */
  virtual DataLossType GetLossType() const = 0;

  /** Gets the first sequence number (inclusive) of the data loss. */
  virtual SequenceNumber GetFirstSequenceNumber() const = 0;

  /** Gets the last sequence number (inclusive) of the data loss. */
  virtual SequenceNumber GetLastSequenceNumber() const = 0;

  virtual ~DataLossInfo() {}
};

/**
 * Response for HasMessageSince query.
 */
enum class HasMessageSinceResult : uint8_t {
  /// The request failed to complete.
  kInvalid = 0,

  /// There are no messages in the requested range.
  kNo = 1,

  /// There definitely was a message in the requested range.
  kYes = 2,

  /// There maybe was a message in the requested range. This may be sent if
  /// the server has missing or approximate information.
  kMaybe = 3,

  kMin = kInvalid,
  kMax = kMaybe,
};

/** Visitor for RocketSpeed statistics. */
class StatisticsVisitor {
 public:
  /**
   * Called for each histogram with each percentile (50, 90, 99, 99.9).
   *
   * @param name Name of the histogram stats, e.g. "latency.p50"
   * @param value The value of that percentile
   */
  virtual void VisitHistogram(const std::string& name, double value) {}

  /**
   * Called for each counter.
   *
   * @param name Name of the counter.
   * @param value The count.
   */
  virtual void VisitCounter(const std::string& name, int64_t value) {}

  /**
   * Called after a batch of statistics have been exported.
   */
  virtual void Flush() {}

  virtual ~StatisticsVisitor() {}
};

/// Type for introduction property key
using IntroPropertyKey = std::string;
/// Type for value of an introduction property
using IntroPropertyValue = std::string;

/**
 * Map of Key:Value properties to send to the server while creating a stream
 * to the server.
 * The properties are defined by the application and are known to both client
 * and server for parsing.
 */
using IntroProperties =
    std::unordered_map<IntroPropertyKey, IntroPropertyValue>;

/// Introduction parameters of a stream
struct IntroParameters {
  explicit IntroParameters(TenantID _tenant_id,
                           IntroProperties _stream_properties,
                           IntroProperties _client_properties)
  : tenant_id(_tenant_id)
  , stream_properties(std::move(_stream_properties))
  , client_properties(std::move(_client_properties)) {}

  IntroParameters() = default;

  // TenantID of the stream
  TenantID tenant_id = GuestTenant;

  // Properties for the stream
  IntroProperties stream_properties;

  // Properties for the client
  IntroProperties client_properties;
};

/**
 * Tests if a IntroPropertyKey is reserved.
 *
 * @param key the name of the property key.
 * @return true iff the IntroPropertyKey is reserved
 */
bool IsPropertyReserved(const IntroPropertyKey& key);

/// Default key for shard_id of the stream
extern const IntroPropertyKey PropertyShardID;

/// Default key for hostname of the client
extern const IntroPropertyKey PropertyHostName;

}  // namespace rocketspeed

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
