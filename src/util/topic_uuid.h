// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Types.h"

namespace rocketspeed {

/**
 * Unique identifier for namespace + topic.
 */
struct TopicUUID {
 public:
  /**
   * Construct a TopicUUID from a namespace and topic.
   *
   * @param namespace_id The namespace of the topic.
   * @param topic The name of the topic.
   */
  TopicUUID(Slice namespace_id, Slice topic);

  /**
   * @return True iff UUIDs are equal.
   */
  bool operator==(const TopicUUID& rhs) const {
    return uuid_ == rhs.uuid_;
  }

  /**
   * @return Hash of the UUID, suitable for using in general hash tables.
   */
  size_t Hash() const;

  /**
   * @return Hash that should be used for routing to logs / control towers.
   */
  size_t RoutingHash() const;

  /**
   * Equivalent to TopicUUID(namespace_id, topic).RoutingHash(), but
   * potentially faster.
   *
   * @param namespace_id namespace of topic to route.
   * @param topic_name name of topic to route.
   * @return Hash that should be used for routing to logs / control towers.
   */
  static size_t RoutingHash(Slice namespace_id, Slice topic_name);

 private:
  std::string uuid_;
  size_t routing_hash_;
};

}  // namespace rocketspeed

namespace std {
  template <>
  struct hash<rocketspeed::TopicUUID> {
    size_t operator()(const rocketspeed::TopicUUID& topic) const {
      return topic.Hash();
    }
  };
}  // namespace std
