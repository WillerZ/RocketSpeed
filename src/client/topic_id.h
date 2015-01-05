// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <string>
#include <utility>

#include "include/Types.h"

namespace rocketspeed {

/**
 * Uniquely identifies topic (across all topics in all namespaces).
 */
struct TopicID {
  NamespaceID namespace_id;
  Topic topic_name;

  TopicID() {}

  TopicID(NamespaceID _namespace_id, Topic _topic_name)
      : namespace_id(_namespace_id), topic_name(std::move(_topic_name)) {}

  bool operator==(const TopicID& other) const {
    return namespace_id == other.namespace_id && topic_name == other.topic_name;
  }

  bool operator<(const TopicID& other) const {
    return namespace_id < other.namespace_id ||
           (namespace_id == other.namespace_id &&
            topic_name < other.topic_name);
  }
};

}  // namespace rocketspeed

namespace std {

template <>
struct hash<rocketspeed::TopicID> {
  std::size_t operator()(const rocketspeed::TopicID& id) const {
    // Based on boost::hash_combine
    // http://www.boost.org/doc/libs/1_56_0/boost/functional/hash/hash.hpp
    size_t seed = std::hash<decltype(id.namespace_id)>()(id.namespace_id);
    seed ^= std::hash<decltype(id.topic_name)>()(id.topic_name) + 0x9e3779b9 +
            (seed << 6) + (seed >> 2);
    return seed;
  }
};

}  // namespace std
