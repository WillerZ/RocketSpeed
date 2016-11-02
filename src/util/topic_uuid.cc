// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/topic_uuid.h"
#include "src/util/common/coding.h"
#include "external/xxhash/xxhash.h"

namespace rocketspeed {

TopicUUID::TopicUUID(Slice namespace_id, Slice topic) {
  PutTopicID(&uuid_, namespace_id, topic);
  routing_hash_ = RoutingHash(namespace_id, topic);
}

size_t TopicUUID::Hash() const {
  return routing_hash_;
}

size_t TopicUUID::RoutingHash() const {
  return routing_hash_;
}

void TopicUUID::GetTopicID(Slice* namespace_id, Slice* topic_name) const {
  RS_ASSERT(namespace_id);
  RS_ASSERT(topic_name);
  Slice in(uuid_);
  if (!rocketspeed::GetTopicID(&in, namespace_id, topic_name)) {
    RS_ASSERT(false);
  }
}

std::string TopicUUID::ToString() const {
  Slice namespace_id;
  Slice topic_name;
  GetTopicID(&namespace_id, &topic_name);
  return "Topic(" + namespace_id.ToString() + "," + topic_name.ToString() + ")";
}

size_t TopicUUID::RoutingHash(Slice namespace_id, Slice topic_name) {
  // *******************************************************************
  // * WARNING: changing this hash will redistribute topics into logs. *
  // *******************************************************************
  const uint64_t seed = 0x9ee8fcef51dbffe8;
  XXH64_state_t state;
  XXH64_reset(&state, seed);
  XXH64_update(&state, namespace_id.data(), namespace_id.size());
  XXH64_update(&state, topic_name.data(), topic_name.size());
  return XXH64_digest(&state);
}

}  // namespace rocketspeed
