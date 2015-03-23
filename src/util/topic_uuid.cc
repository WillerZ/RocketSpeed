// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/topic_uuid.h"
#include "src/util/common/coding.h"
#include "src/util/xxhash.h"

namespace rocketspeed {

TopicUUID::TopicUUID(Slice namespace_id, Slice topic) {
  PutTopicID(&uuid_, namespace_id, topic);
}

size_t TopicUUID::Hash() const {
  const uint64_t seed = 0x91bef3a00e490a92;
  return XXH64(uuid_.data(), uuid_.size(), seed);
}

}  // namespace rocketspeed
