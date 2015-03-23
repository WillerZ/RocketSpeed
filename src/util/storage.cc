// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/storage.h"
#include "src/util/xxhash.h"

namespace rocketspeed {

size_t LogRouter::TopicHash(Slice namespace_id, Slice topic_name) {
  const uint64_t seed = 0x9ee8fcef51dbffe8;
  XXH64_state_t state;
  XXH64_reset(&state, seed);
  XXH64_update(&state, namespace_id.data(), namespace_id.size());
  XXH64_update(&state, topic_name.data(), topic_name.size());
  return XXH64_digest(&state);
}

}
