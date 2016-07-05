/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/util/common/subscription_id.h"

#include <limits>
#include <string>

#include "include/Assert.h"
#include "include/Slice.h"
#include "src/util/common/coding.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

SubscriptionID SubscriptionID::ForShard(ShardID shard_id,
                                        uint64_t hierarchical_id) {
  // Note that implementation of this method affects on-wire format.

  // Internally, the SubscriptionID::encoded_ is composed of:
  // * between 1 and 5 bytes that encode shard ID using VLE,
  // * the remaining bytes that encode hierarchical ID in LSB-first format,
  //   stripping trailing null bytes.

  char buffer[4 + 1 + 8] = {0};
  EncodeFixed64(EncodeVarint32(buffer, shard_id), hierarchical_id);

  // If any of the bytes after the 8-th one is non-zero, then we cannot encode
  // shard + hierarchical ID in 8 bytes.
  for (const char *ptr = buffer + sizeof(uint64_t),
                  *const end = buffer + sizeof(buffer);
       ptr != end;
       ++ptr) {
    if (*ptr) {
      // Cannot fit in 8 bytes.
      return SubscriptionID();
    }
  }

  uint64_t encoded;
  memcpy((char*)&encoded, buffer, sizeof(encoded));
  SubscriptionID sub_id(encoded);
  RS_ASSERT(sub_id);
  RS_ASSERT(shard_id == sub_id.GetShardID());
  RS_ASSERT(hierarchical_id == sub_id.GetHierarchicalID());
  return sub_id;
}

ShardID SubscriptionID::GetShardID() const {
  RS_ASSERT(*this);

  Slice in((char*)&encoded_, sizeof(encoded_));
  ShardID shard_id;
  if (!GetVarint32(&in, &shard_id)) {
    RS_ASSERT(false);
    return std::numeric_limits<ShardID>::max();
  }
  return shard_id;
}

uint64_t SubscriptionID::GetHierarchicalID() const {
  RS_ASSERT(*this);

  char buffer[4 + 1 + 8] = {0};
  memcpy(buffer, (char*)&encoded_, sizeof(encoded_));

  Slice in(buffer, sizeof(buffer));
  ShardID shard_id;
  if (!GetVarint32(&in, &shard_id)) {
    RS_ASSERT(false);
    return 0;
  }

  uint64_t hierarchical_id;
  if (!GetFixed64(&in, &hierarchical_id)) {
    RS_ASSERT(false);
    return 0;
  }
  return hierarchical_id;
}

SubscriptionID SubscriptionIDAllocator::Next(const ShardID shard_id,
                                             const size_t worker_id) {
  RS_ASSERT(worker_id < num_workers_);
  const size_t allocator_index =
      MurmurHash2<size_t, size_t>()(shard_id, worker_id) % allocators_.size();
  const uint64_t seed = allocators_[allocator_index]++;
  const uint64_t hierarchical_id = 1 + worker_id + num_workers_ * seed;
  {  // The expression above overflows only if one of the following does.
    const uint64_t a = seed + 1;
    if (a == 0) {
      return SubscriptionID();
    }
    const uint64_t b = num_workers_ * a;
    if (b / a != num_workers_) {
      return SubscriptionID();
    }
  }
  return SubscriptionID::ForShard(shard_id, hierarchical_id);
}

size_t SubscriptionIDAllocator::GetWorkerID(SubscriptionID sub_id) const {
  RS_ASSERT(sub_id);
  const uint64_t hierarchical_id = sub_id.GetHierarchicalID();
  if (hierarchical_id == 0) {
    return num_workers_;
  }
  const size_t worker_id = (hierarchical_id - 1) % num_workers_;
  return worker_id;
}

void EncodeSubscriptionID(std::string* out, SubscriptionID in) {
  PutVarint64(out, static_cast<uint64_t>(in));
}

bool DecodeSubscriptionID(Slice* in, SubscriptionID* out) {
  uint64_t value;
  if (!GetVarint64(in, &value)) {
    return false;
  }
  *out = SubscriptionID::Unsafe(value);
  return true;
}

}  // namespace rocketspeed
