/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "src/util/common/hash.h"

namespace rocketspeed {

class Slice;

/// Uniquely identifies a shard a particular subscription belongs to.
using ShardID = uint32_t;

/// Uniquely identifies a subscription in communication between subscriber (e.g.
/// a Client) and subscribee (e.g. a Rocketeer).
class SubscriptionID {
 public:
  /// Creates an ID from uint64_t representation, potentially violating any
  /// assertions about internal structure of the ID.
  static SubscriptionID Unsafe(uint64_t value) { return SubscriptionID(value); }

  /// Creates an ID out of a shard ID and an ID unique _within_ the shard.
  static SubscriptionID ForShard(ShardID shard_id, uint64_t hierarchical_id);

  /// Creates an invalid ID, that doesn't represent any subscription.
  constexpr SubscriptionID() noexcept : encoded_(0) {}

  /// Retrieves shard of the subscription.
  ShardID GetShardID() const;

  /// Retrieves shard of the subscription.
  uint64_t GetHierarchicalID() const;

  /* implicit */ operator uint64_t() const { return encoded_; }

  long long unsigned int ForLogging() const { return encoded_; }

  /// Returns whether this ID is valid.
  explicit operator bool() const { return encoded_ != 0; }

  bool operator==(SubscriptionID rhs) const { return encoded_ == rhs.encoded_; }
  bool operator!=(SubscriptionID rhs) const { return encoded_ != rhs.encoded_; }
  bool operator<(SubscriptionID rhs) const { return encoded_ < rhs.encoded_; }
  bool operator>(SubscriptionID rhs) const { return encoded_ > rhs.encoded_; }
  bool operator<=(SubscriptionID rhs) const { return encoded_ <= rhs.encoded_; }
  bool operator>=(SubscriptionID rhs) const { return encoded_ >= rhs.encoded_; }

 private:
  explicit SubscriptionID(uint64_t encoded) noexcept : encoded_(encoded) {}

  uint64_t encoded_;
};

static_assert(sizeof(SubscriptionID) == 8, "Invalid size");

template <>
struct MurmurHash2<rocketspeed::SubscriptionID> {
  size_t operator()(rocketspeed::SubscriptionID id) const {
    return rocketspeed::MurmurHash2<uint64_t>()(static_cast<uint64_t>(id));
  }
};

} // namespace rocketspeed

namespace std {

template <>
struct hash<rocketspeed::SubscriptionID> {
  size_t operator()(rocketspeed::SubscriptionID id) const {
    return rocketspeed::MurmurHash2<rocketspeed::SubscriptionID>()(id);
  }
};

}  // namespace std

namespace rocketspeed {

/// A thread-safe allocator for SubscriptionIDs.
///
/// When allocating a unique SubscriptionIDs, we consistently hash the
/// subscription's shard and worker thread into a set of allocators, and use
/// chosen allocator's next value as the seed for hierarchical ID.
///
/// This enables us to reduce a cost of ID allocation to a single atomic
/// increment, without assuming anything about the total number of shards in the
/// system.
///
/// In the proposed allocation scheme, the trade-off is between memory used for
/// the allocator and the number of allocations that can be performed. If all
/// allocations end up on the same worker and shard, changing the allocator size
/// (number of atomics) does not affect the lifetime.
///
/// To estimate the time before the allocator runs out of subscription IDs, let
/// us assume that:
/// * there are no more than 2 M shards (which fits in 3 byte VL-ecoded uint),
/// * the client uses 2^5 threads,
/// * the allocator size is 2^10.
///
/// We have 5 bytes left to encode the seed and thread ID, which leaves us with
/// over 2^35 unique seed values. If the number of distinct (thread, shard)
/// pairs is sufficiently large, we can perform 2^45 allocations before all
/// allocators overflow.
///
/// At a modest rate of 1 M new subscriptions being created every second, that
/// translates to a bit over a year of uninterrupted operation.
class SubscriptionIDAllocator {
 public:
  SubscriptionIDAllocator(size_t num_workers, size_t num_allocators)
  : num_workers_(num_workers), allocators_(num_allocators) {}

  SubscriptionID Next(ShardID shard_id, size_t worker_id);

  size_t GetWorkerID(SubscriptionID sub_id) const;

 private:
  const size_t num_workers_;

  // TODO(stupaq): would false sharing cause any observable problems?
  std::vector<std::atomic<uint64_t>> allocators_;
};

/// Appends serialized SubscriptionID to provided string.
void EncodeSubscriptionID(std::string* out, SubscriptionID in);

bool DecodeSubscriptionID(Slice* in, SubscriptionID* out);

}  // namespace rocketspeed

