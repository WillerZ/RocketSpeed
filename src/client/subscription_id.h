/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

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

  /// Creates an invalid ID, that doesn't represent any subscription.
  constexpr SubscriptionID() noexcept : encoded_(0) {}

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

/// Appends serialized SubscriptionID to provided string.
void EncodeSubscriptionID(std::string* out, SubscriptionID in);

bool DecodeSubscriptionID(Slice* in, SubscriptionID* out);

}  // namespace rocketspeed

namespace std {

template <>
struct hash<rocketspeed::SubscriptionID> {
  size_t operator()(rocketspeed::SubscriptionID id) const {
    return rocketspeed::MurmurHash2<rocketspeed::SubscriptionID>()(id);
  }
};
}  // namespace std
