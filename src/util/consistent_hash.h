// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <assert.h>
#include <math.h>
#include <algorithm>
#include <functional>
#include <map>
#include <utility>
#include "src/util/hash.h"

namespace rocketspeed {

/**
 * Stores a set of slots and provides a mapping from keys to slots. The mapping
 * will approximately send the same number of keys to each slot. Importantly,
 * a key will likely be mapped to the same slot even after adding or removing
 * other keys.
 *
 * Different weights can be applied to each slot by scaling the number of
 * replicas (more replicas = more weight).
 *
 * Even if varying weights are not desired, it is recommended to add replicas
 * for each node as it greatly improves the balance of the key mappings. The
 * trade-offs are in memory usage and performance.
 *
 * For n total replicas:
 *   Memory usage: O(n)
 *   Get time: O(log(n))
 *
 * For good slot allocation, the hash functions need to have good key
 * distribution. As a default, we use MurmurHash2 rather than std::hash since
 * std::hash has very poor key distribution for integral types.
 */
template <class Key,
          class Slot,
          class KeyHash = MurmurHash2<Key>,
          class SlotHash = MurmurHash2<Slot>>
class ConsistentHash {
 public:
  enum {
    kDefaultReplicaCount = 20
  };

  /**
   * Constructs a ConsistentHash object with given hash function.
   *
   * @param keyHash The hash function object for keys.
   * @param slotHash The hash function object for slots.
   */
  explicit ConsistentHash(const KeyHash& keyHash = KeyHash(),
                          const SlotHash& slotHash = SlotHash());

  /**
   * Adds a new slot to the mapping.
   *
   * @param slot The slot to add.
   * @param replicas The number of virtual slots to add. This number should be
   *        proportional to the weight of the slot. More replicas also means
   *        better key distribution, but at extra cost to Get.
   */
  void Add(const Slot& slot,
           unsigned int replicas = kDefaultReplicaCount);

  /**
   * Removes a slot from the mapping.
   *
   * @param slot The slot to remove.
   */
  void Remove(const Slot& slot);

  /**
   * Gets the slot that key is mapped to.
   *
   * @param key The key to get the mapping for.
   * @return slot The slot that the key is mapped to.
   */
  const Slot& Get(const Key& key) const;

  /**
   * The number of unique slots in the mapping.
   */
  size_t SlotCount() const;

  /**
   * The number of virtual slots in the mapping. This is equal to the
   * number of slots including replications.
   */
  size_t VirtualSlotCount() const;

  /**
   * Computes the ratio of keys mapped to a slot.
   *
   * @param slot The slot to check.
   * @return The fraction of keys mapped to slot.
   */
  double SlotRatio(const Slot& slot) const;

 private:
  // Hashing ring. Using a multimap here as multiple slots could have hash
  // collisions, and we don't want to override slots if that happens as it
  // would break the removal semantics.
  // e.g.
  // Add(X); // hash(X) == 42
  // Add(Y); // hash(Y) == 42 -- overrides X
  // Remove(Y); // now we have no slots, but should still have the X slot!
  std::multimap<size_t, Slot> ring_;
  KeyHash keyHash_;    // Hash function for keys
  SlotHash slotHash_;  // Hash function for slots
  size_t slotCount_;   // Number of unique slots
};

template <class Key, class Slot, class KeyHash, class SlotHash>
ConsistentHash<Key, Slot, KeyHash, SlotHash>::ConsistentHash(
    const KeyHash& keyHash,
    const SlotHash& slotHash)
: keyHash_(keyHash)
, slotHash_(slotHash)
, slotCount_(0) {
}

template <class Key, class Slot, class KeyHash, class SlotHash>
void ConsistentHash<Key, Slot, KeyHash, SlotHash>::Add(
    const Slot& slot,
    unsigned int replicas) {
  size_t hash = slotHash_(slot);
  while (replicas--) {
    ring_.insert(std::make_pair(hash, slot));
    hash = MurmurHash2<size_t>()(hash);
  }
  ++slotCount_;
}

template <class Key, class Slot, class KeyHash, class SlotHash>
void ConsistentHash<Key, Slot, KeyHash, SlotHash>::Remove(
    const Slot& slot) {
  size_t hash = slotHash_(slot);
  bool foundOne = false;
  for (;;) {
    // Might be multiple slots on the same hash value.
    // Need to find the one that maps to 'slot'.
    auto range = ring_.equal_range(hash);
    bool found = false;
    for (auto it = range.first; it != range.second; ) {
      if (it->second == slot) {
        found = true;
        ring_.erase(it);
        break;
      }
    }
    if (!found) {
      // None found for this slot at this hash, so done.
      break;
    }
    // There may be replicas, so get the next hash and keep trying.
    hash = MurmurHash2<size_t>()(hash);
    foundOne = true;
  }
  if (foundOne) {
    --slotCount_;
  }
}

template <class Key, class Slot, class KeyHash, class SlotHash>
const Slot& ConsistentHash<Key, Slot, KeyHash, SlotHash>::Get(
    const Key& key) const {
  assert(!ring_.empty());
  size_t hash = keyHash_(key);
  auto it = ring_.lower_bound(hash);
  if (it == ring_.end()) {
    it = ring_.begin();  // Wrap back to first node.
  }
  return it->second;
}

template <class Key, class Slot, class KeyHash, class SlotHash>
size_t ConsistentHash<Key, Slot, KeyHash, SlotHash>::SlotCount() const {
  return slotCount_;
}

template <class Key, class Slot, class KeyHash, class SlotHash>
size_t ConsistentHash<Key, Slot, KeyHash, SlotHash>::VirtualSlotCount() const {
  return ring_.size();
}

template <class Key, class Slot, class KeyHash, class SlotHash>
double ConsistentHash<Key, Slot, KeyHash, SlotHash>::SlotRatio(
    const Slot& slot) const {
  auto it = ring_.end();
  if (it == ring_.begin()) {
    return 0.0;
  }
  --it;
  size_t prevHash = it->first;
  size_t count = 0;
  bool found = false;
  for (it = ring_.begin(); it != ring_.end(); ++it) {
    if (it->second == slot) {
      count += it->first - prevHash;
      found = true;
    }
    prevHash = it->first;
  }
  if (count == 0) {
    return found ? 1.0 : 0.0;
  } else {
    return static_cast<double>(count) / pow(2.0, sizeof(size_t) * 8);
  }
}

}  // namespace rocketspeed
