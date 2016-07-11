/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <iterator>
#include <random>

#include "include/Assert.h"

namespace rocketspeed {

/**
 * Select a random element of a hashtable-based container, such as unordered_map
 * or unordered_set.
 */
template <typename Hashtable, typename Rng>
const typename Hashtable::value_type& SelectRandom(Hashtable* hashtable,
                                                   Rng* rng) {
  RS_ASSERT(!hashtable->empty());

  {  // Adjust the load factor of a hash map, if necessary. This is amortised by
     // insertions and removals from the hashtable.
    size_t buckets = hashtable->bucket_count();
    if (buckets > 32) {
      size_t size = hashtable->size();
      // Inverse of a min load factor -- dictated by resizing policy.
      size_t min_load_factor_inv = 3;
      if (buckets > min_load_factor_inv * size) {
        buckets = ((min_load_factor_inv + 1) * size) / 2;
        // TODO: use Mersenne number
        hashtable->rehash(buckets);
      }
    }
  }

  // Find a random callback.
  const size_t buckets = hashtable->bucket_count();
  size_t bucket_index =
      std::uniform_int_distribution<size_t>(0, buckets - 1)(*rng);
  // Find non-empty bucket.
  while (hashtable->bucket_size(bucket_index) == 0) {
    bucket_index = (bucket_index + 1) % buckets;
    // This loop's running time is constant on average and under logarithmic
    // WHP, which is the same as in the approach where we reselect a random
    // index in every iteration. This approach is more efficient, as we stride
    // over a contiguous region of memory.
  }
  const size_t bucket_size = hashtable->bucket_size(bucket_index);
  RS_ASSERT(bucket_size > 0);
  // Select a random element from a bucket.
  if (bucket_size == 1) {
    return *hashtable->begin(bucket_index);
  } else {
    auto it = hashtable->begin(bucket_index);
    size_t bucket_offset =
        std::uniform_int_distribution<size_t>(0, bucket_size - 1)(*rng);
    // Expected complexity is constant, under logarithmic WHP.
    std::advance(it, bucket_offset);
    return *it;
  }
}

}  // namespace rocketspeed
