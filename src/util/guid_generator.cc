//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "guid_generator.h"
#include <array>
#include <algorithm>

namespace rocketspeed {

GUIDGenerator::GUIDGenerator() {
  static_assert(std::is_same<decltype(rng_()), uint64_t>::value,
    "std::mt19937_64 must return uint64_t");
  assert(rng_.min() == 0 &&
         rng_.max() == static_cast<uint64_t>(0) - static_cast<uint64_t>(1));

  // 2 = 64 bits / 32 bits; state_size is in 64-bit words
  std::array<int, 2 * std::mt19937_64::state_size> seed_data;
  std::random_device r;
  std::generate(seed_data.begin(), seed_data.end(), std::ref(r));
  std::seed_seq seq(seed_data.begin(), seed_data.end());
  rng_.seed(seq);
}

GUID GUIDGenerator::Generate() {
  union {
    GUID guid;
    uint64_t ui0;
    uint64_t ui1;
  } result;
  result.ui0 = rng_();
  result.ui1 = rng_();
  return result.guid;
}

}
