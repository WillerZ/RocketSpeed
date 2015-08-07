// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/random.h"

#include <algorithm>
#include "src/util/common/thread_local.h"

namespace rocketspeed {

std::mt19937_64& ThreadLocalPRNG() {
  static ThreadLocalObject<std::mt19937_64> prng(
    [] () {
      auto rng = new std::mt19937_64();
      std::array<int, 2 * std::mt19937_64::state_size> seed;
      std::random_device rd;
      std::generate(seed.begin(), seed.end(), std::ref(rd));
      std::seed_seq seq(seed.begin(), seed.end());
      rng->seed(seq);
      return rng;
    });
  return prng.GetThreadLocal();
}

}  // namespace rocketspeed
