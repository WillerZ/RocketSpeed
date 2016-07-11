/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <unordered_set>

#include "src/util/common/select_random.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

TEST(SelectRandom, Fairness) {
  std::unordered_set<size_t> set;
  const size_t num_coupons = 100000;
  size_t coupon_collection_attempts = 0;
  for (size_t i = 1; i <= num_coupons; ++i) {
    coupon_collection_attempts += (num_coupons + i - 1) / i;
    set.emplace(i);
  }
  std::unordered_set<size_t> not_selected = set;  // Copy.
  std::mt19937_64 prng;

  const size_t attempts = 2 * coupon_collection_attempts;
  for (size_t i = 0; i < attempts && !not_selected.empty(); ++i) {
    auto elem = SelectRandom(&set, &prng);
    not_selected.erase(elem);
  }
  ASSERT_EQ(0, not_selected.size());
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
