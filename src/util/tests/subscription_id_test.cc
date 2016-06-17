/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.

#include <random>

#include "src/util/common/random.h"
#include "src/util/common/subscription_id.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class SubscriptionIDTest : public ::testing::Test {};

TEST_F(SubscriptionIDTest, Random) {
  auto& rng = ThreadLocalPRNG();
  std::uniform_int_distribution<uint32_t> shard_dist;
  std::uniform_int_distribution<uint32_t> hierarchical_dist;
  for (size_t i = 0; i < 100;) {
    auto shard_id = shard_dist(rng);
    auto hierarchical_id = hierarchical_dist(rng);
    auto sub_id = SubscriptionID::ForShard(shard_id, hierarchical_id);
    if (!sub_id) {
      // Do not count towards the number of iterations.
      continue;
    }
    ASSERT_EQ(shard_id, sub_id.GetShardID());
    ASSERT_EQ(hierarchical_id, sub_id.GetHierarchicalID());
    ++i;
  }
}

TEST_F(SubscriptionIDTest, Packing) {
#define CHECK(a, b, p)                                                 \
  do {                                                                 \
    uint32_t shard_id = (1 << (a)) - 1;                                \
    uint64_t hierarchical_id = (1ULL << (b)) - 1;                      \
    auto sub_id = SubscriptionID::ForShard(shard_id, hierarchical_id); \
    if (p) {                                                           \
      ASSERT_TRUE(sub_id);                                             \
      ASSERT_EQ(shard_id, sub_id.GetShardID());                        \
      ASSERT_EQ(hierarchical_id, sub_id.GetHierarchicalID());          \
    } else {                                                           \
      ASSERT_TRUE(!sub_id);                                            \
    }                                                                  \
  } while (false);
#define YCHECK(a, b) CHECK(a, b, true)
#define NCHECK(a, b) CHECK(a, b, false)

  YCHECK(0, 56);
  YCHECK(7, 56);
  NCHECK(7, 57);
  NCHECK(8, 56);

  YCHECK(8, 48);
  YCHECK(14, 48);
  NCHECK(14, 49);
  NCHECK(15, 48);

  YCHECK(14, 40);
  YCHECK(21, 40);
  NCHECK(21, 41);
  NCHECK(22, 40);

#undef NCHECK
#undef YCHECK
#undef CHECK
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
