/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.

#include <random>

#include "src/util/common/random.h"
#include "src/util/common/subscription_id.h"
#include "src/util/testharness.h"

namespace rocketspeed {

TEST(SubscriptionIDTest, Random) {
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

TEST(SubscriptionIDTest, Packing) {
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

TEST(SubscriptionIDAllocatorTest, Random) {
  auto& rng = ThreadLocalPRNG();
  std::uniform_int_distribution<uint32_t> shard_dist(0, 31337);
  const size_t num_workers = 11;
  std::uniform_int_distribution<uint32_t> worker_dist(0, num_workers - 1);
  const size_t allocator_size = 100;
  SubscriptionIDAllocator allocator(num_workers, allocator_size);
  const size_t num_attempts = 3 * 100;
  for (size_t i = 0; i < num_attempts; ++i) {
    auto shard_id = shard_dist(rng);
    auto worker_id = worker_dist(rng);
    auto sub_id = allocator.Next(shard_id, worker_id);
    ASSERT_TRUE(sub_id);
    ASSERT_EQ(shard_id, sub_id.GetShardID());
    ASSERT_EQ(worker_id, allocator.GetWorkerID(sub_id));
    // Find out what was the seed at the time of allocation.
    auto seed = (sub_id.GetHierarchicalID() - 1 - worker_id) / num_workers;
    // Poisson CDF for mean = 3 and x >= 30 is < 10^-10.
    ASSERT_LE(seed, 30);
  }
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
