//  Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include "src/messages/load_balancer.h"

#include <map>

#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class PowerOfTwoLoadBalancerTest : public ::testing::Test {
 public:
  PowerOfTwoLoadBalancerTest() : timeout_(std::chrono::seconds(5)) {}

 protected:
  const std::chrono::seconds timeout_;
};

using ShardId = LoadBalancer::ShardId;
using LoadT = LoadBalancer::LoadT;

TEST_F(PowerOfTwoLoadBalancerTest, FairnessTest) {
  auto always_zero = [](ShardId shard_id) {
    return 0;
  };
  PowerOfTwoLoadBalancer lb(always_zero);
  size_t num_test_shards = 10;
  size_t num_requests = 5000;
  for (size_t id=0; id < num_test_shards; ++id) {
    lb.AddShard(id);
  }
  std::map<ShardId, unsigned> shard_counts;
  for (size_t idx=0; idx < num_requests; ++idx) {
    auto shard = lb.GetPreferredShard();
    shard_counts[shard]++;
  }
  // Test the distribution over selected shards
  // looks like the unfiorm distribution (i.e, we break ties fairly)
  double log2 = log(2.0);
  double expected_entropy = log(static_cast<double>(num_test_shards)) / log2;
  double emprirical_entropy = 0.0;
  for (auto& entry: shard_counts) {
      double count = static_cast<double>(entry.second);
      double prob = count / static_cast<double>(num_requests);
      emprirical_entropy += -(prob * log(prob) / log2);
  }
  double diff = fabs(emprirical_entropy - expected_entropy);
  ASSERT_LT(diff, 0.01);
}

TEST_F(PowerOfTwoLoadBalancerTest, MaxLoadTest) {
  // run a simulation where tasks take two time steps
  // to finish and load is number of items assigned
  // in last two steps. max_load should be 1.0
  // (tasks assigned to different shards)
  // with large # of shards with full O(n) load check,
  // and you should be close to that....
  const int num_steps = 1000;
  int cur_step = 0;
  const int num_shards = 1000;
  int num_tasks_per_step = 5;
  int assigned_counts[num_shards][num_steps] = {};
  auto get_load = [&](ShardId shard_id) {
    auto assigned = assigned_counts[shard_id];
    return static_cast<LoadT>(
      cur_step < 2 ? 0 : assigned[cur_step-1] + assigned[cur_step-2]
    );
  };
  PowerOfTwoLoadBalancer lb(get_load);
  for (int id = 0; id < num_shards; ++id) {
    lb.AddShard(id);
  }
  double sum_max_load = 0.0;
  for (cur_step = 0; cur_step < num_steps; ++cur_step) {
    for (int task = 0; task < num_tasks_per_step; ++task) {
      auto shard_id = lb.GetPreferredShard();
      assigned_counts[shard_id][cur_step]++;
    }
    LoadT max_load = std::numeric_limits<LoadT>::min();
    for (int shard_id = 0; shard_id < num_shards; ++shard_id) {
      max_load = std::max(max_load, get_load(shard_id));
    }
    sum_max_load += static_cast<double>(max_load);
  }
  double avg_max_load = sum_max_load / static_cast<double>(num_steps);
  double diff = fabs(avg_max_load - 1.0);
  ASSERT_TRUE(diff < 0.1);
}

}

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
