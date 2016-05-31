// Copyright (c) 2016, Facebook, Inc.  All rights reserved.

// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "load_balancer.h"

#include <iterator>

#include "include/Assert.h"

namespace rocketspeed {

bool PowerOfTwoLoadBalancer::AddShard(ShardId shardId) {
  return shards_.insert(shardId).second;
}

bool PowerOfTwoLoadBalancer::RemoveShard(ShardId shardId) {
  return shards_.erase(shardId);
}

LoadBalancer::ShardId PowerOfTwoLoadBalancer::GetPreferredShard() {
  auto max_rand_idx = static_cast<int>(shards_.size() - 1);
  using DistanceT = decltype(shards_)::iterator::difference_type;
  std::uniform_int_distribution<DistanceT> distr(0, max_rand_idx);
  auto rand_it1 = shards_.begin();
  std::advance(rand_it1, distr(rng_));
  auto rand_it2 = shards_.begin();
  std::advance(rand_it2, distr(rng_));
  auto shard1 = *rand_it1;
  auto shard2 = *rand_it2;
  return get_load_(shard1) < get_load_(shard2) ? shard1 : shard2;
}
}
