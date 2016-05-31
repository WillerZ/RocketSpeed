// Copyright (c) 2016, Facebook, Inc.  All rights reserved.

// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <random>
#include <functional>
#include <set>

#include "src/util/common/random.h"

namespace rocketspeed {
/**
 * LoadBalancer interface class.
 *
 * Primary method is <code>GetPreferredShard</code>
 * which will return a shard id for a new job. You must <code>AddShard</code>
 * to add it to the load balancer and <code>RemoveShard</code> when you no
 * longer want it in the balancer.
 *
 */
class LoadBalancer {
 public:
  using ShardId = uint64_t;
  using LoadT = uint64_t;
  /**
  * Add a shard to the load balancer. Defaults load to 0.
  * If you add an existing shard, no changes will happen.
  * @returns true if this is a new shard and false otherwise
  */
  virtual bool AddShard(ShardId shardId) = 0;

  /**
  * Remove a shard from the load balancer.
  * @returns true if a shard was existing and removed, false otherwise
  */
  virtual bool RemoveShard(ShardId shardId) = 0;

  /**
   * @returns Shard id for current job.
   */
  virtual ShardId GetPreferredShard() = 0;

  virtual ~LoadBalancer() = default;
};

/**
 * PowerOfTwoLoadBalancer
 *
 * Implements the <code>GetPreferredShard</code> algorithm described in
 * https://people.cs.umass.edu/~ramesh/Site/PUBLICATIONS_files/MRS01.pdf
 * which will sample two shards and return the lower of the two. It can
 * be shown that the max load is O(log log n) with high probability for
 * n tasks, which is close to optimal.
 *
 * The definition of load is given by the <code>GetLoadFn</code> callback
 * provided in the constructor.
 *
 * NOTE: The caller must ensure two conditions for this class to
 * be concurrency-safe:
 * 1. Caller is responsible for shard list manipulation concurrency-safety
 * (for <code>AddShard</code> and <code>RemoveShard</code>).
 * 2. Caller must ensure shard manipulations calls are not interleaved
 * with <code>GetPreferredShard</code> calls; for instance, if shard
 * manipulation all occurs before getting preferred shards, this is
 * satisfied.
 *
 * Assuming the above, concurrent calls <code>GetPreferredShard</code> are
 * safely handled by this class.
 */
class PowerOfTwoLoadBalancer : public LoadBalancer {
 public:
  using GetLoadFn = std::function<LoadT(ShardId)>;

  explicit PowerOfTwoLoadBalancer(GetLoadFn&& get_load)
      : get_load_(std::move(get_load)),
        rng_(ThreadLocalPRNG()) {}

  ShardId GetPreferredShard() override;
  bool AddShard(ShardId shardId) override;
  bool RemoveShard(ShardId shardId) override;

 private:
  GetLoadFn get_load_;
  std::mt19937_64& rng_;
  std::set<ShardId> shards_;
};

}
