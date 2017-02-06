// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Centrifuge.h"
#include "include/HostId.h"
#include <atomic>
#include <limits>
#include <mutex>
#include "src/tools/centrifuge/centrifuge.h"
#include <xxhash.h>

namespace rocketspeed {

namespace {
class VolatileShardingStrategyImpl : public ShardingStrategy {
 public:
  using Options = VolatileShardingOptions;

  VolatileShardingStrategyImpl(
      std::shared_ptr<ShardingStrategy> strategy,
      Options&& options)
  : strategy_(std::move(strategy))
  , options_(std::move(options)) {
    std::lock_guard<std::mutex> lock(state_change_mutex_);
    UpdateFailures();
  }

  ~VolatileShardingStrategyImpl() {}

  size_t GetShardWithParams(
      Slice namespace_id, Slice topic_name,
      const IntroParameters& params) const override {
    return strategy_->GetShardWithParams(namespace_id, topic_name, params);
  }

  size_t GetVersion() override {
    std::lock_guard<std::mutex> lock(state_change_mutex_);
    if (Clock::now() > next_version_change_) {
      UpdateFailures();
    }
    // Increasing either will bump the version.
    return strategy_->GetVersion() + volatile_version_;
  }

  HostId GetHost(size_t shard) override {
    std::lock_guard<std::mutex> lock(state_change_mutex_);
    // With probability pct_fail_, a host may be switched with an empty host.
    uint64_t seed = volatile_version_;
    double hash = static_cast<double>(XXH64(&shard, sizeof(shard), seed));
    double hashmax = static_cast<double>(std::numeric_limits<size_t>::max());
    if (hash / hashmax < pct_fail_) {
      return HostId();
    } else {
      return strategy_->GetHost(shard);
    }
  }

  void MarkHostDown(const HostId& host_id) override {
    strategy_->MarkHostDown(host_id);
  }

 private:
  void UpdateFailures() {
    if (options_.next_setup) {
      auto setup = options_.next_setup();
      next_version_change_ = Clock::now() + setup.duration;
      ++volatile_version_;
      pct_fail_ = setup.failure_rate;
    }
  }

  using Clock = std::chrono::steady_clock;

  std::shared_ptr<ShardingStrategy> strategy_;
  Options options_;
  std::mutex state_change_mutex_;
  size_t volatile_version_{0};
  double pct_fail_{0.0};
  Clock::time_point next_version_change_;
};
} // anonymous namespace

std::shared_ptr<ShardingStrategy> CreateVolatileShardingStrategy(
    std::shared_ptr<ShardingStrategy> strategy,
    VolatileShardingOptions&& options) {
  return std::make_shared<VolatileShardingStrategyImpl>(
      std::move(strategy), std::move(options));
}

VolatileShardingOptions::VolatileShardingOptions()
: next_setup([]() {
    VolatileSetup setup;
    setup.duration = std::chrono::seconds(5);
    setup.failure_rate = 0.1;
    return setup;
  }) {
}

}
