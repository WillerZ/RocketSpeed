// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/RocketSpeed.h"

#include <cmath>
#include <limits>
#include <random>

#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/storage/file_storage.h"
#include "src/util/common/client_env.h"

namespace rocketspeed {

namespace backoff {

BackOffStrategy RandomizedTruncatedExponential(
    std::chrono::milliseconds backoff_value,
    std::chrono::milliseconds backoff_limit,
    double backoff_base,
    double jitter) {
  RS_ASSERT(jitter >= 0.0);
  RS_ASSERT(jitter <= 1.0);
  return [=](ClientRNG* rng, size_t retry) -> std::chrono::milliseconds {
    auto multiplier = std::pow(backoff_base, static_cast<double>(retry));
    auto exp_backoff = decltype(backoff_value)(
        static_cast<size_t>(static_cast<double>(backoff_value.count())
                            * multiplier));
    auto trunc_backoff = std::min(exp_backoff, backoff_limit);
    // 0.0 and 1.0 have precise representations.
    std::uniform_real_distribution<double> distribution(1.0 - jitter, 1.0);
    auto fuzz = distribution(*rng);
    auto fuzzed_backoff = decltype(trunc_backoff)(
        static_cast<size_t>(static_cast<double>(trunc_backoff.count()) * fuzz));
    return fuzzed_backoff;
  };
}
}

ClientOptions::ClientOptions()
: env(ClientEnv::Default())
, env_options(EnvOptions())
, num_workers(1)
, timer_period(200)
, backoff_strategy(backoff::RandomizedTruncatedExponential(
      std::chrono::seconds(1), std::chrono::seconds(30), 2.0))
, unsubscribe_deduplication_timeout(10 * 1000)
, publish_timeout(10 * 1000)
, max_subscriptions(std::numeric_limits<size_t>::max())
, connection_without_streams_keepalive(std::chrono::milliseconds(0))
, subscription_rate_limit(1000 * 1000 * 1000)
, queue_size(50000)
, allocator_size(1024)
, stats_prefix("rocketspeed")
, should_notify_health(true)
, max_silent_reconnects(3)
, inactive_stream_linger(std::chrono::seconds(10))
, tenant_id(GuestTenant) {}

}  // namespace rocketspeed
