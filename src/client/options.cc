// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/RocketSpeed.h"

#include <random>
#include <limits>

#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/storage/file_storage.h"
#include "src/util/common/client_env.h"

namespace rocketspeed {

struct DefaultBackOffDistribution {
  double operator()(ClientRNG* rng) const {
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    return distribution(*rng);
  }
};

ClientOptions::ClientOptions()
: env(ClientEnv::Default())
, num_workers(1)
, timer_period(200)
, backoff_base(2.0)
, backoff_initial(1000)
, backoff_limit(30 * 1000)
, backoff_distribution(DefaultBackOffDistribution())
, unsubscribe_deduplication_timeout(10 * 1000)
, publish_timeout(5 * 1000)
, max_subscriptions(std::numeric_limits<size_t>::max())
, connection_without_streams_keepalive(std::chrono::milliseconds(0))
, subscription_rate_limit(1000 * 1000 * 1000)
, collapse_subscriptions_to_tail(false) {
}

}  // namespace rocketspeed
