// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/RocketSpeed.h"

#include <random>

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
    , close_connection_with_no_subscription(true)
    , timer_period(200)
    , backoff_base(2.0)
    , backoff_initial(5 * 1000)
    , backoff_limit(1 * 60 * 1000)
    , backoff_distribution(DefaultBackOffDistribution())
    , unsubscribe_deduplication_timeout(10 * 1000) {
}

}  // namespace rocketspeed
