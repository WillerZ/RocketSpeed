// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/RocketSpeed.h"

#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/storage/file_storage.h"
#include "src/util/common/client_env.h"

namespace rocketspeed {

ClientOptions::ClientOptions()
    : env(ClientEnv::Default())
    , num_workers(1)
    , close_connection_with_no_subscription(true)
    , timer_period(200) {
}

}  // namespace rocketspeed
