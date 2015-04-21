// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/RocketSpeed.h"

#include "include/Types.h"
#include "src/client/client_env.h"

namespace rocketspeed {

ClientOptions::ClientOptions()
    : storage(nullptr)
    , info_log(nullptr)
    , env(ClientEnv::Default())
    , wake_lock(nullptr)
    , num_workers(1) {
}

}  // namespace rocketspeed
