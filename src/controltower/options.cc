// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "src/controltower/options.h"

namespace rocketspeed {

ControlTowerOptions::ControlTowerOptions()
  : env(Env::Default()),
    info_log(nullptr),
#ifdef NDEBUG
    info_log_level(WARN_LEVEL),
#else
    info_log_level(INFO_LEVEL),
#endif
    log_dir(""),
    max_log_file_size(0),
    log_file_time_to_roll(0),
    max_subscription_lag(10000),
    readers_per_room(2) {
}

}  // namespace rocketspeed
