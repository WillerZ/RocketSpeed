// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "src/copilot/options.h"
#include <unistd.h>
#include <thread>

namespace rocketspeed {

CopilotOptions::CopilotOptions()
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
    control_tower_connections(4),
    rollcall_enabled(true),
    rollcall_max_batch_size_bytes(16 << 10),
    rollcall_flush_latency(500),
    timer_interval_micros(500000),
    resubscriptions_per_second(10000),
    tower_subscriptions_check_period(10 * 60),
    rebalances_per_second(1000) {
}

}  // namespace rocketspeed
