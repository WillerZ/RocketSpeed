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
    log_range(1, 100000),
    num_workers(std::thread::hardware_concurrency()),
    worker_queue_size(1000000),
    consistent_hash_replicas(20),
    control_towers_per_log(1) {
}

}  // namespace rocketspeed
