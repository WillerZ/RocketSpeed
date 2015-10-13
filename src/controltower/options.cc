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
    readers_per_room(2),
    cache_size(0),
    cache_data_from_system_namespaces(true),
    timer_interval(std::chrono::milliseconds(100)),
    room_to_client_queue_size(1000) {
}

ControlTowerOptions::TopicTailer::TopicTailer()
: min_reader_restart_duration(std::chrono::seconds(30))
, max_reader_restart_duration(std::chrono::seconds(60))
, storage_to_room_queue_size(1000)
, FAULT_send_log_record_failure_rate(0) {
}


}  // namespace rocketspeed
