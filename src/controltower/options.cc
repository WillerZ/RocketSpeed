// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "src/controltower/options.h"

namespace rocketspeed {

ControlTowerOptions::ControlTowerOptions()
  : env(Env::Default()),
    hostname(""),
    port_number(58500),
    info_log(nullptr),
    info_log_level(INFO_LEVEL),
    log_dir(""),
    max_log_file_size(0),
    log_file_time_to_roll(0),
    number_of_rooms(env->GetNumberOfCpus()),
    log_range(1, 100000),
    max_number_of_hosts(10000),
    storage(nullptr) {
  char myname[1024];
  gethostname(&myname[0], sizeof(myname));
  hostname.assign(myname);
}

}  // namespace rocketspeed
