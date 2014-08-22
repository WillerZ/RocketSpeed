// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "src/pilot/options.h"
#include <unistd.h>

namespace rocketspeed {

PilotOptions::PilotOptions()
  : env(Env::Default()),
    port_number(58600),
    info_log(nullptr),
    info_log_level(INFO_LEVEL),
    log_dir(""),
    max_log_file_size(0),
    log_file_time_to_roll(0) {
  char myname[1024];
  gethostname(&myname[0], sizeof(myname));
  pilotname.assign(myname);
}

}  // namespace rocketspeed
