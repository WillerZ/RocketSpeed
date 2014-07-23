//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocketspeed binaries\n");
  return 1;
}
#else

#include <gflags/gflags.h>
#include <iostream>
#include "include/Types.h"
#include "src/controltower/options.h"
#include "src/controltower/controltower.h"

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::RegisterFlagValidator;
using GFLAGS::SetUsageMessage;

DEFINE_int32(num_threads, 16, "number of threads");
DEFINE_int32(port_number, 60000, "port number");
DEFINE_bool(libevent_debug, false, "Debugging libevent");

/*
 * Dumps libevent info messages to stdout
 */
void
dump_libevent_cb(int severity, const char* msg) {
  const char* s;
  switch (severity) {
    case _EVENT_LOG_DEBUG: s = "dbg"; break;
    case _EVENT_LOG_MSG:   s = "msg";   break;
    case _EVENT_LOG_WARN:  s = "wrn";  break;
    case _EVENT_LOG_ERR:   s = "err"; break;
    default:               s = "?";     break; /* never reached */
  }
  printf("[%s] %s\n", s, msg);
}

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  rocketspeed::Configuration conf;
  rocketspeed::ControlTowerOptions options;

  // Create global options and configs from command line
  if (FLAGS_libevent_debug) {
    ld_event_enable_debug_logging(EVENT_DBG_ALL);
    ld_event_set_log_callback(dump_libevent_cb);
    ld_event_enable_debug_mode();
  }

  // create an instance of the ControlTower
  rocketspeed::ControlTower* ct = nullptr;

  rocketspeed::Status st = rocketspeed::ControlTower::CreateNewInstance(
                             options, conf, &ct);
  if (!st.ok()) {
    std::cout << "Error in Starting ControlTower";
  } else {
    ct->Run();
  }
  delete ct;

  // shutdown libevent for good hygine
  ld_libevent_global_shutdown();

  return 0;
}

#endif  // GFLAGS
