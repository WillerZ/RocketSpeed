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
#include <signal.h>
#include "include/Types.h"
#include "src/pilot/options.h"
#include "src/pilot/pilot.h"
#include "src/util/logdevice.h"

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::RegisterFlagValidator;
using GFLAGS::SetUsageMessage;

DEFINE_int32(num_threads, 16, "number of threads");
DEFINE_int32(port_number, 58600, "port number");
DEFINE_string(logs, "1..100000", "range of logs");
DEFINE_bool(libevent_debug, false, "Debugging libevent");
DEFINE_string(storage_url,
              "configerator:logdevice/rocketspeed.logdevice.primary.conf",
              "Storage service url");
/*
 * Dumps libevent info messages to stdout
 */
void
dump_libevent_cb(int severity, const char* msg) {
  const char* s;
  switch (severity) {
    case _EVENT_LOG_DEBUG: s = "dbg"; break;
    case _EVENT_LOG_MSG:   s = "msg"; break;
    case _EVENT_LOG_WARN:  s = "wrn"; break;
    case _EVENT_LOG_ERR:   s = "err"; break;
    default:               s = "?";   break; /* never reached */
  }
  printf("[%s] %s\n", s, msg);
}

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  // Ignore SIGPIPE, we'll just handle the EPIPE returned by write.
  signal(SIGPIPE, SIG_IGN);

  // Setup Pilot options.
  rocketspeed::PilotOptions options;
  options.port_number = FLAGS_port_number;
  options.storage_url = FLAGS_storage_url;

  // Parse and validate log range.
  int ret = sscanf(FLAGS_logs.c_str(), "%lu..%lu",
    &options.log_range.first, &options.log_range.second);
  if (ret != 2) {
    printf("Error: log_range option must be in the form of \"a..b\"");
    return 1;
  }

  // Create global options and configs from command line
  if (FLAGS_libevent_debug) {
    ld_event_enable_debug_logging(EVENT_DBG_ALL);
    ld_event_set_log_callback(dump_libevent_cb);
    ld_event_enable_debug_mode();
  }

  // create an instance of the Pilot
  rocketspeed::Pilot* pilot = nullptr;

  rocketspeed::Status st = rocketspeed::Pilot::CreateNewInstance(
                             std::move(options), &pilot);
  if (!st.ok()) {
    printf("Error in Starting Pilot\n");
    return 1;
  } else {
    pilot->Run();
  }
  delete pilot;

  // shutdown libevent for good hygine
  ld_libevent_global_shutdown();

  return 0;
}

#endif  // GFLAGS
