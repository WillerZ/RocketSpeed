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
DEFINE_int32(log_count, 1, "number of logs");
DEFINE_bool(libevent_debug, false, "Debugging libevent");

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

  rocketspeed::Configuration* conf = nullptr;
  rocketspeed::PilotOptions options;

  // Create Log Device interface
  rocketspeed::LogDeviceStorage* storage = nullptr;
  std::unique_ptr<facebook::logdevice::ClientSettings> clientSettings(
    facebook::logdevice::ClientSettings::create());
  if (!rocketspeed::LogDeviceStorage::Create(
    "",
    "",
    "",
    std::chrono::milliseconds(5000),
    std::move(clientSettings),
    rocketspeed::Env::Default(),
    &storage).ok()) {
    printf("Error creating Log Device client\n");
    return 1;
  }

  options.port_number = FLAGS_port_number;
  options.log_count = FLAGS_log_count;
  options.log_storage = std::unique_ptr<rocketspeed::LogStorage>(storage);

  // Create global options and configs from command line
  if (FLAGS_libevent_debug) {
    ld_event_enable_debug_logging(EVENT_DBG_ALL);
    ld_event_set_log_callback(dump_libevent_cb);
    ld_event_enable_debug_mode();
  }

  // create an instance of the Pilot
  rocketspeed::Pilot* pilot = nullptr;

  rocketspeed::Status st = rocketspeed::Pilot::CreateNewInstance(
                             std::move(options), conf, &pilot);
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
