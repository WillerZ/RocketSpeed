//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#ifndef GFLAGS
#error "glags is required for rocketspeed"
#endif

#include <gflags/gflags.h>
#include <signal.h>
#include "include/Types.h"
#include "src/pilot/options.h"
#include "src/pilot/pilot.h"
#include "src/copilot/options.h"
#include "src/copilot/copilot.h"
#include "src/controltower/options.h"
#include "src/controltower/tower.h"
#include "src/util/logdevice.h"

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::RegisterFlagValidator;
using GFLAGS::SetUsageMessage;

// Common settings
DEFINE_string(logs, "1..100000", "range of logs");
DEFINE_string(storage_url,
              "configerator:logdevice/rocketspeed.logdevice.primary.conf",
              "Storage service url");
DEFINE_int32(worker_queue_size, 1000000, "number of worker commands in flight");

// Control tower settings
DEFINE_bool(tower, false, "start the control tower");
DEFINE_int32(tower_port, 58500, "tower port number");
DEFINE_int32(tower_rooms, 4, "tower rooms");

// Pilot settings
DEFINE_bool(pilot, false, "start the pilot");
DEFINE_int32(pilot_port, 58600, "pilot port number");
DEFINE_int32(pilot_workers, 4, "pilot worker threads");

// Copilot settings
DEFINE_bool(copilot, false, "start the copilot");
DEFINE_int32(copilot_port, 58700, "copilot port number");
DEFINE_int32(copilot_workers, 4, "copilot worker threads");

int main(int argc, char** argv) {
  rocketspeed::Status st;
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  // As a special case, if no components are specified then all of them
  // are started.
  if (!FLAGS_pilot && !FLAGS_copilot && !FLAGS_tower) {
    FLAGS_pilot = true;
    FLAGS_copilot = true;
    FLAGS_tower = true;
  }

  // Ignore SIGPIPE, we'll just handle the EPIPE returned by write.
  signal(SIGPIPE, SIG_IGN);

  // Parse and validate log range.
  std::pair<rocketspeed::LogID, rocketspeed::LogID> log_range;
  int ret = sscanf(FLAGS_logs.c_str(), "%lu..%lu",
    &log_range.first, &log_range.second);
  if (ret != 2) {
    printf("Error: log_range option must be in the form of \"a..b\"");
    return 1;
  }

  // Create info log
  std::shared_ptr<rocketspeed::Logger> info_log;
  st = rocketspeed::CreateLoggerFromOptions(rocketspeed::Env::Default(),
                                            "",
                                            "LOG",
                                            0,
                                            0,
#ifdef NDEBUG
                                            rocketspeed::WARN_LEVEL,
#else
                                            rocketspeed::INFO_LEVEL,
#endif
                                            &info_log);
  if (!st.ok()) {
    info_log = nullptr;
  }

  rocketspeed::ControlTower* tower = nullptr;
  rocketspeed::Pilot* pilot = nullptr;
  rocketspeed::Copilot* copilot = nullptr;

  // Create Control Tower.
  if (FLAGS_tower) {
    rocketspeed::ControlTowerOptions tower_opts;
    tower_opts.port_number = FLAGS_tower_port;
    tower_opts.storage_url = FLAGS_storage_url;
    tower_opts.worker_queue_size = FLAGS_worker_queue_size;
    tower_opts.number_of_rooms = FLAGS_tower_rooms;
    tower_opts.info_log = info_log;
    tower_opts.log_range = log_range;

    st = rocketspeed::ControlTower::CreateNewInstance(std::move(tower_opts),
                                                      &tower);
    if (!st.ok()) {
      fprintf(stderr, "Error in Starting ControlTower\n");
      return 1;
    }
  }

  // Create Pilot.
  if (FLAGS_pilot) {
    rocketspeed::PilotOptions pilot_opts;
    pilot_opts.port_number = FLAGS_pilot_port;
    pilot_opts.storage_url = FLAGS_storage_url;
    pilot_opts.worker_queue_size = FLAGS_worker_queue_size;
    pilot_opts.log_range = log_range;
    pilot_opts.num_workers = FLAGS_pilot_workers;
    pilot_opts.info_log = info_log;

    st = rocketspeed::Pilot::CreateNewInstance(std::move(pilot_opts),
                                               &pilot);
    if (!st.ok()) {
      fprintf(stderr, "Error in Starting Pilot\n");
      return 1;
    }
  }

  // Create Copilot.
  if (FLAGS_copilot) {
    rocketspeed::CopilotOptions copilot_opts;
    copilot_opts.port_number = FLAGS_copilot_port;
    copilot_opts.worker_queue_size = FLAGS_worker_queue_size;
    copilot_opts.log_range = log_range;
    copilot_opts.num_workers = FLAGS_copilot_workers;
    copilot_opts.info_log = info_log;

    // TODO(pja) 1 : Configure control tower hosts from config file.
    rocketspeed::HostId tower_host("localhost", FLAGS_tower_port);
    copilot_opts.control_towers.push_back(tower_host);

    st = rocketspeed::Copilot::CreateNewInstance(std::move(copilot_opts),
                                                 &copilot);
    if (!st.ok()) {
      fprintf(stderr, "Error in Starting Copilot\n");
      return 1;
    }
  }

  // Start all the services, with the last one running in this thread.
  std::vector<std::thread> threads;
  if (tower) {
    if (!copilot && !pilot) {
      // No other services, run in this thread.
      tower->Run();
    } else {
      // Start in background.
      threads.emplace_back([tower] () { tower->Run(); });
    }
  }

  if (copilot) {
    if (!pilot) {
      // No other services, run in this thread.
      copilot->Run();
    } else {
      // Start in background.
      threads.emplace_back([copilot] () { copilot->Run(); });
    }
  }

  if (pilot) {
    // Run in this thread.
    pilot->Run();
  }

  // Stop all event loops.
  delete pilot;
  delete copilot;
  delete tower;

  // Join all the background threads.
  for (std::thread& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  // Shutdown libevent for good hygiene.
  ld_libevent_global_shutdown();

  return 0;
}
