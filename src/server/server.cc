//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#ifndef GFLAGS
#error "gflags is required for rocketspeed"
#endif

#include <gflags/gflags.h>
#include <signal.h>
#include <algorithm>
#include <string>
#include "include/Types.h"
#include "src/pilot/options.h"
#include "src/pilot/pilot.h"
#include "src/copilot/options.h"
#include "src/copilot/copilot.h"
#include "src/controltower/options.h"
#include "src/controltower/tower.h"
#include "src/util/control_tower_router.h"
#include "src/util/parsing.h"
#include "src/util/storage.h"

// Common settings
DEFINE_int32(worker_queue_size, 1000000, "number of worker commands in flight");
DEFINE_bool(log_to_stderr, false, "log to stderr (otherwise LOG file)");

// Control tower settings
DEFINE_bool(tower, false, "start the control tower");
DEFINE_int32(tower_port,
             rocketspeed::ControlTower::DEFAULT_PORT,
             "tower port number");
DEFINE_int32(tower_rooms, 16, "tower rooms");

// Pilot settings
DEFINE_bool(pilot, false, "start the pilot");
DEFINE_int32(pilot_port,
             rocketspeed::Pilot::DEFAULT_PORT,
             "pilot port number");
DEFINE_int32(pilot_workers, 16, "pilot worker threads");

// Copilot settings
DEFINE_bool(copilot, false, "start the copilot");
DEFINE_int32(copilot_port,
             rocketspeed::Copilot::DEFAULT_PORT,
             "copilot port number");
DEFINE_int32(copilot_workers, 32, "copilot worker threads");
DEFINE_string(control_towers,
              "localhost",
              "comma-separated control tower hostnames");
DEFINE_int32(copilot_connections, 8,
             "num connections between one copilot and one control tower");
DEFINE_bool(rollcall, true, "enable RollCall");

DEFINE_string(rs_log_dir, "", "directory for server logs");

namespace rocketspeed {

int Run(int argc,
        char** argv,
        std::function<std::shared_ptr<LogStorage>(
          Env*, std::shared_ptr<Logger>)> get_storage,
        std::function<std::shared_ptr<LogRouter>()> get_router,
        Env* env,
        EnvOptions env_options) {
  Status st;
  std::shared_ptr<LogRouter> log_router = get_router();

  // As a special case, if no components are specified then all of them
  // are started.
  if (!FLAGS_pilot && !FLAGS_copilot && !FLAGS_tower) {
    FLAGS_pilot = true;
    FLAGS_copilot = true;
    FLAGS_tower = true;
  }

  // Ignore SIGPIPE, we'll just handle the EPIPE returned by write.
  signal(SIGPIPE, SIG_IGN);

#ifdef NDEBUG
  auto log_level = WARN_LEVEL;
#else
  auto log_level = INFO_LEVEL;
#endif

  // Create info log
  std::shared_ptr<Logger> info_log;
  if (FLAGS_log_to_stderr) {
    st = env->StdErrLogger(&info_log);
    if (info_log) {
      info_log->SetInfoLogLevel(log_level);
    }
  } else {
    st = CreateLoggerFromOptions(env,
                                 FLAGS_rs_log_dir,
                                 "LOG",
                                 0,
                                 0,
                                 log_level,
                                 &info_log);
  }
  if (!st.ok()) {
    info_log = std::make_shared<NullLogger>();
  }

  // Utility for creating a message loop.
  auto make_msg_loop = [&] (int port, int workers, std::string name) {
    return new MsgLoop(env,
                       env_options,
                       port,
                       workers,
                       info_log,
                       std::move(name));
  };

  ControlTower* tower = nullptr;
  Pilot* pilot = nullptr;
  Copilot* copilot = nullptr;

  std::unique_ptr<MsgLoop> tower_loop;
  std::shared_ptr<MsgLoop> pilot_loop;
  std::shared_ptr<MsgLoop> copilot_loop;

  if (FLAGS_tower) {
    tower_loop.reset(make_msg_loop(FLAGS_tower_port,
                                   FLAGS_tower_rooms,
                                   "tower"));
  }

  std::shared_ptr<LogStorage> storage;
  if (FLAGS_pilot || FLAGS_tower) {
    // Only need storage for pilot and control tower.
    storage = get_storage(env, info_log);
    if (!storage) {
      fprintf(stderr, "Failed to construct log storage");
      return 1;
    }
  }

  if (FLAGS_pilot && FLAGS_copilot && FLAGS_pilot_port == FLAGS_copilot_port) {
    // Pilot + Copilot sharing message loop.
    int workers = std::max(FLAGS_pilot_workers, FLAGS_copilot_workers);
    pilot_loop.reset(make_msg_loop(FLAGS_pilot_port,
                                   workers,
                                   "cockpit"));
    copilot_loop = pilot_loop;
  } else {
    // Separate message loops if enabled.
    if (FLAGS_pilot) {
      pilot_loop.reset(make_msg_loop(FLAGS_pilot_port,
                                     FLAGS_pilot_workers,
                                     "pilot"));
    }
    if (FLAGS_copilot) {
      copilot_loop.reset(make_msg_loop(FLAGS_copilot_port,
                                       FLAGS_copilot_workers,
                                       "copilot"));
    }
  }

  // Create Control Tower.
  if (FLAGS_tower) {
    ControlTowerOptions tower_opts;
    tower_opts.msg_loop = tower_loop.get();
    tower_opts.worker_queue_size = FLAGS_worker_queue_size;
    tower_opts.number_of_rooms = FLAGS_tower_rooms;
    tower_opts.info_log = info_log;
    tower_opts.storage = storage;
    tower_opts.log_router = log_router;

    st = ControlTower::CreateNewInstance(std::move(tower_opts),
                                                      &tower);
    if (!st.ok()) {
      fprintf(stderr, "Error in Starting ControlTower\n");
      return 1;
    }
  }

  // Create Pilot.
  HostId pilot_host("localhost", FLAGS_pilot_port);
  if (FLAGS_pilot) {
    PilotOptions pilot_opts;
    pilot_opts.msg_loop = pilot_loop.get();
    pilot_opts.info_log = info_log;
    pilot_opts.storage = storage;
    pilot_opts.log_router = log_router;

    st = Pilot::CreateNewInstance(std::move(pilot_opts),
                                               &pilot);
    if (!st.ok()) {
      fprintf(stderr, "Error in Starting Pilot\n");
      return 1;
    }
  }

  // Create Copilot.
  if (FLAGS_copilot) {
    CopilotOptions copilot_opts;
    copilot_opts.msg_loop = copilot_loop.get();
    copilot_opts.worker_queue_size = FLAGS_worker_queue_size;
    copilot_opts.num_workers = FLAGS_copilot_workers;
    copilot_opts.info_log = info_log;
    copilot_opts.control_tower_connections = FLAGS_copilot_connections;
    copilot_opts.log_router = log_router;
    copilot_opts.rollcall_enabled = FLAGS_rollcall;

    // TODO(pja) 1 : Configure control tower hosts from config file.
    // Parse comma-separated control_towers hostname.
    ControlTowerId node_id = 0;
    for (auto hostname : SplitString(FLAGS_control_towers)) {
      HostId host(hostname, FLAGS_tower_port);
      copilot_opts.control_towers.emplace(node_id, host);
      LOG_INFO(info_log, "Adding control tower '%s'",
        host.ToString().c_str());
      ++node_id;
    }
    if (FLAGS_pilot) {
      copilot_opts.pilots.push_back(pilot_host);
    }
    st = Copilot::CreateNewInstance(std::move(copilot_opts),
                                    &copilot);
    if (!st.ok()) {
      fprintf(stderr, "Error in Starting Copilot\n");
      return 1;
    }
  }

  // Start all the services, with the last one running in this thread.
  // Get a list of message loops.
  std::vector<MsgLoop*> msg_loops;
  if (tower_loop) {
    msg_loops.push_back(tower_loop.get());
  }
  if (copilot_loop) {
    msg_loops.push_back(copilot_loop.get());
  }
  if (pilot_loop && pilot_loop != copilot_loop) {
    msg_loops.push_back(pilot_loop.get());
  }

  // Start all the messages loops, with the last loop started in this thread.
  std::vector<std::thread> threads;
  assert(msg_loops.size() != 0);
  for (size_t i = 0; i < msg_loops.size() - 1; ++i) {
    threads.emplace_back(
      [] (MsgLoop* msg_loop) {
        msg_loop->Run();
      },
      msg_loops[i]);
  }
  // Start the last loop, this will block until something the loop exits for
  // some reason.
  msg_loops[msg_loops.size() - 1]->Run();

  // Join all the other message loops.
  for (std::thread& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  tower->Stop();

  // Stop all background services.
  delete pilot;
  delete copilot;
  delete tower;

  // Shutdown libevent for good hygiene.
  EventLoop::GlobalShutdown();

  return 0;
}

}  // namespace rocketspeed
