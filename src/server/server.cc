//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#ifndef GFLAGS
#error "gflags is required for rocketspeed"
#endif

#include "src/server/server.h"
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
#include "src/supervisor/supervisor_loop.h"
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
DEFINE_int32(copilot_workers, 16, "copilot worker threads");
DEFINE_string(control_towers,
              "localhost",
              "comma-separated control tower hostnames");
DEFINE_int32(copilot_connections, 8,
             "num connections between one copilot and one control tower");
DEFINE_bool(rollcall, true, "enable RollCall");

// Supervisor settings
DEFINE_bool(supervisor, true, "start the supervisor");
DEFINE_int32(supervisor_port,
             rocketspeed::SupervisorLoop::DEFAULT_PORT,
             "supervisor port number");

DEFINE_string(rs_log_dir, "", "directory for server logs");

#ifdef NDEBUG
DEFINE_string(loglevel, "warn", "debug|info|warn|error|fatal|vital|none");
#else
DEFINE_string(loglevel, "info", "debug|info|warn|error|fatal|vital|none");
#endif

namespace rocketspeed {

RocketSpeed::RocketSpeed(Env* env, EnvOptions env_options)
: env_(env)
, env_options_(env_options) {
  // Ignore SIGPIPE, we'll just handle the EPIPE returned by write.
  signal(SIGPIPE, SIG_IGN);

  auto log_level = StringToLogLevel(FLAGS_loglevel.c_str());
  fprintf(stderr, "RocketSpeed log level set to %s\n",
    LogLevelToString(log_level));

  // Create info log
  Status st;
  if (FLAGS_log_to_stderr) {
    st = env_->StdErrLogger(&info_log_);
    if (info_log_) {
      info_log_->SetInfoLogLevel(log_level);
    }
  } else {
    st = CreateLoggerFromOptions(env_,
                                 FLAGS_rs_log_dir,
                                 "LOG",
                                 0,
                                 0,
                                 log_level,
                                 &info_log_);
  }
  if (!st.ok()) {
    fprintf(stderr, "RocketSpeed failed to create Logger\n");
    info_log_ = std::make_shared<NullLogger>();
  }
}

Status RocketSpeed::Initialize(
    std::function<std::shared_ptr<LogStorage>(
      Env*, std::shared_ptr<Logger>)> get_storage,
    std::function<std::shared_ptr<LogRouter>()> get_router) {
  // As a special case, if no components are specified then all of them
  // are started.
  if (!FLAGS_pilot && !FLAGS_copilot && !FLAGS_tower) {
    LOG_VITAL(info_log_,
      "Starting all services (pilot, copilot, controltower)");
    FLAGS_pilot = true;
    FLAGS_copilot = true;
    FLAGS_tower = true;
  }

  LOG_VITAL(info_log_, "Creating LogRouter");
  std::shared_ptr<LogRouter> log_router = get_router();
  if (!log_router) {
    LOG_FATAL(info_log_, "Failed to create LogRouter");
  }

  // Utility for creating a message loop.
  auto make_msg_loop = [&] (int port, int workers, std::string name) {
    LOG_VITAL(info_log_, "Constructing MsgLoop port=%d workers=%d name=%s",
      port, workers, name.c_str());
    return new MsgLoop(env_,
                       env_options_,
                       port,
                       workers,
                       info_log_,
                       std::move(name));
  };

  ControlTower* tower = nullptr;
  Pilot* pilot = nullptr;
  Copilot* copilot = nullptr;

  std::shared_ptr<MsgLoop> tower_loop;
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
    LOG_VITAL(info_log_, "Creating LogStorage");
    storage = get_storage(env_, info_log_);
    if (!storage) {
      return Status::InternalError("Failed to construct log storage");
    }
  }

  if (FLAGS_pilot && FLAGS_copilot && FLAGS_pilot_port == FLAGS_copilot_port) {
    // Pilot + Copilot sharing message loop.
    LOG_VITAL(info_log_, "Pilot and copilot sharing MsgLoop port=%d",
      FLAGS_pilot_port);
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
    LOG_VITAL(info_log_, "Creating Control Tower");
    ControlTowerOptions tower_opts;
    tower_opts.msg_loop = tower_loop.get();
    tower_opts.worker_queue_size = FLAGS_worker_queue_size;
    tower_opts.number_of_rooms = FLAGS_tower_rooms;
    tower_opts.info_log = info_log_;
    tower_opts.storage = storage;
    tower_opts.log_router = log_router;

    Status st = ControlTower::CreateNewInstance(std::move(tower_opts),
                                                &tower);
    if (!st.ok()) {
      return st;
    }
    tower_.reset(tower);
  }

  // Create Pilot.
  HostId pilot_host("localhost", FLAGS_pilot_port);
  if (FLAGS_pilot) {
    LOG_VITAL(info_log_, "Creating Pilot");
    PilotOptions pilot_opts;
    pilot_opts.msg_loop = pilot_loop.get();
    pilot_opts.info_log = info_log_;
    pilot_opts.storage = storage;
    pilot_opts.log_router = log_router;

    Status st = Pilot::CreateNewInstance(std::move(pilot_opts),
                                         &pilot);
    if (!st.ok()) {
      return st;
    }
    pilot_.reset(pilot);
  }

  // Create Copilot.
  if (FLAGS_copilot) {
    LOG_VITAL(info_log_, "Creating Copilot");
    CopilotOptions copilot_opts;
    copilot_opts.msg_loop = copilot_loop.get();
    copilot_opts.worker_queue_size = FLAGS_worker_queue_size;
    copilot_opts.num_workers = FLAGS_copilot_workers;
    copilot_opts.info_log = info_log_;
    copilot_opts.control_tower_connections = FLAGS_copilot_connections;
    copilot_opts.log_router = log_router;
    copilot_opts.rollcall_enabled = FLAGS_rollcall;

    // TODO(pja) 1 : Configure control tower hosts from config file.
    // Parse comma-separated control_towers hostname.
    ControlTowerId node_id = 0;
    for (auto hostname : SplitString(FLAGS_control_towers)) {
      HostId host(hostname, FLAGS_tower_port);
      copilot_opts.control_towers.emplace(node_id, host);
      LOG_VITAL(info_log_, "Adding control tower '%s'",
        host.ToString().c_str());
      ++node_id;
    }
    if (FLAGS_pilot) {
      copilot_opts.pilots.push_back(pilot_host);
    }
    Status st = Copilot::CreateNewInstance(std::move(copilot_opts),
                                           &copilot);
    if (!st.ok()) {
      return st;
    }
    copilot_.reset(copilot);
  }

  // Create Supervisor loop
  if (FLAGS_supervisor) {
    LOG_VITAL(info_log_, "Creating Supervisor loop");
    SupervisorOptions supervisor_opts;
    supervisor_opts.port = (uint32_t)FLAGS_supervisor_port;
    supervisor_opts.info_log = info_log_;
    supervisor_opts.tower_loop = tower_loop.get(),
    supervisor_opts.copilot_loop = copilot_loop.get();
    supervisor_opts.pilot_loop = pilot_loop.get();

    Status st = SupervisorLoop::CreateNewInstance(std::move(supervisor_opts),
                                                  &supervisor_loop_);
    if (!st.ok()) {
      LOG_ERROR(info_log_, "Failed to create the supervisor loop");
    }
  }

  // Get a list of message loops.
  if (tower_loop) {
    msg_loops_.emplace_back(tower_loop);
  }
  if (copilot_loop) {
    msg_loops_.emplace_back(copilot_loop);
  }
  if (pilot_loop && pilot_loop != copilot_loop) {
    msg_loops_.emplace_back(pilot_loop);
  }

  // Initialize message loops.
  for (auto& msg_loop : msg_loops_) {
    Status st = msg_loop->Initialize();
    if (!st.ok()) {
      return st;
    }
  }
  // initialize superisor loop
  Status st = supervisor_loop_->Initialize();
  if (!st.ok()) {
    LOG_ERROR(info_log_, "Failed to initialize the supervisor loop");
  }

  return Status::OK();
}

RocketSpeed::~RocketSpeed() {
  // All threads must be stopped first, otherwise we may still have running
  // services.
  assert(threads_.empty());
  assert(msg_loops_.empty());

  // Shutdown libevent for good hygiene.
  EventLoop::GlobalShutdown();
}

void RocketSpeed::Run() {
  // Start the supervisor thread
  if (supervisor_loop_) {
    threads_.emplace_back(
      [this] (SupervisorLoop* s) {
        s->Run();
        LOG_VITAL(info_log_, "Supervisor loop finished.");
      },
      supervisor_loop_.get()
    );
  }

  // Start all the messages loops, with the first loop started in this thread.
  LOG_VITAL(info_log_, "Starting all message loop threads");
  assert(msg_loops_.size() != 0);
  for (size_t i = 1; i < msg_loops_.size(); ++i) {
    threads_.emplace_back(
      [this, i] (MsgLoop* msg_loop) {
        msg_loop->Run();
        LOG_VITAL(info_log_, "Message loop %zu finished.", i);
      },
      msg_loops_[i].get());
  }

  // Start the first loop, this will block until something the loop exits for
  // some reason.
  msg_loops_[0]->Run();
  LOG_VITAL(info_log_, "Message loop 0 finished.");
}

void RocketSpeed::Stop() {
  // Stop all message loops.
  LOG_VITAL(info_log_, "Stopping Message Loops");
  for (std::shared_ptr<MsgLoop>& loop : msg_loops_) {
    loop->Stop();
  }
  if (supervisor_loop_) {
    supervisor_loop_->Stop();
  }

  // Join all the other message loops.
  LOG_VITAL(info_log_, "Joining all the loop threads");
  for (std::thread& t : threads_) {
    if (t.joinable()) {
      t.join();
    }
  }

  // Stop all services.
  if (tower_) {
    LOG_VITAL(info_log_, "Stopping Control Tower");
    tower_->Stop();
  }
  if (copilot_) {
    LOG_VITAL(info_log_, "Stopping Copilot");
    copilot_->Stop();
  }
  if (pilot_) {
    LOG_VITAL(info_log_, "Stopping Pilot");
    pilot_->Stop();
  }
  threads_.clear();
  msg_loops_.clear();
}

Statistics RocketSpeed::GetStatisticsSync() {
  Statistics server_stats;
  if (pilot_) {
    server_stats.Aggregate(pilot_->GetStatisticsSync());
  }
  if (copilot_) {
    server_stats.Aggregate(copilot_->GetStatisticsSync());
  }
  if (tower_) {
    server_stats.Aggregate(tower_->GetStatisticsSync());
  }
  return server_stats;
}

}  // namespace rocketspeed
