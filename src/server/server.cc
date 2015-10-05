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
#include <limits>
#include <set>
#include <string>

#include "include/Types.h"
#include "src/pilot/options.h"
#include "src/pilot/pilot.h"
#include "src/copilot/options.h"
#include "src/copilot/copilot.h"
#include "src/controltower/options.h"
#include "src/controltower/tower.h"
#include "src/supervisor/supervisor_loop.h"
#include "src/util/buffered_storage.h"
#include "src/util/common/parsing.h"
#include "src/util/control_tower_router.h"
#include "src/util/storage.h"

// Common settings
DEFINE_bool(log_to_stderr, false, "log to stderr (otherwise LOG file)");
DEFINE_uint64(buffered_storage_max_messages,
              0,
              "how many messages to batch in a storage record, <= 1 disables");
DEFINE_uint64(buffered_storage_max_bytes,
              std::numeric_limits<size_t>::max(),
              "how many bytes worth of messages to batch");
DEFINE_uint64(buffered_storage_max_latency_us,
              100,
              "for how long to wait before filling sending unfinished batch");
DEFINE_string(node_location, "",
  "location of this node for SSL: {region}.{dc}.{cluster}.{row}.{rack}");

// Control tower settings
DEFINE_bool(tower, false, "start the control tower");
DEFINE_int32(tower_port,
             rocketspeed::ControlTower::DEFAULT_PORT,
             "tower port number");
DEFINE_int32(tower_workers, 40, "tower rooms");
DEFINE_int64(tower_max_subscription_lag, 10000,
             "max seqno lag on subscriptions");
DEFINE_int32(tower_readers_per_room, 2, "log readers per room");
DEFINE_int64(tower_cache_size, -1, "cache size in bytes");
DEFINE_uint64(tower_min_reader_restart_ms, 30000,
  "minimum time to wait before restarting a log reader");
DEFINE_uint64(tower_max_reader_restart_ms, 60000,
  "maximum time to wait before restarting a log reader");
DEFINE_double(FAULT_tower_send_log_record_failure_rate, 0.0,
  "probability of failing to append to topic tailer queue from log storage");

// Pilot settings
DEFINE_bool(pilot, false, "start the pilot");
DEFINE_int32(pilot_port,
             rocketspeed::Pilot::DEFAULT_PORT,
             "pilot port number");
DEFINE_int32(pilot_workers, 40, "pilot worker threads");
DEFINE_double(FAULT_pilot_corrupt_extra_probability, 0.0,
  "probability of writing a corrupt message to the log after each publish");

// Copilot settings
DEFINE_bool(copilot, false, "start the copilot");
DEFINE_int32(copilot_port,
             rocketspeed::Copilot::DEFAULT_PORT,
             "copilot port number");
DEFINE_int32(copilot_workers, 40, "copilot worker threads");
DEFINE_string(control_towers,
              "localhost",
              "comma-separated control tower hostnames");
DEFINE_int32(copilot_connections, 40,
             "num connections between one copilot and one control tower");
DEFINE_int32(copilot_towers_per_log, 2,
             "number of towers to subscribe to for each log");
DEFINE_int64(copilot_timer_interval_micros, 500000,
             "microseconds between health check ticks");
DEFINE_int64(copilot_resubscriptions_per_second, 10000,
             "maximum number of orphaned topic resubscriptions per second");

// Rollcall settings
DEFINE_bool(rollcall, true, "enable RollCall");
DEFINE_int32(rollcall_max_batch_size_bytes, 16 << 10,
             "max rollcall message size (in bytes) before flush");
DEFINE_int32(rollcall_flush_latency_ms, 500,
             "time (milliseconds) to automatically flush rollcall writes");

// Supervisor settings
DEFINE_bool(supervisor, true, "start the supervisor");
DEFINE_int32(supervisor_port,
             rocketspeed::SupervisorLoop::DEFAULT_PORT,
             "supervisor port number");

DEFINE_bool(heartbeat_enabled, false, "enabled the stream heartbeat check");
DEFINE_int32(heartbeat_timeout,
             900,
             "heartbeat timeout for the idle streams, in seconds");
DEFINE_int32(heartbeat_expire_batch, -1 /* unbounded */,
             "number of streams to expire in one blocking call");

DEFINE_string(rs_log_dir, "", "directory for server logs");

DEFINE_int32(socket_buffer_size, 0,
             "The size of a send or receive window associated with a socket");

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

  // Update our local copy of the environment
  if (FLAGS_socket_buffer_size != 0) {
    env_options_.tcp_send_buffer_size = FLAGS_socket_buffer_size;
    env_options_.tcp_recv_buffer_size = FLAGS_socket_buffer_size;
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
    LOG_VITAL(info_log_,
      "Constructing MsgLoop port=%d workers=%d name=%s socketbuf=%d",
      port, workers, name.c_str(), env_options_.tcp_send_buffer_size);
    MsgLoop::Options options;
    options.event_loop.heartbeat_timeout =
      std::chrono::seconds(FLAGS_heartbeat_timeout);
    options.event_loop.heartbeat_expire_batch =
      FLAGS_heartbeat_expire_batch;
    options.event_loop.heartbeat_enabled = FLAGS_heartbeat_enabled;
    return new MsgLoop(env_,
                       env_options_,
                       port,
                       workers,
                       info_log_,
                       std::move(name),
                       std::move(options));
  };

  ControlTower* tower = nullptr;
  Pilot* pilot = nullptr;
  Copilot* copilot = nullptr;

  std::shared_ptr<MsgLoop> tower_loop;
  std::shared_ptr<MsgLoop> pilot_loop;
  std::shared_ptr<MsgLoop> copilot_loop;

  if (FLAGS_tower) {
    tower_loop.reset(make_msg_loop(FLAGS_tower_port,
                                   FLAGS_tower_workers,
                                   "tower"));
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

  // Create LogStorage and wrap if necessary.
  std::shared_ptr<LogStorage> storage;
  if (FLAGS_pilot || FLAGS_tower) {
    // Only need storage for pilot and control tower.
    LOG_VITAL(info_log_, "Creating LogStorage");
    storage = get_storage(env_, info_log_);

    // Wrap in the buffered storage if configured.
    if (FLAGS_buffered_storage_max_messages > 1 &&
        FLAGS_buffered_storage_max_bytes > 0 &&
        FLAGS_buffered_storage_max_latency_us) {
      LOG_VITAL(info_log_,
                "Using BufferedLogStorage, messages: %" PRIu64
                ", bytes: %" PRIu64 ", latency: %" PRIu64 " us",
                FLAGS_buffered_storage_max_messages,
                FLAGS_buffered_storage_max_bytes,
                FLAGS_buffered_storage_max_latency_us);
      LogStorage* raw_storage;
      Status st = BufferedLogStorage::Create(
          env_,
          info_log_,
          std::move(storage),
          pilot_loop.get(),
          FLAGS_buffered_storage_max_messages,
          FLAGS_buffered_storage_max_bytes,
          std::chrono::microseconds(FLAGS_buffered_storage_max_latency_us),
          &raw_storage);
      if (!st.ok()) {
        return st;
      }
      storage.reset(raw_storage);
    }
    if (!storage) {
      return Status::InternalError("Failed to construct log storage");
    }
  }

  // Create Control Tower.
  if (FLAGS_tower) {
    LOG_VITAL(info_log_, "Creating Control Tower");
    ControlTowerOptions tower_opts;
    tower_opts.msg_loop = tower_loop.get();
    tower_opts.info_log = info_log_;
    tower_opts.storage = storage;
    tower_opts.log_router = log_router;
    tower_opts.max_subscription_lag = FLAGS_tower_max_subscription_lag;
    tower_opts.readers_per_room = FLAGS_tower_readers_per_room;
    if (FLAGS_tower_cache_size != -1) {
      tower_opts.cache_size = FLAGS_tower_cache_size;
    }
    tower_opts.topic_tailer.FAULT_send_log_record_failure_rate =
      FLAGS_FAULT_tower_send_log_record_failure_rate;
    tower_opts.topic_tailer.min_reader_restart_duration =
      std::chrono::milliseconds(FLAGS_tower_min_reader_restart_ms);
    tower_opts.topic_tailer.max_reader_restart_duration =
      std::chrono::milliseconds(FLAGS_tower_max_reader_restart_ms);

    Status st = ControlTower::CreateNewInstance(std::move(tower_opts),
                                                &tower);
    if (!st.ok()) {
      return st;
    }
    tower_.reset(tower);
  }

  // Create Pilot.
  HostId pilot_host(
      HostId::CreateLocal(static_cast<uint16_t>(FLAGS_pilot_port)));
  if (FLAGS_pilot) {
    LOG_VITAL(info_log_, "Creating Pilot");
    PilotOptions pilot_opts;
    pilot_opts.msg_loop = pilot_loop.get();
    pilot_opts.info_log = info_log_;
    pilot_opts.storage = storage;
    pilot_opts.log_router = log_router;
    pilot_opts.FAULT_corrupt_extra_probability =
      FLAGS_FAULT_pilot_corrupt_extra_probability;

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
    copilot_opts.info_log = info_log_;
    copilot_opts.control_tower_connections = FLAGS_copilot_connections;
    copilot_opts.log_router = log_router;
    copilot_opts.rollcall_enabled = FLAGS_rollcall;
    copilot_opts.rollcall_max_batch_size_bytes =
      FLAGS_rollcall_max_batch_size_bytes;
    copilot_opts.rollcall_flush_latency =
      std::chrono::milliseconds(FLAGS_rollcall_flush_latency_ms);
    copilot_opts.timer_interval_micros = FLAGS_copilot_timer_interval_micros;
    copilot_opts.resubscriptions_per_second =
      FLAGS_copilot_resubscriptions_per_second;

    // TODO(pja) 1 : Configure control tower hosts from config file.
    // Parse comma-separated control_towers hostname.
    ControlTowerId node_id = 0;
    std::unordered_map<ControlTowerId, HostId> nodes;
    for (auto hostname : SplitString(FLAGS_control_towers)) {
      HostId host;
      auto st = HostId::Resolve(
          hostname, static_cast<uint16_t>(FLAGS_tower_port), &host);
      if (!st.ok()) {
        return st;
      }
      nodes.emplace(node_id, host);
      LOG_VITAL(info_log_, "Adding control tower '%s'",
        host.ToString().c_str());
      ++node_id;
    }
    copilot_opts.control_tower_router =
        std::make_shared<ConsistentHashTowerRouter>(
            std::move(nodes), 20, FLAGS_copilot_towers_per_log);
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
    supervisor_opts.tower = tower_.get(),
    supervisor_opts.copilot = copilot_.get();
    supervisor_opts.pilot = pilot_.get();

    Status st = SupervisorLoop::CreateNewInstance(std::move(supervisor_opts),
                                                  &supervisor_loop_);
    if (!st.ok()) {
      LOG_ERROR(info_log_, "Failed to create the supervisor loop");
    }
  }

  if (supervisor_loop_) {
    // initialize superisor loop
    Status st = supervisor_loop_->Initialize();
    if (!st.ok()) {
      LOG_ERROR(info_log_, "Failed to initialize the supervisor loop");
    }
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
  // Set of MsgLoops for all components.
  // std::set is used since pilot and copilot often share same MsgLoop, and
  // we only want to gather stats once.
  std::set<MsgLoop*> msg_loops;
  if (pilot_) {
    server_stats.Aggregate(pilot_->GetStatisticsSync());
    msg_loops.insert(pilot_->GetMsgLoop());
  }
  if (copilot_) {
    server_stats.Aggregate(copilot_->GetStatisticsSync());
    msg_loops.insert(copilot_->GetMsgLoop());
  }
  if (tower_) {
    server_stats.Aggregate(tower_->GetStatisticsSync());
    msg_loops.insert(tower_->GetMsgLoop());
  }
  for (MsgLoop* msg_loop : msg_loops) {
    server_stats.Aggregate(msg_loop->GetStatisticsSync());
  }
  return server_stats;
}

}  // namespace rocketspeed
