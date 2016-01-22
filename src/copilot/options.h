// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include "include/Types.h"
#include "include/Env.h"
#include "include/HostId.h"

namespace rocketspeed {

class ControlTowerRouter;
class LogRouter;
class MsgLoop;

struct CopilotOptions {
  //
  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env;

  // The options for the environment
  EnvOptions env_options;

  // The configuration of this rocketspeed instance
  Configuration* conf;

  // Message loop for this copilot.
  // This is not owned by the copilot and should not be deleted.
  MsgLoop* msg_loop;

  // If non-null, then server info logs are written to this object.
  // If null, then server info logs are written to log_dir.
  // This allows multiple instances of the server to log to the
  // same object.
  // Default: nullptr
  std::shared_ptr<Logger> info_log;

  // Logging level of server logs
  // Default: INFO_LEVEL
  InfoLogLevel info_log_level;

  // The relative path name from the copilot's current working dir
  // where info logs are stored
  // Default: "" (store logs in current working directory)
  std::string log_dir;

  // Specify the maximal size of the info log file. If the log file
  // is larger than `max_log_file_size`, a new info log file will
  // be created.
  // If max_log_file_size == 0, all logs will be written to one
  // log file.
  size_t max_log_file_size;

  // Time for the info log file to roll (in seconds).
  // If specified with non-zero value, log file will be rolled
  // if it has been active longer than `log_file_time_to_roll`.
  // Default: 0 (disabled)
  size_t log_file_time_to_roll;

  // Control Tower router
  std::shared_ptr<ControlTowerRouter> control_tower_router;

  // Pilot hostids (needed for writing rollcall topic).
  std::vector<HostId> pilots;

  // Number of connections between this copilot and each control tower.
  // Should be <= number of message loop workers.
  // Default: 4
  uint32_t control_tower_connections;

  // Log router.
  std::shared_ptr<LogRouter> log_router;

  // Is RollCall enabled?
  // Default: true
  bool rollcall_enabled;

  // Threshold in message size for when to automatically flush Rollcall batch.
  // Batches will be flushed when the publish size reaches this limit, or the
  // age of the batch reaches rollcall_flush_latency, whichever happens first.
  // Default: 16KB
  size_t rollcall_max_batch_size_bytes;

  // Time to wait before flushing a rollcall batch.
  // Default: 500ms
  std::chrono::milliseconds rollcall_flush_latency;

  // Time between health check ticks.
  // Default: 500,000 (0.5s)
  uint64_t timer_interval_micros;

  // Maximum number of re-subscriptions per second.
  // Default: 10,000
  uint64_t resubscriptions_per_second;

  // Age of tower subscription before checking we are connected to the
  // correct control towers. We may be subscribed to incorrect control tower
  // in cases where control tower goes down and we fail-over to another.
  // For long-lived subscriptions, this can create imbalance on control towers.
  // Default: 10 minutes
  std::chrono::seconds tower_subscriptions_check_period;

  // Max number of tower subscriptions to rebalance at once.
  // Default: 1000
  int rebalances_per_second;

  // Create CopilotOptions with default values for all fields
  CopilotOptions();
};

}  // namespace rocketspeed
