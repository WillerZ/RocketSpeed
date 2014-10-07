// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include "include/Env.h"
#include "include/Types.h"
#include "src/util/storage.h"

namespace rocketspeed {

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

  // The machine name that identifies this copilot
  std::string copilotname;

  // The port number for this service
  int port_number;

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

  // Range of log IDs to use.
  // Warning: changing this range will change the mapping of topics to logs.
  std::pair<LogID, LogID> log_range;

  // Control Tower host IDs.
  std::vector<HostId> control_towers;

  // Number of worker loops for the pilot.
  uint32_t num_workers;

  // Size of the worker queue (number of commands in flight).
  uint32_t worker_queue_size;

  // Number of replicas for the consistent hash ring for control tower lookup.
  // Trade-off: higher means better distribution of log IDs to control towers,
  // but also higher memory usage (linear with num replicas), and slower
  // lookups (logarithmic).
  uint32_t consistent_hash_replicas;

  // Each topic should be tailed by this many control towers.
  uint32_t control_towers_per_log;

  // Create CopilotOptions with default values for all fields
  CopilotOptions();
};

}  // namespace rocketspeed
