// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/util/storage.h"

namespace rocketspeed {

class MsgLoop;

struct PilotOptions {
  //
  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env;

  // The options for the environment
  EnvOptions env_options;

  // The configuration of this rocketspeed instance
  Configuration* conf;

  // Message loop for this pilot.
  // This is not owned by the pilot and should not be deleted.
  MsgLoop* msg_loop;

  // The machine name that identifies this pilot
  std::string pilotname;

  // If non-null, then server info logs are written to this object.
  // If null, then server info logs are written to log_dir.
  // This allows multiple instances of the server to log to the
  // same object.
  // Default: nullptr
  std::shared_ptr<Logger> info_log;

  // Logging level of server logs
  // Default: INFO_LEVEL
  InfoLogLevel info_log_level;

  // The relative path name from the pilot's current working dir
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

  // Pointer to persistent log storage to use.
  // If null, open a new storage interface using the storage_url.
  std::shared_ptr<LogStorage> storage;

  // A URL that identifies a storage configuration resource that
  // describes the storage cluster the controltower will use.
  // The only supported formats are currently
  // file:<path-to-configuration-file> and
  // configerator:<configerator-path>. Examples:
  //   "file:logdevice.test.conf"
  //   "configerator:logdevice/logdevice.test.conf"
  URL storage_url;

  // Range of log IDs to use.
  // Warning: changing this range will change the mapping of topics to logs.
  std::pair<LogID, LogID> log_range;

  // Number of worker loops for the pilot.
  uint32_t num_workers;

  // Size of the worker queue (number of commands in flight).
  uint32_t worker_queue_size;

  // Create PilotOptions with default values for all fields
  PilotOptions();
};

}  // namespace rocketspeed
