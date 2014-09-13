// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <unistd.h>
#include <string>
#include <utility>
#include "include/Env.h"
#include "include/Types.h"
#include "src/util/storage.h"

namespace rocketspeed {

struct ControlTowerOptions {
  //
  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env;

  // The options for the environment
  EnvOptions env_options;

  // The configuration of this rocketspeed instance
  // Default: TODO(dhruba) 1234
  Configuration* conf;

  // The machine name that identifies this control tower
  std::string hostname;

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

  // If log_dir has the default value, then log files are created in the
  // current working directory. If log_dir not is not the default value,
  // then logs are created in the specified directory.
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

  // The number of rooms in the Control Tower. Each room serves
  // a subset of all the topics served by this Tower. This is a
  // easy way to partition all the topics into parallelizable
  // processing units.
  // Default: number of cpus on the machine
  unsigned int number_of_rooms;

  // Range of log IDs to use.
  // Warning: this should be picked up from Configuration
  std::pair<LogID, LogID> log_range;

  // Create ControlTowerOptions with default values for all fields
  ControlTowerOptions();
};

}  // namespace rocketspeed