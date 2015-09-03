// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <unistd.h>
#include <string>
#include <utility>
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/util/storage.h"

namespace rocketspeed {

class MsgLoop;

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

  // Message loop for this control tower.
  // This is not owned by the control tower and should not be deleted.
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

  // Pointer to persistent log storage to use.
  std::shared_ptr<LogStorage> storage;

  // Log router.
  std::shared_ptr<LogRouter> log_router;

  // Maximum number of sequence numbers that a subscription can lag behind
  // before being sent a gap. This is to ensure that (a) subscribers regularly
  // receive updates for each topic, even if there are no records, and (b) that
  // temporary disconnections don't result in excessive rewind.
  // Default: 10K
  int64_t max_subscription_lag;

  // Maximum number of readers on a single log per room.
  // Default: 2
  size_t readers_per_room;

  // Options for TopicTailer
  struct TopicTailer {
    // Probability of failing to enqueue a log record to the TopicTailer queue.
    // For testing the log storage backoff/flow control.
    double FAULT_send_log_record_failure_rate = 0.0;
  } topic_tailer;

  // Create ControlTowerOptions with default values for all fields
  ControlTowerOptions();
};

}  // namespace rocketspeed
