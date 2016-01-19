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

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif
namespace rocketspeed {

class Logger;
class Configuration;

struct ProxyOptions {
  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  BaseEnv* env;

  // The options for the environment.
  EnvOptions env_options;

  // The configuration of the Rocketspeed instance.
  std::shared_ptr<Configuration> conf;

  // Info logs are written to this object.
  std::shared_ptr<Logger> info_log;

  // Number of worker threads.
  // Default: 1
  int num_workers;

  // An upper bound on the number of messages that arrived out of order.
  // Default: 16
  int ordering_buffer_size;

  // Create PilotOptions with default values for all fields
  ProxyOptions();
};

}  // namespace rocketspeed
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
