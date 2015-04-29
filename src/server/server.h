// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>
#include <utility>

// These ones are in server.cc
DECLARE_bool(tower);
DECLARE_bool(pilot);
DECLARE_bool(copilot);
DECLARE_int32(pilot_port);
DECLARE_int32(copilot_port);

DECLARE_string(loglevel);
DECLARE_string(rs_log_dir);
DECLARE_bool(log_to_stderr);

namespace rocketspeed {

class Env;
class EnvOptions;
class Logger;
class LogRouter;
class LogStorage;

// Run the RocketSpeed server.
extern int Run(int argc,
               char** argv,
               std::function<std::shared_ptr<LogStorage>(
                 Env*, std::shared_ptr<Logger>)> get_storage,
               std::function<std::shared_ptr<LogRouter>()> get_router,
               Env* env,
               EnvOptions env_options);

}  // namespace rocketspeed
