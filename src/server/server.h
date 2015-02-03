// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>
#include <utility>
#include "src/util/storage.h"
#include "src/port/Env.h"

// These ones are in server.cc
DECLARE_bool(tower);
DECLARE_bool(pilot);
DECLARE_bool(copilot);
DECLARE_int32(pilot_port);
DECLARE_int32(copilot_port);

namespace rocketspeed {

// Run the RocketSpeed server.
extern int Run(int argc,
               char** argv,
               std::function<std::shared_ptr<LogStorage>(Env*)> get_storage,
               std::function<std::shared_ptr<LogRouter>()> get_router,
               Env* env,
               EnvOptions env_options);

}  // namespace rocketspeed
