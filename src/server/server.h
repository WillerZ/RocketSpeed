// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <utility>
#include "src/util/storage.h"
#include "src/port/Env.h"

namespace rocketspeed {

// Run the RocketSpeed server.
extern int Run(int argc,
               char** argv,
               std::shared_ptr<LogStorage> storage,
               std::pair<LogID, LogID> log_range,
               Env* env,
               EnvOptions env_options);

}  // namespace rocketspeed
