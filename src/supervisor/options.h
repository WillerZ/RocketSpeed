// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>

namespace rocketspeed {

class ControlTower;
class Copilot;
class Logger;
class Pilot;

// Options used for initializing/running the SupervisorLoop.
struct SupervisorOptions {
  // Port used by supervisor
  uint32_t port;

  // Log everything using this logger, if specified
  std::shared_ptr<Logger> info_log;

  // Msg Loops (options does not own these pointers)
  ControlTower* tower;
  Pilot* pilot;
  Copilot* copilot;
};

}
