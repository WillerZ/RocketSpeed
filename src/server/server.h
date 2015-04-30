// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#include <gflags/gflags.h>
#include "src/port/Env.h"

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

class ControlTower;
class Copilot;
class Logger;
class LogRouter;
class LogStorage;
class MsgLoop;
class Pilot;

class RocketSpeed {
 public:
  /**
   * Creates a RocketSpeed server. Uses options from gflags.
   *
   * @param env Environment.
   * @param env_options Environment options.
   */
  explicit RocketSpeed(Env* env,
                       EnvOptions env_options);

  /**
   * Shutdown all services.
   *
   * Pre-condition: Stop() must be called first.
   */
  ~RocketSpeed();

  /**
   * Initializes all services.
   *
   * @param get_storage Function used to create LogStorage.
   * @param get_router Function used to create LogRouter.
   * @return ok() if successful, otherwise error.
   */
  Status Initialize(std::function<std::shared_ptr<LogStorage>(
                      Env*, std::shared_ptr<Logger>)> get_storage,
                    std::function<std::shared_ptr<LogRouter>()> get_router);

  /**
   * Runs the server until error or explicit shutdown.
   */
  void Run();

  /**
   * Synchronously and gracefully stops the server.
   */
  void Stop();

  /** Info Log */
  std::shared_ptr<Logger> GetInfoLog() { return info_log_; }

  /** Pilot */
  Pilot* GetPilot() { return pilot_.get(); }

  /** Copilot */
  Copilot* GetCopilot() { return copilot_.get(); }

  /** ControlTower */
  ControlTower* GetControlTower() { return tower_.get(); }

 protected:
  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;
  std::unique_ptr<Pilot> pilot_;
  std::unique_ptr<Copilot> copilot_;
  std::unique_ptr<ControlTower> tower_;
  std::vector<std::thread> threads_;
  std::vector<std::shared_ptr<MsgLoop>> msg_loops_;
};

}  // namespace rocketspeed
