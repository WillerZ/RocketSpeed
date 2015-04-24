// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <utility>

#include "include/Types.h"
#include "src/client/client.h"
#include "src/controltower/options.h"
#include "src/controltower/tower.h"
#include "src/copilot/copilot.h"
#include "src/copilot/options.h"
#include "src/pilot/options.h"
#include "src/pilot/pilot.h"
#include "src/messages/msg_loop.h"
#include "src/util/storage.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

/**
 * Creates a test cluster consisting of one pilot, one copilot, one control
 * tower, and a logdevice instance, all running on the same process. This is
 * useful for mock end-to-end tests.
 */
class LocalTestCluster {
 public:
  struct Options {
    std::shared_ptr<Logger> info_log;
    bool start_controltower = true;
    bool start_copilot = true;
    bool start_pilot = true;
    bool single_log = false;
    std::string storage_url;
    Env* env = Env::Default();
    PilotOptions pilot;
    CopilotOptions copilot;
    ControlTowerOptions tower;
    int controltower_port = ControlTower::DEFAULT_PORT;
    int copilot_port = Copilot::DEFAULT_PORT;
    int pilot_port = Pilot::DEFAULT_PORT;
  };

  /**
   * Constructs a new test cluster. Once this has finished constructing, the
   * pilot, copilot, and control tower will be running.
   *
   * @param info_log The logger for server informational messages.
   * @param start_controltower Whether or not to start CT.
   * @param start_copilot Whether or not to start Copilot.
   * @param start_pilot Whether or not to start Pilot.
   * @param storage_url URL of logdevice config. Leave blank to use test util.
   */
  explicit LocalTestCluster(std::shared_ptr<Logger> info_log,
                            bool start_controltower = true,
                            bool start_copilot = true,
                            bool start_pilot = true,
                            const std::string& storage_url = "",
                            Env* env = Env::Default()
                            );

  /**
   * Create a new test cluster using provided options.
   *
   * @param opts Options for construction.
   */
  explicit LocalTestCluster(Options opts);

  /**
   * Stops all parts of the test cluster.
   */
  ~LocalTestCluster();

  /**
   * Gets status which reflects last (if any) error condition encountered in the
   * cluster.
   */
  Status GetStatus() const {
    return status_;
  }

  /**
   * Creates a Configuration, which can be used by a client to talk to the
   * test cluster.
   */
  std::shared_ptr<Configuration> GetConfiguration() const;

  Pilot* GetPilot() {
    return pilot_;
  }

  Copilot* GetCopilot() {
    return copilot_;
  }

  ControlTower* GetControlTower() {
    return control_tower_;
  }

  std::shared_ptr<LogStorage> GetLogStorage();

  std::shared_ptr<LogRouter> GetLogRouter();

  Statistics GetStatistics() const;

  // Returns the environment used by the TestCluster
  Env* GetEnv() const { return env_; }

  // Create a new client for the test cluster.
  Status CreateClient(std::unique_ptr<ClientImpl>* client,
                      bool is_internal);

 private:
  void Initialize(Options opts);

  struct LogDevice;
  std::unique_ptr<LogDevice> logdevice_;

  // General cluster status.
  Status status_;
  // Environment used by the cluster.
  Env* env_;
  // Logger.
  std::shared_ptr<Logger> info_log_;

  Pilot* pilot_;
  Copilot* copilot_;
  ControlTower* control_tower_;

  // Configuration generated here
  std::unique_ptr<Configuration> configuration_;

  // Message loops and threads
  std::unique_ptr<MsgLoop> cockpit_loop_;
  BaseEnv::ThreadId cockpit_thread_;
  std::unique_ptr<MsgLoop> control_tower_loop_;
  BaseEnv::ThreadId control_tower_thread_;
};

}  // namespace rocketspeed
