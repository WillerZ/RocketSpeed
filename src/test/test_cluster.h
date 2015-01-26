// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <utility>

#include "include/Types.h"
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
                            const std::string& storage_url = "");

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

  // Get the pilot host IDs.
  std::vector<HostId> GetPilotHostIds() const {
    assert(pilot_);
    return std::vector<HostId>{ pilot_->GetHostId() };
  }

  // Get the copilot host IDs.
  std::vector<HostId> GetCopilotHostIds() const {
    assert(copilot_);
    return std::vector<HostId>{ copilot_->GetHostId() };
  }

  ControlTower* GetControlTower() {
    return control_tower_;
  }

  Statistics GetStatistics() const;

 private:
  struct LogDevice;
  std::unique_ptr<LogDevice> logdevice_;

  // General cluster status.
  Status status_;
  // Environment used by the cluster.
  Env* env_;
  // Logger.
  std::shared_ptr<Logger> info_log_;

  // Pilot
  PilotOptions pilot_options_;
  Pilot* pilot_;

  // Copilot
  CopilotOptions copilot_options_;
  Copilot* copilot_;

  // Control Tower
  ControlTowerOptions control_tower_options_;
  ControlTower* control_tower_;

  // Message loops and threads
  std::unique_ptr<MsgLoop> cockpit_loop_;
  std::thread cockpit_thread_;
  std::unique_ptr<MsgLoop> control_tower_loop_;
  std::thread control_tower_thread_;
};

}  // namespace rocketspeed
