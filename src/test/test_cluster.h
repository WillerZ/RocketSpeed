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
#include "src/logdevice/storage.h"
#include "src/logdevice/log_router.h"

namespace rocketspeed {

struct TestStorage {
 public:
  virtual ~TestStorage() {}
  virtual std::shared_ptr<LogStorage> GetLogStorage() = 0;
  virtual std::shared_ptr<LogRouter> GetLogRouter() = 0;

 protected:
  TestStorage() {}
};

/**
 * Creates a test cluster consisting of one pilot, one copilot, one control
 * tower, and a logdevice instance, all running on the same process. This is
 * useful for mock end-to-end tests.
 */
class LocalTestCluster {
 public:
  struct Options {
    Options() {
      // Flush aggressively for tests.
      copilot.rollcall_max_batch_size_bytes = 100;
      copilot.rollcall_flush_latency = std::chrono::milliseconds(20);
      copilot.timer_interval_micros = 100000;
    }

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
    // Allocate ports automatically.
    int controltower_port = 0;
    int cockpit_port = 0;
    std::shared_ptr<LogDeviceStorage> log_storage;
    std::shared_ptr<LogDeviceLogRouter> log_router;
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
   * Creates storage.
   */
  static std::unique_ptr<TestStorage>
  CreateStorage(Env* env,
                std::shared_ptr<Logger> info_log,
                std::pair<LogID, LogID> log_range,
                std::string storage_url = "");

  Pilot* GetPilot() {
    return pilot_;
  }

  Copilot* GetCopilot() {
    return copilot_;
  }

  ControlTower* GetControlTower() {
    return control_tower_;
  }

  MsgLoop* GetCockpitLoop() {
    return cockpit_loop_.get();
  }

  MsgLoop* GetControlTowerLoop() {
    return control_tower_loop_.get();
  }

  std::shared_ptr<LogStorage> GetLogStorage();

  std::shared_ptr<LogRouter> GetLogRouter();

  Statistics GetStatisticsSync() const;

  // Returns the environment used by the TestCluster
  Env* GetEnv() const { return env_; }

  Status CreateClient(std::unique_ptr<ClientImpl>* client,
                      bool is_internal);

  /**
   * Creates a new client for the test cluster.
   * Sets default PublisherRouter and ShardingStrategy.
   */
  Status CreateClient(std::unique_ptr<Client>* client);

  /**
   * Creates a new client for the test cluster with options.
   * If PublisherRouter and ShardingStrategy are not set, it sets them to
   * default.
   */
  Status CreateClient(std::unique_ptr<Client>* client,
                      ClientOptions options);

 private:
  void Initialize(Options opts);

  std::unique_ptr<TestStorage> storage_;

  // General cluster status.
  Status status_;
  // Environment used by the cluster.
  Env* env_;
  // Logger.
  std::shared_ptr<Logger> info_log_;

  Pilot* pilot_;
  Copilot* copilot_;
  ControlTower* control_tower_;

  // Message loops and threads
  std::unique_ptr<MsgLoop> cockpit_loop_;
  BaseEnv::ThreadId cockpit_thread_;
  std::unique_ptr<MsgLoop> control_tower_loop_;
  BaseEnv::ThreadId control_tower_thread_;
};

}  // namespace rocketspeed
