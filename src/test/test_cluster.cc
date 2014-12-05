// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/test/test_cluster.h"
#include <memory>
#include <utility>
#include <vector>
#include <stdio.h>
#include "src/util/logdevice.h"
#include "src/util/testharness.h"

#ifdef USE_LOGDEVICE
#include "external/logdevice/include/debug.h"
#endif

namespace rocketspeed {

LocalTestCluster::LocalTestCluster(std::shared_ptr<Logger> info_log,
  const std::string& storage_url) :
  pilot_(nullptr),
  copilot_(nullptr),
  control_tower_(nullptr),
  info_log_(info_log) {
  Env* env = Env::Default();
  EnvOptions env_options;
  Status st;

#ifdef USE_LOGDEVICE
#ifdef NDEBUG
  // Disable LogDevice info logging in release.
  facebook::logdevice::dbg::currentLevel =
    facebook::logdevice::dbg::Level::WARNING;
#endif
#endif

  // Range of logs to use.
  std::pair<LogID, LogID> log_range;
#ifdef USE_LOGDEVICE
  if (storage_url.empty()) {
    // Can only use one log as the logdevice test utils only supports that.
    // See T4894216
    log_range = std::pair<LogID, LogID>(1, 1);
  } else {
    log_range = std::pair<LogID, LogID>(1, 100000);
  }
#else
  // Something more substantial for the mock logdevice.
  log_range = std::pair<LogID, LogID>(1, 1000);
#endif

#ifdef USE_LOGDEVICE
  if (storage_url.empty()) {
    // Setup the local LogDevice cluster and create, client, and storage.
    logdevice_cluster_ = ld::IntegrationTestUtils::ClusterFactory().create(3);
    logdevice_client_ = logdevice_cluster_->createClient();
    LogDeviceStorage* storage = nullptr;
    st = LogDeviceStorage::Create(logdevice_client_, Env::Default(), &storage);
    ASSERT_TRUE(st.ok());
    logdevice_storage_.reset(storage);

    // Tell the pilot and control tower to use this storage interface instead
    // of opening a new one.
    pilot_options_.storage.reset(storage);
    control_tower_options_.storage.reset(storage);
  } else {
    // Just give the storage url to the components.
    pilot_options_.storage_url = storage_url;
    control_tower_options_.storage_url = storage_url;
  }
#endif

  control_tower_loop_.reset(new MsgLoop(env,
                                        env_options,
                                        ControlTower::DEFAULT_PORT,
                                        16,
                                        info_log_,
                                        "tower"));
  cockpit_loop_.reset(new MsgLoop(env,
                                  env_options,
                                  Copilot::DEFAULT_PORT,
                                  16,
                                  info_log_,
                                  "cockpit"));

  // Create ControlTower
  control_tower_options_.log_range = log_range;
  control_tower_options_.info_log = info_log_;
  control_tower_options_.number_of_rooms = 16;
  control_tower_options_.msg_loop = control_tower_loop_.get();
  st = ControlTower::CreateNewInstance(control_tower_options_, &control_tower_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create control tower.\n");
    return;
  }

  // Create Copilot
  copilot_options_.control_towers.push_back(control_tower_->GetClientId(0));
  copilot_options_.info_log = info_log_;
  copilot_options_.num_workers = 16;
  copilot_options_.msg_loop = cockpit_loop_.get();
  copilot_options_.control_tower_connections = cockpit_loop_->GetNumWorkers();
  st = Copilot::CreateNewInstance(copilot_options_, &copilot_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create copilot.\n");
    return;
  }

  // Create Pilot
  pilot_options_.log_range = log_range;
  pilot_options_.info_log = info_log_;
  pilot_options_.msg_loop = cockpit_loop_.get();
  st = Pilot::CreateNewInstance(pilot_options_, &pilot_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create pilot.\n");
    return;
  }

  // Start threads.
  control_tower_thread_ = std::thread([env, this]() {
    env->SetCurrentThreadName("tower");
    control_tower_loop_->Run();
  });

  cockpit_thread_ = std::thread([env, this]() {
    env->SetCurrentThreadName("cockpit");
    cockpit_loop_->Run();
  });

  // Wait for message loops to start.
  while (!control_tower_loop_->IsRunning() || !cockpit_loop_->IsRunning()) {
    std::this_thread::yield();
  }
}

LocalTestCluster::~LocalTestCluster() {
  // Stop message loops.
  cockpit_loop_->Stop();
  control_tower_loop_->Stop();

  // Join threads.
  if (cockpit_thread_.joinable()) {
    cockpit_thread_.join();
  }
  if (control_tower_thread_.joinable()) {
    control_tower_thread_.join();
  }

  // Delete all components (this stops their worker/room loops).
  delete pilot_;
  delete copilot_;
  delete control_tower_;
}

Statistics LocalTestCluster::GetStatistics() const {
  Statistics aggregated;
  if (pilot_) {
    aggregated.Aggregate(pilot_->GetStatistics());
  }
  aggregated.Aggregate(control_tower_loop_->GetStatistics());
  aggregated.Aggregate(cockpit_loop_->GetStatistics());
  // TODO(pja) 1 : Add copilot and control tower once they have stats.
  return std::move(aggregated);
}


}  // namespace rocketspeed
