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

namespace rocketspeed {

LocalTestCluster::LocalTestCluster(const std::string& storage_url) :
  pilot_(nullptr),
  copilot_(nullptr),
  control_tower_(nullptr) {
  Env* env = Env::Default();
  EnvOptions env_options;
  Status st;

  // Range of logs to use.
  std::pair<LogID, LogID> log_range;
#ifdef USE_LOGDEVICE
  if (storage_url.empty()) {
    // Can only use one log as the logdevice test utils only supports that.
    // See T4894216
    log_range = std::pair<LogID, LogID>(1, 1);
  } else {
    log_range = std::pair<LogID, LogID>(1, 1000);
  }
#else
  // Something more substantial for the mock logdevice.
  log_range = std::pair<LogID, LogID>(1, 1000);
#endif

#ifdef USE_LOGDEVICE
  LogDeviceStorage* storage = nullptr;
  if (storage_url.empty()) {
    // Setup the local LogDevice cluster and create, client, and storage.
    logdevice_cluster_ = ld::IntegrationTestUtils::ClusterFactory().create(3);
    logdevice_client_ = logdevice_cluster_->createClient();
    st = LogDeviceStorage::Create(logdevice_client_, Env::Default(), &storage);
    ASSERT_TRUE(st.ok());
  } else {
    // Create log device client
    std::unique_ptr<facebook::logdevice::ClientSettings> clientSettings(
      facebook::logdevice::ClientSettings::create());
    rocketspeed::LogDeviceStorage::Create(
      "rocketspeed.logdevice.primary",
      storage_url,
      "",
      std::chrono::milliseconds(1000),
      std::move(clientSettings),
      env,
      &storage);
  }
  logdevice_storage_.reset(storage);

  // Tell the pilot and control tower to use this storage interface instead
  // of opening a new one.
  pilot_options_.storage.reset(storage);
  control_tower_options_.storage.reset(storage);
#endif

  // Create info log
  std::shared_ptr<Logger> info_log;
  st = CreateLoggerFromOptions(Env::Default(),
                               "",
                               "LOG",
                               0,
                               0,
#ifdef NDEBUG
                               WARN_LEVEL,
#else
                               INFO_LEVEL,
#endif
                               &info_log);
  if (!st.ok()) {
    info_log = nullptr;
  }

  control_tower_loop_.reset(new MsgLoop(env,
                                        env_options,
                                        ControlTower::DEFAULT_PORT,
                                        info_log));
  cockpit_loop_.reset(new MsgLoop(env,
                                  env_options,
                                  Copilot::DEFAULT_PORT,
                                  info_log));

  // Create ControlTower
  control_tower_options_.log_range = log_range;
  control_tower_options_.info_log = info_log;
  control_tower_options_.number_of_rooms = 4;
  control_tower_options_.msg_loop = control_tower_loop_.get();
  st = ControlTower::CreateNewInstance(control_tower_options_, &control_tower_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create control tower.\n");
    return;
  }

  // Create Copilot
  copilot_options_.control_towers.push_back(control_tower_->GetHostId());
  copilot_options_.info_log = info_log;
  copilot_options_.num_workers = 4;
  copilot_options_.msg_loop = cockpit_loop_.get();
  st = Copilot::CreateNewInstance(copilot_options_, &copilot_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create copilot.\n");
    return;
  }

  // Create Pilot
  pilot_options_.log_range = log_range;
  pilot_options_.info_log = info_log;
  pilot_options_.num_workers = 4;
  pilot_options_.msg_loop = cockpit_loop_.get();
  st = Pilot::CreateNewInstance(pilot_options_, &pilot_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create pilot.\n");
    return;
  }

  // Start threads.
  control_tower_thread_ = std::thread([env, this]() {
    env->SetThreadName(env->GetCurrentThreadId(), "tower");
    control_tower_loop_->Run();
  });

  cockpit_thread_ = std::thread([env, this]() {
    env->SetThreadName(env->GetCurrentThreadId(), "cockpit");
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

}  // namespace rocketspeed
