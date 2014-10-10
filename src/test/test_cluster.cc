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

  // Create ControlTower
  control_tower_options_.log_range = log_range;
  control_tower_options_.info_log = info_log;
  control_tower_options_.number_of_rooms = 4;
  st = ControlTower::CreateNewInstance(control_tower_options_, &control_tower_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create control tower.\n");
    return;
  }

  // Create Copilot
  HostId control_tower_host(control_tower_options_.hostname,
                            control_tower_options_.port_number);
  copilot_options_.control_towers.push_back(control_tower_host);
  copilot_options_.info_log = info_log;
  copilot_options_.num_workers = 4;
  st = Copilot::CreateNewInstance(copilot_options_, &copilot_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create copilot.\n");
    return;
  }

  // Create Pilot
  pilot_options_.log_range = log_range;
  pilot_options_.info_log = info_log;
  pilot_options_.num_workers = 4;
  st = Pilot::CreateNewInstance(pilot_options_, &pilot_);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create pilot.\n");
    return;
  }

  // Start threads.
  pilot_thread_ = std::thread([env, this]() {
    env->SetThreadName(env->GetCurrentThreadId(), "pilot");
    pilot_->Run();
  });

  copilot_thread_ = std::thread([env, this]() {
    env->SetThreadName(env->GetCurrentThreadId(), "copilot");
    copilot_->Run();
  });

  control_tower_thread_ = std::thread([env, this]() {
    env->SetThreadName(env->GetCurrentThreadId(), "tower");
    control_tower_->Run();
  });

  // Wait for message loops to start.
  while (!pilot_->IsRunning() ||
         !copilot_->IsRunning() ||
         !control_tower_->IsRunning()) {
    std::this_thread::yield();
  }
}

LocalTestCluster::~LocalTestCluster() {
  // Delete all components (this stops their message loop).
  delete pilot_;
  delete copilot_;
  delete control_tower_;

  // Join threads.
  if (pilot_thread_.joinable()) {
    pilot_thread_.join();
  }
  if (copilot_thread_.joinable()) {
    copilot_thread_.join();
  }
  if (control_tower_thread_.joinable()) {
    control_tower_thread_.join();
  }
}

}  // namespace rocketspeed
