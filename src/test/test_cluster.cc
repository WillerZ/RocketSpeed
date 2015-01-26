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

#include "src/logdevice/storage.h"

#ifdef USE_LOGDEVICE
#include "logdevice/include/debug.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"
#endif  // USE_LOGDEVICE

namespace rocketspeed {

struct LocalTestCluster::LogDevice {
#ifdef USE_LOGDEVICE
  // LogDevice cluster and client.
  std::unique_ptr<facebook::logdevice::IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<facebook::logdevice::Client> client_;
  std::shared_ptr<LogStorage> storage_;
#endif  // USE_LOGDEVICE
};

LocalTestCluster::LocalTestCluster(std::shared_ptr<Logger> info_log,
                                   bool start_controltower,
                                   bool start_copilot,
                                   bool start_pilot,
                                   const std::string& storage_url)
    : logdevice_(new LogDevice)
    , env_(Env::Default())
    , info_log_(info_log)
    , pilot_(nullptr)
    , copilot_(nullptr)
    , control_tower_(nullptr) {
  if (start_copilot && !start_controltower) {
    status_ = Status::InvalidArgument("Copilot needs ControlTower.");
    return;
  }
#ifdef USE_LOGDEVICE
#ifdef NDEBUG
  // Disable LogDevice info logging in release.
  facebook::logdevice::dbg::currentLevel =
      facebook::logdevice::dbg::Level::WARNING;
#endif  // NDEBUG
#endif  // USE_LOGDEVICE

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
#endif  // USE_LOGDEVICE

#ifdef USE_LOGDEVICE
  if (storage_url.empty()) {
    // Setup the local LogDevice cluster and create, client, and storage.
    logdevice_->cluster_ =
        facebook::logdevice::IntegrationTestUtils::ClusterFactory().create(3);
    logdevice_->client_ = logdevice_->cluster_->createClient();
    LogDeviceStorage* storage = nullptr;
    status_ = LogDeviceStorage::Create(logdevice_->client_, env_, &storage);
    if (!status_.ok()) {
      LOG_ERROR(info_log_, "Failed to create LogDeviceStorage.");
      return;
    }
    logdevice_->storage_.reset(storage);
    storage = nullptr;

    // Tell the pilot and control tower to use this storage interface instead
    // of opening a new one.
    pilot_options_.storage = logdevice_->storage_;
    control_tower_options_.storage = logdevice_->storage_;
  } else {
    // Just give the storage url to the components.
    pilot_options_.storage_url = storage_url;
    control_tower_options_.storage_url = storage_url;
  }
#endif  // USE_LOGDEVICE

  EnvOptions env_options;

  if (start_controltower) {
    control_tower_loop_.reset(new MsgLoop(
        env_, env_options, ControlTower::DEFAULT_PORT, 16, info_log_, "tower"));

    // Create ControlTower
    control_tower_options_.log_range = log_range;
    control_tower_options_.info_log = info_log_;
    control_tower_options_.number_of_rooms = 16;
    control_tower_options_.msg_loop = control_tower_loop_.get();
    status_ = ControlTower::CreateNewInstance(control_tower_options_,
                                              &control_tower_);
    if (!status_.ok()) {
      LOG_ERROR(info_log_, "Failed to create ControlTower.");
      return;
    }

    // Start threads.
    control_tower_thread_ =
        std::thread([this]() { control_tower_loop_->Run(); });

    // Wait for message loop to start.
    while (!control_tower_loop_->IsRunning()) {
      std::this_thread::yield();
    }
  }

  if (start_copilot || start_pilot) {
    static_assert(Copilot::DEFAULT_PORT == Pilot::DEFAULT_PORT,
                  "Default port for pilot and copilot differ.");
    cockpit_loop_.reset(new MsgLoop(
        env_, env_options, Copilot::DEFAULT_PORT, 16, info_log_, "cockpit"));

    if (start_copilot) {
      // Create Copilot
      copilot_options_.control_towers.push_back(control_tower_->GetClientId(0));
      copilot_options_.info_log = info_log_;
      copilot_options_.num_workers = 16;
      copilot_options_.msg_loop = cockpit_loop_.get();
      copilot_options_.control_tower_connections =
          cockpit_loop_->GetNumWorkers();
      status_ = Copilot::CreateNewInstance(copilot_options_, &copilot_);
      if (!status_.ok()) {
        LOG_ERROR(info_log_, "Failed to create Copilot.");
        return;
      }
    }

    if (start_pilot) {
      // Create Pilot
      pilot_options_.log_range = log_range;
      pilot_options_.info_log = info_log_;
      pilot_options_.msg_loop = cockpit_loop_.get();
      status_ = Pilot::CreateNewInstance(pilot_options_, &pilot_);
      if (!status_.ok()) {
        LOG_ERROR(info_log_, "Failed to create Pilot.");
        return;
      }
    }

    cockpit_thread_ = std::thread([this]() { cockpit_loop_->Run(); });

    // Wait for message loop to start.
    while (!cockpit_loop_->IsRunning()) {
      std::this_thread::yield();
    }
  }
}

LocalTestCluster::~LocalTestCluster() {
  // Stop message loops.
  if (cockpit_loop_) {
    cockpit_loop_->Stop();
  }
  if (control_tower_loop_) {
    control_tower_loop_->Stop();
  }

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
  if (control_tower_loop_) {
    aggregated.Aggregate(control_tower_loop_->GetStatistics());
  }
  if (cockpit_loop_) {
    aggregated.Aggregate(cockpit_loop_->GetStatistics());
  }
  // TODO(pja) 1 : Add copilot and control tower once they have stats.
  return std::move(aggregated);
}

}  // namespace rocketspeed
