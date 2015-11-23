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
#include "src/logdevice/log_router.h"
#include "src/util/common/fixed_configuration.h"
#include "src/util/control_tower_router.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow"
#ifdef USE_LOGDEVICE
#include "logdevice/include/debug.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"
#else
#include "src/logdevice/Common.h"
#endif  // USE_LOGDEVICE
#pragma GCC diagnostic pop

namespace rocketspeed {

struct TestStorageImpl : public TestStorage {
 public:
  ~TestStorageImpl() {
#ifdef USE_LOGDEVICE
    client_.reset();
#endif
    RS_ASSERT(storage_.unique());  // should be last reference
  }

  std::shared_ptr<LogStorage> GetLogStorage() override {
    return storage_;
  }

  std::shared_ptr<LogRouter> GetLogRouter() override {
    return log_router_;
  }

#ifdef USE_LOGDEVICE
  // LogDevice cluster and client.
  static std::unique_ptr<facebook::logdevice::IntegrationTestUtils::Cluster>
    cluster_;
  std::shared_ptr<facebook::logdevice::Client> client_;
#endif  // USE_LOGDEVICE
  std::shared_ptr<LogDeviceStorage> storage_;
  std::shared_ptr<LogDeviceLogRouter> log_router_;
};

#ifdef USE_LOGDEVICE
std::unique_ptr<facebook::logdevice::IntegrationTestUtils::Cluster>
  TestStorageImpl::cluster_;
#endif  // USE_LOGDEVICE

LocalTestCluster::LocalTestCluster(std::shared_ptr<Logger> info_log,
                                   bool start_controltower,
                                   bool start_copilot,
                                   bool start_pilot,
                                   const std::string& storage_url,
                                   Env* env) {
  Options opts;
  opts.info_log = info_log;
  opts.start_controltower = start_controltower;
  opts.start_copilot = start_copilot;
  opts.start_pilot = start_pilot;
  opts.storage_url = storage_url;
  opts.env = env;
  Initialize(opts);
}

LocalTestCluster::LocalTestCluster(Options opts) {
  Initialize(opts);
}

std::unique_ptr<TestStorage>
LocalTestCluster::CreateStorage(Env* env,
                                std::shared_ptr<Logger> info_log,
                                std::pair<LogID, LogID> log_range,
                                std::string storage_url) {
  std::unique_ptr<TestStorageImpl> test_storage(new TestStorageImpl);
  Status st;
  LogDeviceStorage* storage = nullptr;

#ifdef USE_LOGDEVICE
  if (storage_url.empty()) {
    // Setup the local LogDevice cluster and create, client, and storage.
    if (!TestStorageImpl::cluster_) {
      TestStorageImpl::cluster_ =
        facebook::logdevice::IntegrationTestUtils::ClusterFactory()
          .setNumLogs(log_range.second - log_range.first + 1)
          .create(3);
    }
    test_storage->client_ = TestStorageImpl::cluster_->createClient();
    st = LogDeviceStorage::Create(test_storage->client_,
                                  env,
                                  info_log,
                                  &storage);
  } else {
    st = LogDeviceStorage::Create("rocketspeed.logdevice.primary",
                                  storage_url,
                                  "",
                                  std::chrono::milliseconds(1000),
                                  4,
                                  1024 * 1024,
                                  "none",
                                  "",
                                  env,
                                  info_log,
                                  &storage);
  }
#else
  static bool first_cluster = true;
  if (first_cluster) {
    Env::Default()->DeleteDirRecursive(facebook::logdevice::MOCK_LOG_DIR);
    first_cluster = false;
  }
  st = LogDeviceStorage::Create("",
                                "",
                                "",
                                std::chrono::milliseconds(1000),
                                4,
                                32 * 1024 * 1024,
                                "none",
                                "",
                                env,
                                info_log,
                                &storage);
#endif  // USE_LOGDEVICE
  if (!st.ok()) {
    return nullptr;
  }
  test_storage->log_router_ =
    std::make_shared<LogDeviceLogRouter>(log_range.first, log_range.second);
  test_storage->storage_.reset(storage);
  return std::move(test_storage);
}

void LocalTestCluster::Initialize(Options opts) {
  env_ = opts.env;
  info_log_ = opts.info_log;
  pilot_ = nullptr;
  copilot_ = nullptr;
  control_tower_ = nullptr;
  cockpit_thread_ = 0;
  control_tower_thread_ = 0;

  if (opts.start_copilot && !opts.start_controltower) {
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

  if (!opts.log_storage) {
    // Range of logs to use.
    std::pair<LogID, LogID> log_range;
    if (opts.single_log) {
      log_range = std::pair<LogID, LogID>(1, 1);
    } else {
      log_range = std::pair<LogID, LogID>(1, 1000);
    }

    if (opts.start_pilot || opts.start_controltower) {
      storage_ = CreateStorage(env_, info_log_, log_range, opts.storage_url);
      if (!storage_) {
        status_ = Status::InternalError("Failed to create storage");
        LOG_ERROR(info_log_, "Failed to create LogDeviceStorage.");
        return;
      }
    }
  } else {
    std::unique_ptr<TestStorageImpl> temp;
    temp->storage_ = std::move(opts.log_storage);
    temp->log_router_ = std::move(opts.log_router);
    storage_ = std::move(temp);
  }

  // Tell rocketspeed to use this storage interface/router.
  opts.pilot.storage = storage_->GetLogStorage();
  opts.pilot.log_router = storage_->GetLogRouter();
  opts.copilot.log_router = storage_->GetLogRouter();
  opts.tower.storage = storage_->GetLogStorage();
  opts.tower.log_router = storage_->GetLogRouter();

  EnvOptions env_options;

  if (opts.start_controltower) {
    control_tower_loop_.reset(new MsgLoop(
        env_, env_options, opts.controltower_port, 4, info_log_, "tower"));
    status_ = control_tower_loop_->Initialize();
    if (!status_.ok()) {
      LOG_ERROR(info_log_, "Failed to initialize Control Tower loop.");
      return;
    }

    // Create ControlTower
    opts.tower.info_log = info_log_;
    opts.tower.msg_loop = control_tower_loop_.get();
    status_ = ControlTower::CreateNewInstance(opts.tower,
                                              &control_tower_);
    if (!status_.ok()) {
      LOG_ERROR(info_log_, "Failed to create ControlTower.");
      return;
    }

    // Start threads.
    auto entry_point = [] (void* arg) {
      LocalTestCluster* cluster = static_cast<LocalTestCluster*>(arg);
      cluster->control_tower_loop_->Run();
    };
    control_tower_thread_ = env_->StartThread(entry_point, (void *)this,
                                              "tower");

    // Wait for message loop to start.
    status_ = control_tower_loop_->WaitUntilRunning();
    if (!status_.ok()) {
      LOG_ERROR(info_log_, "Failed to start ControlTower (%s)",
        status_.ToString().c_str());
      return;
    }
  }

  if (opts.start_copilot || opts.start_pilot) {
    cockpit_loop_.reset(new MsgLoop(
        env_, env_options, opts.cockpit_port, 4, info_log_, "cockpit"));
    status_ = cockpit_loop_->Initialize();
    if (!status_.ok()) {
      LOG_ERROR(info_log_, "Failed to initialize Cockpit loop.");
      return;
    }

    // If we need to start the copilot, then it is better to start the
    // pilot too. Any subscribe/unsubscribe requests to the copilot needs
    // to write to the rollcall topic (via the pilot).
    opts.start_pilot = true;
    if (opts.start_copilot) {
      // Create Copilot
      std::unordered_map<ControlTowerId, HostId> tower_hosts = {
          {0, control_tower_->GetHostId()},
      };
      opts.copilot.control_tower_router =
          std::make_shared<RendezvousHashTowerRouter>(tower_hosts, 1);
      opts.copilot.info_log = info_log_;
      opts.copilot.msg_loop = cockpit_loop_.get();
      opts.copilot.control_tower_connections =
          cockpit_loop_->GetNumWorkers();
      if (opts.copilot.rollcall_enabled) {
        opts.copilot.pilots.push_back(cockpit_loop_->GetHostId());
      }
      status_ = Copilot::CreateNewInstance(opts.copilot, &copilot_);
      if (!status_.ok()) {
        LOG_ERROR(info_log_, "Failed to create Copilot.");
        return;
      }
    }

    if (opts.start_pilot) {
      // Create Pilot
      opts.pilot.info_log = info_log_;
      opts.pilot.msg_loop = cockpit_loop_.get();
      status_ = Pilot::CreateNewInstance(opts.pilot, &pilot_);
      if (!status_.ok()) {
        LOG_ERROR(info_log_, "Failed to create Pilot.");
        return;
      }
    }

    auto entry_point = [] (void* arg) {
      LocalTestCluster* cluster = static_cast<LocalTestCluster*>(arg);
      cluster->cockpit_loop_->Run();
    };
    cockpit_thread_ = env_->StartThread(entry_point, (void *)this, "cockpit");

    // Wait for message loop to start.
    status_ = cockpit_loop_->WaitUntilRunning();
    if (!status_.ok()) {
      LOG_ERROR(info_log_, "Failed to start cockpit (%s)",
        status_.ToString().c_str());
      return;
    }
  }
}

Status
LocalTestCluster::CreateClient(std::unique_ptr<ClientImpl>* client,
                               bool is_internal) {
  std::unique_ptr<ClientImpl> cl;
  ClientOptions client_options;
  client_options.info_log = info_log_;
  client_options.config = GetConfiguration();
  Status status = ClientImpl::Create(std::move(client_options),
                                     client, is_internal);
  return status;
}

Status
LocalTestCluster::CreateClient(std::unique_ptr<Client>* client) {
  ClientOptions client_options;
  client_options.info_log = info_log_;
  client_options.config = GetConfiguration();
  return Client::Create(std::move(client_options), client);
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
  if (cockpit_thread_) {
    env_->WaitForJoin(cockpit_thread_);
  }
  if (control_tower_thread_) {
    env_->WaitForJoin(control_tower_thread_);
  }

  if (control_tower_) {
    control_tower_->Stop();
  }

  if (pilot_) {
    pilot_->Stop();
  }

  if (copilot_) {
    copilot_->Stop();
  }

  // Should now be safe to shutdown LogStorage.
  storage_.reset();

  delete control_tower_;
  delete pilot_;
  delete copilot_;
}

Statistics LocalTestCluster::GetStatisticsSync() const {
  Statistics aggregated;
  // Set of MsgLoops for all components.
  // std::set is used since pilot and copilot often share same MsgLoop, and
  // we only want to gather stats once.
  std::set<MsgLoop*> msg_loops;
  if (pilot_) {
    aggregated.Aggregate(pilot_->GetStatisticsSync());
  }
  if (control_tower_) {
    aggregated.Aggregate(control_tower_->GetStatisticsSync());
  }
  if (copilot_) {
    aggregated.Aggregate(copilot_->GetStatisticsSync());
  }
  for (MsgLoop* msg_loop : msg_loops) {
    aggregated.Aggregate(msg_loop->GetStatisticsSync());
  }
  return aggregated;
}

std::shared_ptr<LogStorage> LocalTestCluster::GetLogStorage() {
  return storage_->GetLogStorage();
}

std::shared_ptr<LogRouter> LocalTestCluster::GetLogRouter() {
  return storage_->GetLogRouter();
}

std::shared_ptr<Configuration> LocalTestCluster::GetConfiguration() const {
  HostId pilot = pilot_ ? pilot_->GetHostId() : HostId();
  HostId copilot = copilot_ ? copilot_->GetHostId() : HostId();
  return std::make_shared<FixedConfiguration>(pilot, copilot);
}

}  // namespace rocketspeed
