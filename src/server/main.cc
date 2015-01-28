// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <gflags/gflags.h>
#include <stdio.h>
#include <memory>
#include <utility>
#include "src/server/server.h"
#include "src/logdevice/storage.h"

// Needed to set logdevice::dbg::currentLevel
#include "external/logdevice/include/debug.h"

// LogDevice settings.
DEFINE_string(logs, "1..100000", "range of logs");
DEFINE_string(storage_url,
              "configerator:logdevice/rocketspeed.logdevice.primary.conf",
              "Storage service url");
DEFINE_int32(storage_workers, 16, "number of logdevice storage workers");


int main(int argc, char** argv) {
  GFLAGS::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                                      " [OPTIONS]...");
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  // Environment.
  rocketspeed::Env* env = rocketspeed::Env::Default();
  rocketspeed::EnvOptions env_options;

#ifdef NDEBUG
  // Disable LogDevice info logging in release.
  facebook::logdevice::dbg::currentLevel =
    facebook::logdevice::dbg::Level::WARNING;
#endif

  // Parse and validate log range.
  std::pair<rocketspeed::LogID, rocketspeed::LogID> log_range;
  int ret = sscanf(FLAGS_logs.c_str(), "%lu..%lu",
    &log_range.first, &log_range.second);
  if (ret != 2) {
    printf("Error: log_range option must be in the form of \"a..b\"");
    return 1;
  }

  // Create LogDevice storage.
  rocketspeed::LogDeviceStorage* logdevice_storage = nullptr;
  rocketspeed::LogDeviceStorage::Create(
    "rocketspeed.logdevice.primary",
    FLAGS_storage_url,
    "",
    std::chrono::milliseconds(1000),
    FLAGS_storage_workers,
    env,
    &logdevice_storage);
  std::shared_ptr<rocketspeed::LogStorage> storage(logdevice_storage);

  // Run the server.
  return rocketspeed::Run(argc, argv, storage, log_range, env, env_options);
}
