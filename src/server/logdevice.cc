// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <gflags/gflags.h>
#include <stdio.h>
#include <memory>
#include <utility>
#include "src/server/storage_setup.h"
#include "src/logdevice/storage.h"
#include "src/logdevice/log_router.h"

// Needed to set logdevice::dbg::currentLevel
#include "external/logdevice/include/debug.h"

DEFINE_string(logs, "1..100000", "range of logs");
DEFINE_string(storage_url,
              "configerator:logdevice/rocketspeed.logdevice.primary.conf",
              "Storage service url");
DEFINE_int32(storage_workers, 16, "number of logdevice storage workers");

namespace rocketspeed {

std::shared_ptr<LogStorage> CreateLogStorage(Env* env) {
#ifdef NDEBUG
  // Disable LogDevice info logging in release.
  facebook::logdevice::dbg::currentLevel =
    facebook::logdevice::dbg::Level::WARNING;
#endif

  rocketspeed::LogDeviceStorage* storage = nullptr;
  rocketspeed::LogDeviceStorage::Create(
    "rocketspeed.logdevice.primary",
    FLAGS_storage_url,
    "",
    std::chrono::milliseconds(1000),
    FLAGS_storage_workers,
    env,
    &storage);
  return std::shared_ptr<rocketspeed::LogStorage>(storage);
}

std::shared_ptr<LogRouter> CreateLogRouter() {
  // Parse and validate log range.
  LogID first_log, last_log;
  int ret = sscanf(FLAGS_logs.c_str(), "%lu..%lu",
    &first_log, &last_log);
  if (ret != 2) {
    fprintf(stderr, "Error: log_range option must be in the form of \"a..b\"");
    return nullptr;
  }

  // Create LogDevice log router.
  return std::make_shared<LogDeviceLogRouter>(first_log, last_log);
}

}  // namespace rocketspeed
