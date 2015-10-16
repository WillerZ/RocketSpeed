// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#define __STDC_FORMAT_MACROS
#include <cinttypes>
#include <cstdio>
#include <memory>
#include <utility>

#include "src/server/server.h"
#include "src/server/storage_setup.h"
#include "src/logdevice/storage.h"
#include "src/logdevice/log_router.h"

// Needed to set logdevice::dbg::currentLevel
#include "external/logdevice/include/debug.h"

#include <gflags/gflags.h>

DEFINE_string(logs, "1..1000", "range of logs");
DEFINE_string(storage_url, "", "Storage config url");
DEFINE_string(logdevice_cluster, "", "LogDevice cluster tier name");
DEFINE_int32(storage_workers, 16, "number of logdevice storage workers");
DEFINE_int32(storage_timeout, 1000, "storage timeout in milliseconds");
DEFINE_uint64(storage_max_payload_size, 32 * 1024 * 1024,
  "maximum storage payload size (bytes)");
DEFINE_string(logdevice_ssl_boundary, "none",
  "boundary for SSL usage: none, node, rack, row, cluster, dc, or region.");
DEFINE_string(logdevice_my_location, "auto",
  "location of this node for SSL: {region}.{dc}.{cluster}.{row}.{rack} or "
  "\"auto\" to copy from RocketSpeed --node_location flag");

namespace rocketspeed {

std::shared_ptr<LogStorage> CreateLogStorage(Env* env,
                                             std::shared_ptr<Logger> info_log) {
#ifdef NDEBUG
  // Disable LogDevice info logging in release.
  facebook::logdevice::dbg::currentLevel =
    facebook::logdevice::dbg::Level::WARNING;
#endif

  if (FLAGS_logdevice_my_location == "auto") {
    // Use the --node_location flag.
    FLAGS_logdevice_my_location = FLAGS_node_location;
    LOG_VITAL(info_log,
      "Using --node_location (%s) for LogDevice --my-location",
      FLAGS_node_location.c_str());
  }

  rocketspeed::LogDeviceStorage* storage = nullptr;
  rocketspeed::LogDeviceStorage::Create(
    FLAGS_logdevice_cluster,
    FLAGS_storage_url,
    "",
    std::chrono::milliseconds(FLAGS_storage_timeout),
    FLAGS_storage_workers,
    FLAGS_storage_max_payload_size,
    FLAGS_logdevice_ssl_boundary,
    FLAGS_logdevice_my_location,
    env,
    std::move(info_log),
    &storage);
  return std::shared_ptr<rocketspeed::LogStorage>(storage);
}

std::shared_ptr<LogRouter> CreateLogRouter() {
  // Parse and validate log range.
  LogID first_log, last_log;
  int ret = sscanf(FLAGS_logs.c_str(), "%" PRIu64 "..%" PRIu64,
    &first_log, &last_log);
  if (ret != 2) {
    fprintf(stderr, "Error: log_range option must be in the form of \"a..b\"");
    return nullptr;
  }

  // Create LogDevice log router.
  return std::make_shared<LogDeviceLogRouter>(first_log, last_log);
}

}  // namespace rocketspeed
