// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include <cstdio>
#include <cstdlib>
#include <string>

#include "include/Status.h"
#include "include/Logger.h"
#include "include/Env.h"
#include "src/test/test_cluster.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/testharness.h"

int main(int argc, char** argv) {
  using namespace rocketspeed;

  // Create logger.
  std::shared_ptr<Logger> info_log;
  Status status = CreateLoggerFromOptions(Env::Default(),
                               test::TmpDir(),
                               "LOG.testcluster",
                               0,
                               0,
                               rocketspeed::INFO_LEVEL,
                               &info_log);
  if (!status.ok()) {
    fprintf(stderr, "Failed creating logger: %s", status.ToString().c_str());
    std::abort();
  }
  LOG_INFO(info_log, "Logger created, spinning up cluster.");

  // Start test cluster.
  LocalTestCluster cluster(info_log);
  LOG_INFO(info_log, "Local cluster started.");

  // Notify that the cluster is READY.
  std::fputc('R', stdout);
  std::fflush(stdout);
  if (ferror(stdout)) {
    LOG_ERROR(info_log, "Failed writing a command.");
    info_log->Flush();
    std::abort();
  }

  int command = 0;
  do {
    LOG_INFO(info_log, "Awaiting next command.");
    command = std::fgetc(stdin);
    if (ferror(stdin)) {
      LOG_ERROR(info_log, "Failed reading a command.");
      info_log->Flush();
      std::abort();
    }
    LOG_INFO(info_log, "Received command: %c", command);
    switch (command) {
      case 'Q':
      case EOF:
        // Exit cleanly.
        command = 'Q';
        break;
      default:
        // Command not recognised
        LOG_ERROR(info_log, "Failed parsing a command: %c", command);
        info_log->Flush();
        std::abort();
    }
  } while (command != 'Q');

  return 0;
}
