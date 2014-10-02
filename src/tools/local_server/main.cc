// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <gflags/gflags.h>
#include <signal.h>
#include "include/Types.h"
#include "src/test/test_cluster.h"

/**
 * This process just starts a pilot, copilot, control tower, and logdevice
 * instance all inside the same process. This is for testing and benchmark
 * purposes.
 */
int main(int argc, char** argv) {
  GFLAGS::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          " [OPTIONS]...");
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  // Ignore SIGPIPE, we'll just handle the EPIPE returned by write.
  signal(SIGPIPE, SIG_IGN);

  // Start local cluster.
  rocketspeed::LocalTestCluster cluster;
  while (1) {
    // Run forever - just keep this thread asleep.
    std::this_thread::sleep_for(std::chrono::seconds(1000));
  }

  return 0;
}
