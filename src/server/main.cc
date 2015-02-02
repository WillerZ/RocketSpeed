// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <gflags/gflags.h>
#include <string>
#include "src/server/server.h"
#include "src/server/storage_setup.h"

int main(int argc, char** argv) {
  GFLAGS::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                                      " [OPTIONS]...");
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  // Run the server.
  return rocketspeed::Run(argc,
                          argv,
                          &rocketspeed::CreateLogStorage,
                          &rocketspeed::CreateLogRouter,
                          rocketspeed::Env::Default(),
                          rocketspeed::EnvOptions());
}
