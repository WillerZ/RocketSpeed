// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include <string>

#include <gflags/gflags.h>

#include "src/port/Env.h"
#include "src/server/server.h"
#include "src/server/storage_setup.h"

using namespace rocketspeed;

int main(int argc, char** argv) {
  Env::InstallSignalHandlers();

  GFLAGS::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                                      " [OPTIONS]...");
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  // Run the server.
  RocketSpeed server(Env::Default(), EnvOptions());
  Status st = server.Initialize(&CreateLogStorage, &CreateLogRouter);
  if (st.ok()) {
    server.Run();
    return 0;
  } else {
    LOG_FATAL(server.GetInfoLog(), "Failed to initialize server: %s",
      st.ToString().c_str());
    return 1;
  }
}
