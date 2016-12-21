//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/util/testharness.h"

#include <string>
#include <stdlib.h>
#include <gflags/gflags.h>

#include "include/Env.h"
#include "src/port/stack_trace.h"
#include "src/util/auto_roll_logger.h"

namespace rocketspeed {
namespace test {

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
  if (s.ok()) {
    return ::testing::AssertionSuccess();
  } else {
    return ::testing::AssertionFailure() << s_expr << std::endl << s.ToString();
  }
}

std::string TmpDir() {
  std::string dir;
  Status s = Env::Default()->GetTestDirectory(&dir);
  [&]() { ASSERT_OK(s); }();
  return dir;
}

Status CreateLogger(Env* env,
                    const std::string& dir,
                    std::shared_ptr<Logger>* logger) {
  return CreateLoggerFromOptions(env,
                                 TmpDir() + "/" + dir,
                                 "LOG",
                                 0,
                                 0,
                                 DEBUG_LEVEL,
                                 logger);
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != nullptr ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

int RunAllTests(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  Env::Default()->InstallSignalHandlers();
  return RUN_ALL_TESTS();
}

}  // namespace test
}  // namespace rocketspeed
