// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Status.h"
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/util/buffered_storage.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"
#include "src/test/test_cluster.h"

namespace rocketspeed {

class BufferedLogStorageTest {
 public:
  BufferedLogStorageTest()
  : env(Env::Default())
  , log_range(1, 1000) {
    ASSERT_OK(test::CreateLogger(env, "BufferedLogStorageTest", &info_log));
    underlying_storage =
      LocalTestCluster::CreateStorage(env, info_log, log_range);
    ASSERT_TRUE(underlying_storage);
  }

 protected:
  Env* const env;
  std::shared_ptr<rocketspeed::Logger> info_log;
  std::pair<LogID, LogID> log_range;
  std::unique_ptr<TestStorage> underlying_storage;
};

TEST(BufferedLogStorageTest, Creation) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());
  MsgLoopThread t1(env, &loop, "loop");

  LogStorage* storage;
  Status st =
    BufferedLogStorage::Create(env,
                               info_log,
                               underlying_storage->GetLogStorage(),
                               &loop,
                               128,
                               4096,
                               std::chrono::microseconds(10000),
                               &storage);
  std::unique_ptr<LogStorage> owned_storage(storage);
  ASSERT_OK(st);
  ASSERT_TRUE(owned_storage);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
