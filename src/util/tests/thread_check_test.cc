// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include <thread>

#include "src/util/common/thread_check.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class ThreadCheckTest : public ::testing::Test { };

TEST_F(ThreadCheckTest, Test) {
  ThreadCheck c1;
  ThreadCheck c2;
  ASSERT_TRUE(c1.Ok());
  c1.Check();

  std::thread([&] () {
    ASSERT_TRUE(!c1.Ok());  // different thread, should fail
    ASSERT_TRUE(c2.Ok());  // set owner to this thread
    ASSERT_TRUE(c2.Ok());  // check we're still owner

    c1.Reset();  // take ownership of c1
    ASSERT_TRUE(c1.Ok());  // after reset, should succeed
  }).join();

  ASSERT_TRUE(!c1.Ok());  // owned by other thread, should fail
  ASSERT_TRUE(!c2.Ok());  // owned by other thread, should fail
}

TEST_F(ThreadCheckTest, MoveTest) {
  ThreadCheck c1;
  ThreadCheck c2(c1);
  ThreadCheck c3 = c1;
  ASSERT_TRUE(c1.Ok());
  ASSERT_TRUE(c2.Ok());
  ASSERT_TRUE(c3.Ok());

  std::thread([&]() {
    ASSERT_TRUE(!c1.Ok());
    ThreadCheck c4(c1);
    ThreadCheck c5 = c1;
    ASSERT_TRUE(!c4.Ok());
    ASSERT_TRUE(!c5.Ok());

  }).join();
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
