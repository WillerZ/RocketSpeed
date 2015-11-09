//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include <string>
#include <unordered_set>
#include <vector>

#include "src/messages/event_loop.h"
#include "src/port/Env.h"
#include "src/port/port.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class EventLoopTest {
 public:
  EventLoopTest()
  : positive_timeout(1000), negative_timeout(100), env(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env, "EventLoopTest", &info_log));
    options.info_log = info_log;
  }

  const std::chrono::milliseconds positive_timeout;
  const std::chrono::milliseconds negative_timeout;
  Env* const env;
  std::shared_ptr<Logger> info_log;
  EventLoop::Options options;
  StreamAllocator stream_allocator;
};

TEST(EventLoopTest, AddTask) {
  EventLoop loop(options, std::move(stream_allocator));
  EventLoop::Runner runner(&loop);

  port::Semaphore task_sem;
  // Will only be modified from a thread that used the thread check below.
  // All reads from other threads are properly guarded with a semaphore.
  bool done = false;
  ThreadCheck thread_check;
  std::unique_ptr<Command> command(MakeExecuteCommand([&]() {
    thread_check.Check();
    loop.AddTask([&]() {
      thread_check.Check();
      done = true;
      task_sem.Post();
    });
    // The task cannot be invoked inline.
    ASSERT_TRUE(!done);
  }));
  ASSERT_OK(loop.SendCommand(command));
  ASSERT_TRUE(task_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(done);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
