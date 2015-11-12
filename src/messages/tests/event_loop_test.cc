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

TEST(EventLoopTest, TriggerableEvent) {
  EventLoop loop(options, std::move(stream_allocator));
  EventLoop::Runner runner(&loop);

  // Create a trigger for events, this call is thread-safe, so that other
  // threads can create triggers.
  EventTrigger trigger = loop.CreateEventTrigger();

  port::Semaphore task_sem;
  const int kExpected = 2;
  // Will only be modified from a thread that used the thread check below.
  // All reads from other threads are properly guarded with a semaphore.
  int done = 0;
  std::vector<std::unique_ptr<EventCallback>> events;
  ThreadCheck thread_check;
  std::unique_ptr<Command> command(MakeExecuteCommand([&]() {
    thread_check.Check();

    // The code to be executed in the callback.
    const auto cb = [&] {
      thread_check.Check();
      assert(done < kExpected);
      ++done;
      if (done == kExpected) {
        loop.Unnotify(trigger);
      }
      task_sem.Post();
    };
    // Create two events that will be invoked.
    events.emplace_back(loop.CreateEventCallback(cb, trigger));
    events.back()->Enable();
    events.emplace_back(loop.CreateEventCallback(cb, trigger));
    events.back()->Enable();
    // One event that is disabled.
    events.emplace_back(
        loop.CreateEventCallback([]() { std::abort(); }, trigger));
    // And one event that will be destroyed before being invoked.
    auto event = loop.CreateEventCallback([]() { std::abort(); }, trigger);
    event->Enable();

    // Notify all registered events.
    loop.Notify(trigger);
    // The callbacks cannot be invoked inline.
    ASSERT_EQ(0, done);
  }));
  ASSERT_OK(loop.SendCommand(command));
  ASSERT_TRUE(task_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(task_sem.TimedWait(positive_timeout));
  ASSERT_EQ(kExpected, done);

  port::Semaphore destroyed;
  std::unique_ptr<Command> command1(MakeExecuteCommand([&]() {
    // EventCallbacks must be destroyed on the EventLoop thread.
    events.clear();
    destroyed.Post();
  }));
  ASSERT_OK(loop.SendCommand(command1));
  ASSERT_TRUE(destroyed.TimedWait(positive_timeout));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
