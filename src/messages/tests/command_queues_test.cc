//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <chrono>
#include <memory>
#include <random>
#include <thread>

#include "include/Env.h"
#include "src/messages/event_loop.h"
#include "src/messages/queues.h"
#include "src/util/common/processor.h"
#include "src/util/random.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class CommandQueueTest : public ::testing::Test {
 public:
  CommandQueueTest() : timeout_(std::chrono::seconds(5)) {}

 protected:
  const std::chrono::seconds timeout_;
  StreamAllocator stream_allocator_;
};

TEST_F(CommandQueueTest, Liveness) {
  EventLoop loop(EventLoop::Options(), std::move(stream_allocator_));
  EventLoop::Runner runner(&loop);
  ASSERT_OK(runner.GetStatus());

  const auto no_op = []() {};

  std::mt19937 reader_rng(912971275);
  std::mt19937 writer_rng(127997152);
  std::uniform_int_distribution<size_t> reader_dist(0, 80), writer_dist(0, 100);
  const auto random_wait = [&]() {
    /* sleep override */
    std::this_thread::sleep_for(
        std::chrono::nanoseconds(reader_dist(reader_rng)));
  };

  port::Semaphore sem1, sem2;
  const auto wait_sem1 = [&]() { ASSERT_TRUE(sem1.TimedWait(timeout_)); };
  const auto notify_sem2 = [&]() { sem2.Post(); };

  auto send_command = [&](std::function<void()> cb) {
    std::unique_ptr<Command> command(MakeExecuteCommand(std::move(cb)));
    ASSERT_OK(loop.SendCommand(command));
  };

  // Write exactly kMaxBatchSize + 1 commands before the first one gets
  // processed, they will be read in one batch. Wait for all of them, to verify
  // that we didn't lost the wakup due to batch size limit.
  send_command(wait_sem1);
  for (size_t i = 1; i < kMaxQueueBatchReadSize; ++i) {
    send_command(no_op);
  }
  send_command(notify_sem2);
  sem1.Post();
  ASSERT_TRUE(sem2.TimedWait(timeout_));

  // Random waits after reading and writing, the average wait time after reading
  // is shorter than the average time for writing, which should keep the queue
  // nearly empty and force many notifications being issued by the writer
  // (checking the edge case).
  for (size_t i = 0; i < 100000; ++i) {
    send_command(random_wait);
    /* sleep override */
    std::this_thread::sleep_for(
        std::chrono::nanoseconds(writer_dist(writer_rng)));
  }
  send_command(notify_sem2);
  ASSERT_TRUE(sem2.TimedWait(timeout_));
}

TEST_F(CommandQueueTest, TwoItemsTwoBatches) {
  EventLoop loop(EventLoop::Options(), std::move(stream_allocator_));
  ASSERT_OK(loop.Initialize());

  // Simple unattached queue.
  SPSCQueue<int> queue(std::make_shared<NullLogger>(),
                       std::make_shared<QueueStats>("test"),
                       100);

  // Create callback that reads one from queue then posts to a semaphore
  port::Semaphore sem;
  InstallSource<int>(&loop, &queue, [&](Flow*, int) {
    sem.Post();
  });

  // Before enabling, write 2 commands.
  // We don't care what they are, null will do.
  int x = 0, y = 1;
  queue.Write(x);
  queue.Write(y);

  // Now enable and check that we read both.
  std::thread loop_thread([&]() { loop.Run(); });
  ASSERT_TRUE(sem.TimedWait(timeout_));
  ASSERT_TRUE(sem.TimedWait(timeout_));

  loop.Stop();
  loop_thread.join();
}


TEST_F(CommandQueueTest, WriteHistogram) {
  EventLoop loop(EventLoop::Options(), std::move(stream_allocator_));

  // Simple unattached queue.
  SPSCQueue<int> queue(std::make_shared<NullLogger>(),
                       std::make_shared<QueueStats>("test"),
                       100);

  EventLoop::Runner runner(&loop);
  ASSERT_OK(runner.GetStatus());

  const int kNumMessages = 5;
  for(int i = 0; i < kNumMessages; i++){
    queue.Write(i);
  }

  port::Semaphore sem;
  InstallSource<int>(&loop, &queue, [&](Flow*, int x) {
    if (x == kNumMessages - 1) {
      // This is the last message, so check stats now.
      auto histogram = queue.GetStats().size_on_read;
      ASSERT_EQ(histogram->GetNumSamples(), kNumMessages);
      ASSERT_LT(histogram->Percentile(0.1), 1);
      ASSERT_GT(histogram->Percentile(0.9), kNumMessages - 2);
      sem.Post();
    }
  });
  sem.Wait();
}
}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
