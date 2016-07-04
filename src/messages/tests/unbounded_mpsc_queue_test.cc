// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include "src/messages/unbounded_mpsc_queue.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class UnboundedMPSCQueueTest : public ::testing::Test {
  public:
  UnboundedMPSCQueueTest(){}
  static void TestImpl(int num_writers, bool use_soft_limit);
};

void UnboundedMPSCQueueTest::TestImpl(int num_writers, bool use_soft_limit) {
  const int num_messages = 64*1024;
  ASSERT_EQ(num_messages % num_writers, 0);

  int read_val;
  int messages_read = 0;
  auto offset = num_messages / num_writers;

  std::vector<int> expect_val;
  auto stats = std::make_shared<QueueStats>("test");
  std::vector<std::thread> producers;
  UnboundedMPSCQueue<int> queue(std::make_shared<NullLogger>(), stats, 10);

  for (int t = 0; t < num_writers; ++t) {
    expect_val.emplace_back(t * offset);
  }

  auto try_write = [&queue, use_soft_limit](int write_val, int end_val){
    while (write_val < end_val) {
      if (use_soft_limit) {
        while (!queue.TryWrite(write_val)) {
          std::this_thread::yield();
        }
      } else {
        queue.Write(write_val);
      }
      write_val++;
    }
  };

  for (int t = 0; t < num_writers; ++t) {
    producers.emplace_back(try_write, t * offset, (t + 1) * offset);
  }

  int reads_failed = 0;
  while (messages_read < num_messages) {
    while (!queue.TryRead(read_val)) {
      ++reads_failed;
    }
    auto bucket = read_val / offset;
    ASSERT_EQ(read_val, expect_val[bucket]);
    expect_val[bucket]++;
    messages_read++;
  }

  for (int t = 0; t < num_writers; ++t) {
    producers[t].join();
  }

  ASSERT_EQ(messages_read, num_messages);
  ASSERT_EQ(messages_read * 2, stats->eventfd_num_writes->Get());
  ASSERT_GE(stats->eventfd_num_reads->Get(), messages_read);
  ASSERT_EQ(reads_failed + messages_read, stats->eventfd_num_reads->Get());
  ASSERT_EQ(false, queue.TryRead(read_val));
}

// Multiple producers write a different sequence of values to the queue.
// Reader ensures that the read values are some interleaving of those values.
TEST_F(UnboundedMPSCQueueTest, ReadWrite_1_NoLimit) {
  TestImpl(1, false);
}

TEST_F(UnboundedMPSCQueueTest, ReadWrite_4_NoLimit) {
  TestImpl(4, false);
}

TEST_F(UnboundedMPSCQueueTest, ReadWrite_16_NoLimit) {
  TestImpl(16, false);
}

TEST_F(UnboundedMPSCQueueTest, ReadWrite_1_Limit) {
  TestImpl(1, true);
}

TEST_F(UnboundedMPSCQueueTest, ReadWrite_4_Limit) {
  TestImpl(4, true);
}

TEST_F(UnboundedMPSCQueueTest, ReadWrite_16_Limit) {
  TestImpl(16, true);
}

TEST_F(UnboundedMPSCQueueTest, LimitTest) {
  // Basic test for soft queue limit.
  // Tests that writes fail after the limit is hit, and being succeeding again
  // after some reads.
  const size_t limit = 10;
  auto stats = std::make_shared<QueueStats>("test");
  UnboundedMPSCQueue<int> queue(std::make_shared<NullLogger>(), stats, limit);
  int x = 0;

  // 10 writes should succeed as we have capacity.
  for (size_t i = 0; i < limit; ++i) {
    ASSERT_TRUE(queue.TryWrite(x));
  }

  // Next should fail as we're at the limit.
  ASSERT_FALSE(queue.TryWrite(x));

  // 10 reads should succeed.
  for (size_t i = 0; i < limit; ++i) {
    ASSERT_TRUE(queue.TryRead(x));
  }

  // Next should fail as we're now empty.
  ASSERT_FALSE(queue.TryRead(x));

  // Write should be succeeding again.
  ASSERT_TRUE(queue.TryWrite(x));
}

TEST_F(UnboundedMPSCQueueTest, WriteUniversalRef) {
  // Test that writing both l-value and r-value refs works.
  const size_t limit = 4;
  auto stats = std::make_shared<QueueStats>("test");
  UnboundedMPSCQueue<int> queue(std::make_shared<NullLogger>(), stats, limit);
  int x = 3;
  int y = 4;
  ASSERT_TRUE(queue.TryWrite(1));
  queue.Write(2);
  ASSERT_TRUE(queue.TryWrite(x));
  queue.Write(y);
}


}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
