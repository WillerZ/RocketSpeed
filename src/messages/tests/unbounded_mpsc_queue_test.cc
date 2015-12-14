// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include "src/messages/unbounded_mpsc_queue.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class UnboundedMPSCQueueTest {
  public:
  UnboundedMPSCQueueTest(){}
  static void TestImpl(int num_writers);
};

void UnboundedMPSCQueueTest::TestImpl(int num_writers) {
#ifdef OS_LINUX
  const int num_messages = 64*1024;
#else
  // don't make it bigger to not overflow Eventfd's pipe
  const int num_messages = 64;
#endif
  ASSERT_EQ(num_messages % num_writers, 0);

  int read_val;
  int messages_read = 0;
  auto offset = num_messages / num_writers;

  std::vector<int> expect_val;
  auto stats = std::make_shared<QueueStats>("test");
  std::vector<std::thread> producers;
  UnboundedMPSCQueue<int> queue(std::make_shared<NullLogger>(), stats);

  for (int t = 0; t < num_writers; ++t) {
    expect_val.emplace_back(t * offset);
  }

  auto try_write = [&queue](int write_val, int end_val){
    while (write_val < end_val) {
      const bool success = queue.Write(write_val);
      ASSERT_EQ(true, success);
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
TEST(UnboundedMPSCQueueTest, ReadWrite_1) {
  TestImpl(1);
}

TEST(UnboundedMPSCQueueTest, ReadWrite_4) {
  TestImpl(4);
}

TEST(UnboundedMPSCQueueTest, ReadWrite_16) {
  TestImpl(16);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
