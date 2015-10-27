// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include "src/util/common/unbounded_mpsc_queue.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class UnboundedMPSCQueueTest {
  public:
  UnboundedMPSCQueueTest(){}
};

// Multiple producers write a different sequence of values to the queue.
// Reader ensures that the read values are some interleaving of those values.
TEST(UnboundedMPSCQueueTest, ReadWrite) {
  const int kNumProducers = 10;
  const int kNumMessages = 1000000;

  int read_val;
  int messages_read = 0;
  auto offset = kNumMessages / kNumProducers;

  std::vector<int> expect_val;
  std::vector<std::thread> producers;
  UnboundedMPSCQueue<int> queue;

  for (int t = 0; t < kNumProducers; ++t) {
    expect_val.emplace_back(t * offset);
  }

  auto try_write = [&queue](int write_val, int end_val){
    while (write_val < end_val) {
      queue.write(write_val);
      write_val++;
    }
  };

  for (int t = 0; t < kNumProducers; ++t) {
    producers.emplace_back(try_write, t * offset, (t + 1) * offset);
  }

  while (messages_read < kNumMessages) {
    while (!queue.read(read_val)) {
    }
    auto bucket = read_val / offset;
    ASSERT_EQ(read_val, expect_val[bucket]);
    expect_val[bucket]++;
    messages_read++;
  }

  for (int t = 0; t < kNumProducers; ++t) {
    producers[t].join();
  }
  ASSERT_EQ(messages_read, kNumMessages);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
