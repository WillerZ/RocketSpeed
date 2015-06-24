//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <atomic>

#include "src/util/testharness.h"
#include "src/util/testutil.h"
#include "src/util/common/ordered_processor.h"
#include <algorithm>
#include <array>
#include <set>
#include <vector>

namespace rocketspeed {

class OrderedProcessorTest {};

TEST(OrderedProcessorTest, Basic) {
  typedef std::vector<int> Ints;
  Ints processed;
  OrderedProcessor<int> p(3, [&] (int x) { processed.push_back(x); });

  // seqno 0, should be process immediately
  ASSERT_OK(p.Process(100, 0));
  ASSERT_EQ(processed, (Ints{100}));

  // seqno 2, should not be processed yet.
  ASSERT_OK(p.Process(300, 2));
  ASSERT_EQ(processed, (Ints{100}));

  // seqno 1, should be processed, then seqno 2 which was already queued.
  ASSERT_OK(p.Process(200, 1));
  ASSERT_EQ(processed, (Ints{100, 200, 300}));

  // seqno 0, 1 and 2, 7 should fail to process
  ASSERT_TRUE(p.Process(666, 0).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 1).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 2).IsInvalidArgument());  // in past
  ASSERT_EQ(processed, (Ints{100, 200, 300}));

  // seqno 4, 5, 6 should not be processed yet.
  ASSERT_OK(p.Process(500, 4));
  ASSERT_OK(p.Process(700, 6));
  ASSERT_OK(p.Process(600, 5));
  ASSERT_EQ(processed, (Ints{100, 200, 300}));

  // seqno 3, should process all.
  ASSERT_OK(p.Process(400, 3));
  ASSERT_EQ(processed, (Ints{100, 200, 300, 400, 500, 600, 700}));

  // reset processor without changing processor functor
  p.Reset();

  // seqno 0, should process immediately
  ASSERT_OK(p.Process(800, 0));
  ASSERT_EQ(processed, (Ints{100, 200, 300, 400, 500, 600, 700, 800}));

  // test running out of space
  ASSERT_OK(p.Process(666, 100));
  ASSERT_OK(p.Process(666, 101));
  ASSERT_OK(p.Process(666, 102));
  ASSERT_TRUE(p.Process(666, 103).IsNoBuffer());  // not enough buffer space
}

TEST(OrderedProcessorTest, Randomized) {
  const int n = 1000;
  std::vector<int> processed;
  OrderedProcessor<int> p(n, [&] (int x) { processed.push_back(x); });

  // Generate numbers 0..1000 in random order.
  std::array<int, n> numbers;
  std::iota(numbers.begin(), numbers.end(), 0);
  std::random_device rd;
  std::mt19937 rng(rd());
  std::shuffle(numbers.begin(), numbers.end(), rng);

  for (int x : numbers) {
    ASSERT_OK(p.Process(x, x));
  }
  ASSERT_EQ(processed.size(), n);
  for (int i = 0; i < n; ++i) {
    ASSERT_EQ(processed[i], i);
  }
}

TEST(OrderedProcessorTest, SkipNext) {
  typedef std::vector<int> Ints;
  Ints processed;
  OrderedProcessor<int> p(3, [&] (int x) { processed.push_back(x); });

  // seqno 0, should be process immediately
  ASSERT_OK(p.Process(100, 0));
  ASSERT_EQ(processed, (Ints{100}));

  // seqno 2, should not be processed yet.
  ASSERT_OK(p.Process(300, 2));
  ASSERT_EQ(processed, (Ints{100}));

  // skip seqno 1, then seqno 2 which was already queued.
  p.SkipNext();
  ASSERT_EQ(processed, (Ints{100, 300}));

  // seqno 0, 1 and 2, 7 should fail to process
  ASSERT_TRUE(p.Process(666, 0).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 1).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 2).IsInvalidArgument());  // in past
  ASSERT_EQ(processed, (Ints{100, 300}));

  // seqno 4, 5, 6 should not be processed yet.
  ASSERT_OK(p.Process(500, 4));
  ASSERT_OK(p.Process(700, 6));
  ASSERT_OK(p.Process(600, 5));
  ASSERT_EQ(processed, (Ints{100, 300}));

  // skip 3, should process all.
  p.SkipNext();
  ASSERT_EQ(processed, (Ints{100, 300, 500, 600, 700}));

  // reset processor without changing processor functor
  p.Reset();

  // skip 3 then process 3, should process immediately
  p.SkipNext();
  p.SkipNext();
  p.SkipNext();
  ASSERT_OK(p.Process(800, 3));
  ASSERT_EQ(processed, (Ints{100, 300, 500, 600, 700, 800}));
}

TEST(OrderedProcessorTest, LossyRandomized) {
  const int n = 100000;
  const int buffer = 1000;
  std::vector<int> processed;
  OrderedProcessor<int> p(buffer, [&] (int x) { processed.push_back(x); },
    OrderedProcessorMode::kLossy);

  // Generate numbers 0..n in random order.
  std::vector<int> numbers(n);
  std::iota(numbers.begin(), numbers.end(), 0);
  std::random_device rd;
  std::mt19937 rng(rd());
  std::shuffle(numbers.begin(), numbers.end(), rng);

  // Process first 1000.
  std::vector<int> expected;
  std::priority_queue<int, std::vector<int>, std::greater<int>> queue;
  for (int x : numbers) {
    p.Process(x, x);
    if (queue.size() == buffer) {
      expected.push_back(queue.top());
      queue.pop();
    }
    if (expected.empty() || expected.back() < x) {
      queue.push(x);
    }
  }
  for (; !queue.empty(); queue.pop()) {
    expected.push_back(queue.top());
  }

  ASSERT_EQ(processed, expected);
}

TEST(OrderedProcessorTest, LossyBasic) {
    typedef std::vector<int> Ints;
  Ints processed;
  OrderedProcessor<int> p(3, [&] (int x) { processed.push_back(x); },
    OrderedProcessorMode::kLossy);

  // seqno 0, should be process immediately
  ASSERT_OK(p.Process(100, 0));
  ASSERT_EQ(processed, (Ints{100}));

  // seqno 2, should not be processed yet.
  ASSERT_OK(p.Process(300, 2));
  ASSERT_EQ(processed, (Ints{100}));

  // seqno 1, should be processed, then seqno 2 which was already queued.
  ASSERT_OK(p.Process(200, 1));
  ASSERT_EQ(processed, (Ints{100, 200, 300}));

  // seqno 0, 1 and 2 should fail to process
  ASSERT_TRUE(p.Process(666, 0).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 1).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 2).IsInvalidArgument());  // in past

  ASSERT_OK(p.Process(700, 6));
  ASSERT_OK(p.Process(600, 5));
  ASSERT_OK(p.Process(500, 4));

  // 8 not enough space, but should drop 3 and process 4, 5, 6
  ASSERT_TRUE(p.Process(900, 8).IsNoBuffer());
  ASSERT_EQ(processed, (Ints{100, 200, 300, 500, 600, 700}));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
