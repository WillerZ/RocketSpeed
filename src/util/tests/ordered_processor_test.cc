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

namespace rocketspeed {

class OrderedProcessorTest {};

TEST(OrderedProcessorTest, Basic) {
  typedef std::vector<int> Ints;
  Ints processed;
  OrderedProcessor<int> p(3, [&] (int x) { processed.push_back(x); });

  // seqno 0, should be process immediately
  ASSERT_OK(p.Process(100, 0));
  ASSERT_TRUE(processed == (Ints{100}));

  // seqno 2, should not be processed yet.
  ASSERT_OK(p.Process(300, 2));
  ASSERT_TRUE(processed == (Ints{100}));

  // seqno 1, should be processed, then seqno 2 which was already queued.
  ASSERT_OK(p.Process(200, 1));
  ASSERT_TRUE(processed == (Ints{100, 200, 300}));

  // seqno 0, 1 and 2, 6 should fail to process
  ASSERT_TRUE(p.Process(666, 0).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 1).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 2).IsInvalidArgument());  // in past
  ASSERT_TRUE(p.Process(666, 7).IsNoBuffer());  // not enough buffer space
  ASSERT_TRUE(processed == (Ints{100, 200, 300}));

  // seqno 4, 5, 6 should not be processed yet.
  ASSERT_OK(p.Process(500, 4));
  ASSERT_OK(p.Process(700, 6));
  ASSERT_OK(p.Process(600, 5));
  ASSERT_TRUE(processed == (Ints{100, 200, 300}));

  // seqno 3, should process all.
  ASSERT_OK(p.Process(400, 3));
  ASSERT_TRUE(processed == (Ints{100, 200, 300, 400, 500, 600, 700}));

  // reset processor without changing processor functor
  p.Reset();

  // seqno 0, should process immediately
  ASSERT_OK(p.Process(800, 0));
  ASSERT_TRUE(processed == (Ints{100, 200, 300, 400, 500, 600, 700, 800}));
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

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
