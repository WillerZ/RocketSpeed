//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <limits>
#include <random>
#include "src/util/common/heterogeneous_queue.h"
#include "src/util/testharness.h"
#include "include/Env.h"

namespace rocketspeed {

class HeterogeneousQueueTest : public ::testing::Test {};

TEST_F(HeterogeneousQueueTest, Basic) {
  HeterogeneousQueue q(16);  // 16 bytes

  char c = '\0';
  bool b = false;
  short s = 0;
  int i = 0;

  // Starts empty.
  ASSERT_TRUE(!q.Read(&c));

  // Single Write.
  ASSERT_TRUE(q.Write('x'));
  ASSERT_TRUE(q.Read(&c));
  ASSERT_EQ(c, 'x');
  ASSERT_TRUE(!q.Read(&c));

  // Multi-write.
  HeterogeneousQueue::Transaction tx1(&q);
  tx1.Write('y');
  tx1.Write(true);
  tx1.Write(short(12345));
  tx1.Write(int(123456789));
  ASSERT_TRUE(tx1.Commit());

  ASSERT_TRUE(q.Read(&c));
  ASSERT_TRUE(q.Read(&b));
  ASSERT_TRUE(q.Read(&s));
  ASSERT_TRUE(q.Read(&i));
  ASSERT_EQ(c, 'y');
  ASSERT_EQ(b, true);
  ASSERT_EQ(s, 12345);
  ASSERT_EQ(i, 123456789);

  // Unaligned write.
  HeterogeneousQueue::Transaction tx2(&q);
  tx2.Write('z');
  tx2.Write(int(987654321));
  ASSERT_TRUE(tx2.Commit());

  ASSERT_TRUE(q.Read(&c));
  ASSERT_TRUE(q.Read(&i));
  ASSERT_EQ(c, 'z');
  ASSERT_EQ(i, 987654321);

  // Wrap around.
  HeterogeneousQueue::Transaction tx3(&q);
  tx3.Write(int(123));
  tx3.Write(int(456));
  tx3.Write(int(789));
  ASSERT_TRUE(tx3.Commit());

  ASSERT_TRUE(q.Read(&i));
  ASSERT_EQ(i, 123);
  ASSERT_TRUE(q.Read(&i));
  ASSERT_EQ(i, 456);
  ASSERT_TRUE(q.Read(&i));
  ASSERT_EQ(i, 789);
}

TEST_F(HeterogeneousQueueTest, MultiThreaded) {
  Env* env = Env::Default();


  enum { kNumWrites = 100000 };
  const int seed = 912971275;
  std::mt19937 rng1(seed);
  std::mt19937 rng2(seed);
  std::uniform_int_distribution<int> mode_dist(0, 3);
  std::uniform_int_distribution<int>
    value_dist(0, std::numeric_limits<int>::max());

  HeterogeneousQueue q(64);
  Env::ThreadId tid1 = env->StartThread([&] () {
    // Write thread.
    for (int n = 0; n < kNumWrites; ++n) {
      int mode = mode_dist(rng1);
      int x1 = value_dist(rng1);
      int x2 = value_dist(rng1);
      int x3 = value_dist(rng1);
      bool r = false;
      while (!r) {
        HeterogeneousQueue::Transaction tx(&q);
        switch (mode) {
          case 0:
            tx.Write(char(x1));
            break;
          case 1:
            tx.Write(int(x1));
            break;
          case 2:
            tx.Write(short(x1));
            tx.Write(double(x2));
            break;
          case 3:
            tx.Write(int(x1));
            tx.Write(int(x2));
            tx.Write(int(x3));
            break;
          default:
            ASSERT_TRUE(false);
        }
        r = tx.Commit();
        if (!r) {
          std::this_thread::yield();
        }
      }
    }
  });

  Env::ThreadId tid2 = env->StartThread([&] () {
    // Read thread.
    for (int n = 0; n < kNumWrites; ++n) {
      int mode = mode_dist(rng2);
      int x1 = value_dist(rng2);
      int x2 = value_dist(rng2);
      int x3 = value_dist(rng2);
      char c = '\0';
      int i1 = 0, i2 = 0, i3 = 0;
      short s = 0;
      double d = 0.0;
      switch (mode) {
        case 0:
          while (!q.Read(&c)) {
            std::this_thread::yield();
          }
          ASSERT_EQ(c, char(x1));
          break;
        case 1:
          while(!q.Read(&i1)) {
            std::this_thread::yield();
          }
          ASSERT_EQ(i1, x1);
          break;
        case 2:
          while(!q.Read(&s)) {
            std::this_thread::yield();
          }
          ASSERT_TRUE(q.Read(&d));
          ASSERT_EQ(s, short(x1));
          ASSERT_EQ(d, double(x2));
          break;
        case 3:
          while(!q.Read(&i1)) {
            std::this_thread::yield();
          }
          ASSERT_TRUE(q.Read(&i2));
          ASSERT_TRUE(q.Read(&i3));
          ASSERT_EQ(i1, x1);
          ASSERT_EQ(i2, x2);
          ASSERT_EQ(i3, x3);
          break;
        default: ASSERT_TRUE(false);
      }
    }
  });

  env->WaitForJoin(tid1);
  env->WaitForJoin(tid2);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
