//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <memory>
#include <random>
#include <string>
#include "src/util/testharness.h"
#include "src/util/common/flow_control.h"
#include "src/messages/msg_loop.h"
#include "src/messages/observable_map.h"
#include "src/messages/queues.h"

namespace rocketspeed {

class FlowTest {
 public:
  FlowTest() {
    env_ = Env::Default();
    ASSERT_OK(test::CreateLogger(env_, "FlowTest", &info_log_));
  }

  template <typename T>
  std::shared_ptr<Queue<T>> MakeQueue(size_t size) {
    return std::make_shared<Queue<T>>(
      info_log_,
      std::make_shared<QueueStats>("queue"),
      size);
  }

  std::shared_ptr<Queue<int>> MakeIntQueue(size_t size) {
    return MakeQueue<int>(size);
  }

  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;
};

TEST(FlowTest, PartitionedFlow) {
  // Setup:
  //                    overflow
  //                  P0   |     P1
  //                 +--+  v   +-----+
  //   +------+=10k=>|  |=100=>|sleep|=> counter0++
  //   | 10k  |      +--+      +-----+
  //   | msgs |      +--+      +-----+
  //   +------+=10k=>|  |=10k=>|     |=> counter1++
  //                 +--+      +-----+
  //                  P2         P3
  //
  // This thread fills queues into P0 and P2 with messages.
  // P0 and P2 forward messages to P1 and P3 respectively, with backoff.
  // P1 sleeps on each message (so incoming queue will overflow).
  // Check that all messages are processed.

  enum : int { kNumMessages = 10000 };
  enum : int { kSmallQueue = 100 };
  int sleep_micros = 100;
  MsgLoop loop(env_, env_options_, 0, 4, info_log_, "flow");
  ASSERT_OK(loop.Initialize());
  EventLoop* event_loop[4];
  for (int i = 0; i < 4; ++i) {
    event_loop[i] = loop.GetEventLoop(i);
  }

  // Setup flow control state for each processor.
  FlowControl flow0("", event_loop[0]);
  FlowControl flow1("", event_loop[1]);
  FlowControl flow2("", event_loop[2]);
  FlowControl flow3("", event_loop[3]);

  // Create all our queues.
  auto queue0 = MakeIntQueue(kNumMessages);
  auto queue2 = MakeIntQueue(kNumMessages);
  auto queue01 = MakeIntQueue(kSmallQueue);
  auto queue23 = MakeIntQueue(kNumMessages);

  // Register queue read event handlers.
  flow0.Register<int>(queue0.get(),
    [&] (Flow* flow, int x) {
      flow->Write(queue01.get(), x);
    });

  port::Semaphore sem1;
  flow1.Register<int>(queue01.get(),
    [&] (Flow*, int) {
      env_->SleepForMicroseconds(sleep_micros);
      sem1.Post();
    });

  flow2.Register<int>(queue2.get(),
    [&] (Flow* flow, int x) {
      ASSERT_TRUE(flow->Write(queue23.get(), x));
    });

  port::Semaphore sem3;
  flow3.Register<int>(queue23.get(),
    [&] (Flow*, int) {
      sem3.Post();
    });

  MsgLoopThread flow_threads(env_, &loop, "flow");

  for (int i = 0; i < kNumMessages; ++i) {
    // Queue is big enough for all these writes, all writes should succeed.
    int x = i, y = i;
    ASSERT_TRUE(queue0->Write(x));
    ASSERT_TRUE(queue2->Write(y));
  }

  // Should complete nearly immediately.
  uint64_t start = env_->NowMicros();
  for (int i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem3.TimedWait(std::chrono::milliseconds(100)));
  }

  // Sleeping pipeline should take longer.
  // At least the sum of sleep_micros for all messages.
  int expected = kNumMessages * sleep_micros;
  for (int i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem1.TimedWait(std::chrono::milliseconds(100)));
  }

  // Check that everything took roughly the expected amount of time.
  uint64_t taken = env_->NowMicros() - start;
  ASSERT_GT(taken, expected);
  ASSERT_LT(taken, expected * 2.0);
}

TEST(FlowTest, Fanout) {
  // Setup:
  //                    overflow
  //                  P0   |     P1
  //                 +--+  v   +-----+
  //   +------+=10k=>|  |=100=>|sleep|=> counter0++
  //   | 10k  |      |  |  |   +-----+
  //   | msgs |      |  |  V   +-----+
  //   +------+=10k=>|  |=100=>|sleep|=> counter1++
  //                 +--+      +-----+
  //                             P2
  //
  // This thread fills queues into P0 with messages.
  // P0 forward messages to P1 and P3, with backoff.
  // P1 and P2 sleeps on each message to cause overflow.
  // Check that all messages are processed.

  enum : int { kNumMessages = 10000 };
  enum : int { kSmallQueue = 100 };
  int sleep_micros = 100;
  MsgLoop loop(env_, env_options_, 0, 3, info_log_, "flow");
  ASSERT_OK(loop.Initialize());
  EventLoop* event_loop[3];
  for (int i = 0; i < 3; ++i) {
    event_loop[i] = loop.GetEventLoop(i);
  }

  // Setup flow control state for each processor.
  FlowControl flow0("", event_loop[0]);
  FlowControl flow1("", event_loop[1]);
  FlowControl flow2("", event_loop[2]);

  // Create all our queues.
  auto queue0 = MakeIntQueue(kNumMessages);
  auto queue01 = MakeIntQueue(kSmallQueue);
  auto queue02 = MakeIntQueue(kSmallQueue);

  // Register queue read event handlers.
  flow0.Register<int>(queue0.get(),
    [&] (Flow* flow, int x) {
      // Fanout to P1 and P2
      flow->Write(queue01.get(), x);
      flow->Write(queue02.get(), x);
    });

  port::Semaphore sem1;
  flow1.Register<int>(queue01.get(),
    [&] (Flow*, int) {
      env_->SleepForMicroseconds(sleep_micros);
      sem1.Post();
    });

  port::Semaphore sem2;
  flow2.Register<int>(queue02.get(),
    [&] (Flow* flow, int x) {
      env_->SleepForMicroseconds(sleep_micros);
      sem2.Post();
    });

  MsgLoopThread flow_threads(env_, &loop, "flow");

  for (int i = 0; i < kNumMessages; ++i) {
    // Queue is big enough for all these writes, all writes should succeed.
    int x = i;
    ASSERT_TRUE(queue0->Write(x));
  }

  // Sleeping pipeline should take some time.
  // At least the sum of sleep_micros for all messages.
  uint64_t start = env_->NowMicros();
  int expected = kNumMessages * sleep_micros;
  for (int i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem1.TimedWait(std::chrono::milliseconds(100)));
    ASSERT_TRUE(sem2.TimedWait(std::chrono::milliseconds(100)));
  }

  // Check that everything took roughly the expected amount of time.
  uint64_t taken = env_->NowMicros() - start;
  ASSERT_GT(taken, expected);
  ASSERT_LT(taken, expected * 2.0);
}

TEST(FlowTest, MultiLayerRandomized) {
  // Setup:
  // Many layers of processors, with each processor in each layer forwarding to
  // all processors in next layer. Inter-layer queues will be very small to
  // ensure overflow.

  enum : int { kNumMessages = 100000 };
  enum : int { kSmallQueue = 10 };
  enum : int { kLayers = 10 };
  enum : int { kPerLayer = 5 };
  enum : int { kNumProcessors = kLayers * kPerLayer };
  MsgLoop loop(env_, env_options_, 0, kNumProcessors, info_log_, "flow");
  ASSERT_OK(loop.Initialize());

  // Setup flow control state for each processor.
  std::unique_ptr<FlowControl> flows[kLayers][kPerLayer];
  for (int i = 0; i < kLayers; ++i) {
    for (int j = 0; j < kPerLayer; ++j) {
      flows[i][j].reset(
        new FlowControl("", loop.GetEventLoop(i * kPerLayer + j)));
    }
  }

  // Create all our queues.
  // queue[i][j][k] is to processor j in the ith layer, from processor k in
  // layer (i - 1).
  std::shared_ptr<Queue<int>> queue[kLayers][kPerLayer][kPerLayer];
  for (int i = 1; i < kLayers; ++i) {
    for (int j = 0; j < kPerLayer; ++j) {
      for (int k = 0; k < kPerLayer; ++k) {
        queue[i][j][k] = MakeIntQueue(kSmallQueue);
      }
    }
  }
  // Queues into top layer processors.
  std::shared_ptr<Queue<int>> input[kPerLayer];
  for (int i = 0; i < kPerLayer; ++i) {
    input[i] = MakeIntQueue(kNumMessages);
  }

  // Register queue read event handlers.
  port::Semaphore sem;
  for (int i = 1; i < kLayers; ++i) {
    for (int j = 0; j < kPerLayer; ++j) {
      for (int k = 0; k < kPerLayer; ++k) {
        flows[i][j]->Register<int>(queue[i][j][k].get(),
          [&, i, j] (Flow* flow, int x) {
            if (i == kLayers - 1) {
              sem.Post();
            } else {
              // Route to a processor in next layer based on value.
              int p = x % kPerLayer;
              x /= kPerLayer;
              flow->Write(queue[i + 1][p][j].get(), x);
            }
          });
      }
    }
  }
  for (int i = 0; i < kPerLayer; ++i) {
    flows[0][i]->Register<int>(input[i].get(),
      [&, i] (Flow* flow, int x) {
        // Route to a processor in next layer based on value.
        int p = x % kPerLayer;
        x /= kPerLayer;
        flow->Write(queue[1][p][i].get(), x);
      });
  }

  MsgLoopThread flow_threads(env_, &loop, "flow");

  std::mt19937 rng;
  int routing_max = 1;
  for (int i = 0; i < kLayers; ++i) {
    routing_max *= kPerLayer;
  }
  std::uniform_int_distribution<int> dist(0, routing_max);
  for (int i = 0; i < kNumMessages; ++i) {
    // Queue is big enough for all these writes, all writes should succeed.
    int x = dist(rng);
    ASSERT_TRUE(input[i % kPerLayer]->Write(x));
  }

  // Sleeping pipeline should take some time.
  // At least the sum of sleep_micros for all messages.
  for (int i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem.TimedWait(std::chrono::milliseconds(1000)));
  }
  // No more.
  ASSERT_TRUE(!sem.TimedWait(std::chrono::milliseconds(1000)));
}

TEST(FlowTest, ObservableMap) {
  // Setup:
  //
  //   +----------+    +---------+    +--------+
  //   | 10k msgs |===>| obs map |=1=>| reader |
  //   +----------+    +---------+    +--------+


  enum : int { kNumMessages = 10000 };
  int sleep_micros = 100;
  MsgLoop loop(env_, env_options_, 0, 1, info_log_, "flow");
  ASSERT_OK(loop.Initialize());

  FlowControl flow_control("", loop.GetEventLoop(0));
  auto obs_map = std::make_shared<ObservableMap<std::string, int>>();
  auto queue = MakeQueue<std::pair<std::string, int>>(1);

  port::Semaphore done;
  int reads = 0;
  int last_a = -1;
  int last_b = -1;
  flow_control.Register<std::pair<std::string, int>>(obs_map.get(),
    [&] (Flow* flow, std::pair<std::string, int> kv) {
      flow->Write(queue.get(), kv);
    });

  flow_control.Register<std::pair<std::string, int>>(queue.get(),
    [&] (Flow* flow, std::pair<std::string, int> kv) {
      auto key = kv.first;
      auto value = kv.second;
      int* last = key == "a" ? &last_a : &last_b;
      ASSERT_GT(value, *last);  // always increasing
      *last = value;
      ++reads;
      if (last_a == kNumMessages - 1 && last_b == kNumMessages - 1) {
        done.Post();
      }
      env_->SleepForMicroseconds(sleep_micros);
    });

  MsgLoopThread flow_threads(env_, &loop, "flow");
  for (int i = 0; i < kNumMessages; ++i) {
    std::unique_ptr<Command> cmd(
      MakeExecuteCommand([&, i] () {
        obs_map->Write("a", i);
        obs_map->Write("b", i);
      }));
    loop.SendCommand(std::move(cmd), 0);
  }

  ASSERT_TRUE(done.TimedWait(std::chrono::seconds(5)));
  ASSERT_LT(reads, kNumMessages * 2);  // ensure some were merged
  ASSERT_EQ(last_a, kNumMessages - 1);  // ensure all written
  ASSERT_EQ(last_b, kNumMessages - 1);  // ensure all written
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
