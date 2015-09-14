// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/pacer.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"
#include "src/messages/msg_loop.h"

namespace rocketspeed {

class PacerTest {
 public:
  PacerTest() {
    env_ = Env::Default();
    ASSERT_OK(test::CreateLogger(env_, "PacerTest", &info_log_));
  }

  uint64_t RunTests(uint64_t max_throughput,
                    uint64_t max_inflight,
                    std::chrono::microseconds max_latency,
                    uint64_t num_samples,
                    std::function<void()> job,
                    int job_parallelism) {
    // Start loops
    MsgLoop loop(env_, EnvOptions(), 0, job_parallelism, info_log_, "pacer");
    ASSERT_OK(loop.Initialize());

    Pacer pacer(max_throughput, max_latency, max_inflight, num_samples);

    MsgLoopThread thread(env_, &loop, "pacer");

    // For measuring mean latency.
    std::atomic<uint64_t> total_latency_micros;
    std::atomic<uint64_t> total_requests;

    port::Semaphore recv;
    int worker = 0;
    auto send = [&] () {
      // Send a request when ready.
      pacer.Wait();
      auto now = env_->NowMicros();
      std::unique_ptr<Command> cmd(MakeExecuteCommand(
        [&, now] () {
          job();
          pacer.EndRequest();
          total_latency_micros += env_->NowMicros() - now;
          ++total_requests;
          recv.Post();
        }));
      loop.SendCommand(std::move(cmd), worker++ % job_parallelism);
    };

    // Send requests until the pacer has converged on a window size.
    uint64_t num_sent = 0;
    while (!pacer.HasConverged()) {
      send();
      ++num_sent;
    }
    // Wait for all outstanding requests.
    while (num_sent) {
      recv.Wait();
      --num_sent;
    }

    // Now reset counters and run for real.
    total_latency_micros = 0;
    total_requests = 0;
    for (uint64_t i = 0; i < num_samples; ++i) {
      send();
      ++num_sent;
    }
    // Wait for all outstanding requests.
    while (num_sent) {
      recv.Wait();
      --num_sent;
    }
    return total_latency_micros / total_requests;
  }

  Env* env_;
  std::shared_ptr<Logger> info_log_;
};



TEST(PacerTest, MaxLatencyLimited) {
  // This system can handle > 100qps, but at high latency.
  // Limiting latency to 2ms forces QPS and windows size down, but we should
  // reach a final latency of ~2ms after tuning.
  auto latency = RunTests(100, 100, std::chrono::microseconds(2000), 100,
    [] () {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    },
    10);
  ASSERT_GT(latency, 1700);
  ASSERT_LT(latency, 2300);
}

TEST(PacerTest, MinLatencyLimited) {
  // This system cannot reach < 1ms latency, no matter what the qps.
  // The pacer should optimize for lowest latency, however, which is ~1ms.
  auto latency = RunTests(100, 100, std::chrono::microseconds(500), 100,
    [] () {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    },
    10);
  ASSERT_GT(latency, 900);
  ASSERT_LT(latency, 1200);
}

TEST(PacerTest, ThroughputLimited) {
  // With 10x parallelism and 1ms service time, this system cannot reach more
  // that 10kqps. Asking for higher QPS (20k) will never be achieved, no
  // matter the max latency, so resulting latency will just keep getting higher,
  // as we try to throw more at the system.
  // With Little's law: latency = window / qps = 1k / 10k/s = 100ms
  auto latency = RunTests(20000, 1000, std::chrono::microseconds(500000), 5000,
    [] () {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    },
    10);
  ASSERT_GT(latency, 80000);
  ASSERT_LT(latency, 120000);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
