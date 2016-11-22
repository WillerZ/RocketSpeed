// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include <array>
#include <chrono>
#include <memory>
#include <vector>

#include "external/folly/producer_consumer_queue.h"

#include "include/Rocketeer.h"
#include "include/RocketeerServer.h"
#include "include/RocketSpeed.h"
#include "src/util/common/fixed_configuration.h"
#include "src/util/testharness.h"
#include "src/port/port.h"

namespace rocketspeed {

class DeadLockTest : public ::testing::Test {
 public:
  DeadLockTest() : env_(Env::Default()) {
    EXPECT_OK(test::CreateLogger(env_, "DeadLockTest", &info_log_));
  }

 protected:
  Env* env_;
  std::shared_ptr<Logger> info_log_;
};

class DeadLockRocketeer : public Rocketeer {
 public:
  explicit DeadLockRocketeer(RocketeerServer* server)
  : new_tasks_(2)  // 2 allows for 1 entry for this queue impl
  , server_(server) {
    // Start a thread that will deliver a message on each topic periodically.
    thread_ = Env::Default()->StartThread(
      [this] () {
        // Loop until done is signalled.
        std::unordered_map<InboundID, Task> tasks;
        while (!done_) {
          /* sleep override */
          std::this_thread::sleep_for(std::chrono::milliseconds(1));

          // Check for new tasks
          std::pair<InboundID, Task> p;
          if (new_tasks_.read(p)) {
            if (p.second.seqno == 0) {
              tasks.erase(p.first);
            } else {
              tasks.emplace(p.first, std::move(p.second));
            }
          }

          for (auto& entry : tasks) {
            Task& task = entry.second;
            // Simulate backpressure by spinning try to deliver.
            // This will block new tasks coming in also.
            // Write in bursts of 100 to speed things up.
            for (int i = 0; i < 100; ++i) {
              while (!server_->Deliver(entry.first, task.seqno, "hello")) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
              }
              task.seqno++;
            }
          }
        }
      },
      "deadlock");
  }

  void Stop() {
    done_ = true;
    Env::Default()->WaitForJoin(thread_);
  }

  BackPressure TryHandleNewSubscription(
      InboundID id, SubscriptionParameters params) override {
    // Add subscription to task list.
    std::pair<InboundID, Task> p;
    p.first = id;
    p.second.seqno = params.start_seqno + 1;
    if (new_tasks_.write(p)) {
      return BackPressure::None();
    } else {
      return BackPressure::RetryAfter(std::chrono::milliseconds(10));
    }
  }

  BackPressure TryHandleTermination(
      InboundID id, TerminationSource) override {
    // Remove subscription from task list.
    std::pair<InboundID, Task> p;
    p.first = id;
    p.second.seqno = 0;  // seqno == 0 => remove task
    if (new_tasks_.write(p)) {
      return BackPressure::None();
    } else {
      return BackPressure::RetryAfter(std::chrono::milliseconds(10));
    }
  }

 private:
  struct Task {
    std::string payload;
    SequenceNumber seqno;  // set to 0 to remove element
  };

  folly::ProducerConsumerQueue<std::pair<InboundID, Task>> new_tasks_;

  RocketeerServer* server_;
  std::atomic<bool> done_{false};
  Env::ThreadId thread_;
};

TEST_F(DeadLockTest, DeadLock) {
  // This test checks that a single client cannot block progress on the server
  // simply by sleeping in its message received callback. We expect that the
  // slow client should be forcefully killed if not keeping up.
  //
  // Method: two clients subscribed to a server publishing rapidly. One client
  // sleeps in its callback, the other is used to detect liveness. We wait for
  // deadlock, then wait for the slow client to be killed, allowing progress
  // of the other client to be made.

  // Create Rocketeer
  RocketeerOptions options;
  options.port = 0;
  options.info_log = info_log_;
  options.socket_timeout = std::chrono::seconds(4);

  // Set heartbeats to be more frequent than timeout to check that heartbeats
  // do not accidentally keep a socket alive.
  options.heartbeat_period = std::chrono::seconds(1);

  std::vector<std::unique_ptr<DeadLockRocketeer>> rocketeers;
  RocketeerServer server(options);
  rocketeers.emplace_back(new DeadLockRocketeer(&server));
  server.Register(rocketeers.back().get());
  ASSERT_OK(server.Start());

  // Create two RocketSpeed clients.
  std::array<std::unique_ptr<Client>, 2> client;
  HostId host_id = server.GetHostId();
  for (size_t i = 0; i < 2; ++i) {
    ClientOptions client_options;
    client_options.num_workers = 1;
    client_options.info_log = info_log_;
    client_options.sharding = std::make_unique<FixedShardingStrategy>(host_id);
    ASSERT_OK(Client::Create(std::move(client_options), &client[i]));
  }

  auto& sleeper_client = client[0];
  auto& live_client = client[1];

  // Setup a subscription on the live client. This is our signal that the
  // server is not deadlocked.
  using Clock = std::chrono::steady_clock;
  auto now = [] () {
    using namespace std::chrono;
    return static_cast<uint64_t>(
      duration_cast<milliseconds>(Clock::now().time_since_epoch()).count());
  };
  std::atomic<uint64_t> last_received{now()};
  live_client->Subscribe(GuestTenant, GuestNamespace, "live", 0,
      [&] (std::unique_ptr<MessageReceived>&) {
        last_received = now();
      });

  auto is_deadlocked = [&]() -> bool {
    // We say we are deadlocked if the live client hasn't received anything
    // in a few seconds.
    const uint64_t kTimeoutMs{2000};
    return now() - last_received.load() > kTimeoutMs;
  };

  // Setup a subscription on the sleeper client, which will sleep in the
  // message received callback until the test is over.
  port::Semaphore stop_sleeping;
  std::atomic<bool> still_sleep{true};
  sleeper_client->Subscribe(GuestTenant, GuestNamespace, "sleeper", 0,
      [&] (std::unique_ptr<MessageReceived>&) {
        if (still_sleep.load()) {
          // On receive, block the thread
          ASSERT_TRUE(stop_sleeping.TimedWait(std::chrono::seconds(30)));
          still_sleep.store(false);
        }
      });

  ASSERT_EVENTUALLY_TRUE(is_deadlocked());

  // We're now deadlocked. After some time, the event loop should kill the
  // sleeper client since it is being too slow, and the live client will wake
  // up again.

  // Check that we become live again.
  ASSERT_EVENTUALLY_TRUE(!is_deadlocked());

  stop_sleeping.Post();
  sleeper_client.reset();
  live_client.reset();
  rocketeers.back()->Stop();
  server.Stop();
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
