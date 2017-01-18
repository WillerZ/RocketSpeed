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

#include "include/Rocketeer.h"
#include "include/RocketeerServer.h"
#include "include/RocketSpeed.h"
#include "src/util/common/fixed_configuration.h"
#include "src/util/testharness.h"
#include "src/port/port.h"
#include "src/messages/event_loop.h"
#include "src/client/backlog_query_store.h"

namespace rocketspeed {

class BacklogQueryTest : public ::testing::Test {
 public:
  BacklogQueryTest()
  : env_(Env::Default())
  , negative_timeout_(100)
  , positive_timeout_(5000) {
    EXPECT_OK(test::CreateLogger(env_, "BacklogQueryTest", &info_log_));
  }

  void Run(std::function<void()> callback, EventLoop* loop) {
    std::unique_ptr<Command> command(MakeExecuteCommand(std::move(callback)));
    ASSERT_OK(loop->SendCommand(command));
  }

 protected:
  Env* env_;
  std::shared_ptr<Logger> info_log_;
  std::chrono::milliseconds negative_timeout_;
  std::chrono::milliseconds positive_timeout_;
};

class BacklogQueryRocketeer : public Rocketeer {
 public:
  explicit BacklogQueryRocketeer() {}

  BackPressure TryHandleNewSubscription(
      InboundID id, SubscriptionParameters params) override {
    return BackPressure::None();
  }

  BackPressure TryHandleUnsubscribe(
      InboundID, NamespaceID, Topic, TerminationSource) override {
    return BackPressure::None();
  }

  void HandleHasMessageSince(
      Flow* flow, InboundID id, NamespaceID namespace_id, Topic topic,
      DataSource source, SequenceNumber seqno) override {
    HasMessageSinceResponse(flow, id, namespace_id, topic, source, seqno,
        HasMessageSinceResult::kNo, "test");
  }

 private:
};

TEST_F(BacklogQueryTest, PendingAwaitingInteraction) {
  EventLoop loop{EventLoop::Options(), StreamAllocator()};
  EventLoop::Runner runner(&loop);

  port::Semaphore sent;
  auto message_handler = [&] (Flow*, std::unique_ptr<Message>) {
    sent.Post();
  };

  std::unique_ptr<BacklogQueryStore> store;

  Run([&]() {
    store.reset(
        new BacklogQueryStore(info_log_, std::move(message_handler), &loop));

    // Add an awaiting sync request.
    store->Insert(BacklogQueryStore::Mode::kAwaitingSync,
                  SubscriptionID::Unsafe(1),
                  "namespace",
                  "topic",
                  "source",
                  123,
                  [](HasMessageSinceResult, std::string) {});
    store->StartSync();
  }, &loop);

  // Shouldn't be sent.
  ASSERT_TRUE(!sent.TimedWait(negative_timeout_));

  Run([&]() {
    // Still shouldn't be sent if we mark it synced if we stop the sync.
    store->StopSync();
    store->MarkSynced(SubscriptionID::Unsafe(1));
  }, &loop);

  ASSERT_TRUE(!sent.TimedWait(negative_timeout_));

  Run([&]() {
    // Should be sent now if we start syncing.
    store->StartSync();
  }, &loop);

  ASSERT_TRUE(sent.TimedWait(positive_timeout_));

  Run([&]() {
    // Add an awaiting sync request.
    store->Insert(BacklogQueryStore::Mode::kPendingSend,
                  SubscriptionID::Unsafe(2),
                  "namespace",
                  "topic",
                  "source",
                  123,
                  [](HasMessageSinceResult, std::string) {});
  }, &loop);

  // Should be sent.
  ASSERT_TRUE(sent.TimedWait(positive_timeout_));

  port::Semaphore done;
  Run([&]() {
    store.reset();
    done.Post();
  }, &loop);
  ASSERT_TRUE(done.TimedWait(positive_timeout_));
}

TEST_F(BacklogQueryTest, ResendSentOnStopSync) {
  // Checks that sent messages are resent after a stop sync.
  EventLoop loop{EventLoop::Options(), StreamAllocator()};
  EventLoop::Runner runner(&loop);

  port::Semaphore sent;
  auto message_handler = [&] (Flow*, std::unique_ptr<Message>) {
    sent.Post();
  };

  std::unique_ptr<BacklogQueryStore> store;

  Run([&]() {
    store.reset(
        new BacklogQueryStore(info_log_, std::move(message_handler), &loop));

    // Add an awaiting sync request.
    store->Insert(BacklogQueryStore::Mode::kAwaitingSync,
                  SubscriptionID::Unsafe(1),
                  "namespace",
                  "topic",
                  "source",
                  123,
                  [](HasMessageSinceResult, std::string) {});
    store->StartSync();
    store->MarkSynced(SubscriptionID::Unsafe(1));
  }, &loop);

  // Should have sent now.
  ASSERT_TRUE(sent.TimedWait(positive_timeout_));

  Run([&]() {
    store->StopSync();
    store->StartSync();
  }, &loop);

  ASSERT_TRUE(!sent.TimedWait(negative_timeout_));

  Run([&]() {
    store->MarkSynced(SubscriptionID::Unsafe(1));
  }, &loop);

  // Should be sent again.
  ASSERT_TRUE(sent.TimedWait(positive_timeout_));

  port::Semaphore done;
  Run([&]() {
    store.reset();
    done.Post();
  }, &loop);
  ASSERT_TRUE(done.TimedWait(positive_timeout_));
}

TEST_F(BacklogQueryTest, ResetPendingOnStopSync) {
  // Checks that pending messages are reset to awaiting on stop sync.
  EventLoop loop{EventLoop::Options(), StreamAllocator()};
  EventLoop::Runner runner(&loop);

  port::Semaphore sent;
  auto message_handler = [&] (Flow*, std::unique_ptr<Message>) {
    sent.Post();
  };

  std::unique_ptr<BacklogQueryStore> store;

  Run([&]() {
    store.reset(
        new BacklogQueryStore(info_log_, std::move(message_handler), &loop));

    // Add an awaiting sync request.
    store->Insert(BacklogQueryStore::Mode::kAwaitingSync,
                  SubscriptionID::Unsafe(1),
                  "namespace",
                  "topic",
                  "source",
                  123,
                  [](HasMessageSinceResult, std::string) {});
    store->MarkSynced(SubscriptionID::Unsafe(1));
  }, &loop);

  // Should not have sent yet.
  ASSERT_TRUE(!sent.TimedWait(negative_timeout_));

  Run([&]() {
    store->StopSync();
    store->StartSync();
  }, &loop);

  ASSERT_TRUE(!sent.TimedWait(negative_timeout_));

  Run([&]() {
    store->MarkSynced(SubscriptionID::Unsafe(1));
  }, &loop);

  // Should be sent again.
  ASSERT_TRUE(sent.TimedWait(positive_timeout_));

  port::Semaphore done;
  Run([&]() {
    store.reset();
    done.Post();
  }, &loop);
  ASSERT_TRUE(done.TimedWait(positive_timeout_));
}

TEST_F(BacklogQueryTest, MultipleRequestsOnSameSub) {
  // Checks that sent messages are resent after a stop sync.
  EventLoop loop{EventLoop::Options(), StreamAllocator()};
  EventLoop::Runner runner(&loop);
  std::unique_ptr<BacklogQueryStore> store;

  port::Semaphore sent;
  auto message_handler = [&] (Flow*, std::unique_ptr<Message> msg) {
    sent.Post();
  };

  port::Semaphore result1;
  port::Semaphore result2;
  Run([&]() {
    store.reset(
        new BacklogQueryStore(info_log_, std::move(message_handler), &loop));

    // Add an awaiting sync request.
    store->Insert(BacklogQueryStore::Mode::kAwaitingSync,
                  SubscriptionID::Unsafe(1),
                  "namespace",
                  "topic",
                  "source",
                  123,
                  [&](HasMessageSinceResult, std::string) { result1.Post(); });
    store->Insert(BacklogQueryStore::Mode::kAwaitingSync,
                  SubscriptionID::Unsafe(1),
                  "namespace",
                  "topic",
                  "source",
                  456,
                  [&](HasMessageSinceResult, std::string) { result2.Post(); });
    store->StartSync();
    store->MarkSynced(SubscriptionID::Unsafe(1));
  }, &loop);

  // Should have sent both now.
  ASSERT_TRUE(sent.TimedWait(positive_timeout_));
  ASSERT_TRUE(sent.TimedWait(positive_timeout_));

  // Now send results.
  Run([&]() {
    MessageBacklogFill fill1(
        GuestTenant,
        "namespace",
        "topic",
        "source",
        123,
        124,
        HasMessageSinceResult::kNo,
        "test");
    store->ProcessBacklogFill(fill1);

    MessageBacklogFill fill2(
        GuestTenant,
        "namespace",
        "topic",
        "source",
        456,
        457,
        HasMessageSinceResult::kNo,
        "test");
    store->ProcessBacklogFill(fill2);
  }, &loop);

  // Should also get results.
  ASSERT_TRUE(result1.TimedWait(positive_timeout_));
  ASSERT_TRUE(result2.TimedWait(positive_timeout_));

  port::Semaphore done;
  Run([&]() {
    store.reset();
    done.Post();
  }, &loop);
  ASSERT_TRUE(done.TimedWait(positive_timeout_));
}

TEST_F(BacklogQueryTest, Integration) {
  // This tests things E2E through the client to a Rocketeer.

  // Create Rocketeer
  RocketeerOptions options;
  options.port = 0;
  options.info_log = info_log_;

  std::vector<std::unique_ptr<BacklogQueryRocketeer>> rocketeers;
  auto server = RocketeerServer::Create(std::move(options));
  rocketeers.emplace_back(new BacklogQueryRocketeer());
  server->Register(rocketeers.back().get());
  ASSERT_OK(server->Start());
  HostId host_id = server->GetHostId();

  // Create client.
  std::unique_ptr<Client> client;
  ClientOptions client_options;
  client_options.info_log = info_log_;
  client_options.sharding = std::make_unique<FixedShardingStrategy>(host_id);
  ASSERT_OK(Client::Create(std::move(client_options), &client));

  // Start subscription.
  auto sub = client->Subscribe(GuestTenant, "namespace", "topic", 1);
  ASSERT_TRUE(sub);

  //std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // HasMessageSince check.
  port::Semaphore done;
  client->HasMessageSince(sub, "namespace", "topic", "source", 1,
      [&] (HasMessageSinceResult result, std::string info) {
        EXPECT_EQ(info, "test");
        done.Post();
      });
  ASSERT_TRUE(done.TimedWait(positive_timeout_));

  server->Stop();
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
