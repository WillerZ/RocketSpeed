//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include <array>
#include <chrono>
#include <memory>
#include <thread>

#include "include/Env.h"
#include "include/RocketSpeed.h"
#include "include/Rocketeer.h"
#include "include/RocketeerServer.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

using DeliveryParams =
    std::tuple<size_t /* rate_limit */,
               std::chrono::milliseconds /* rate_duration */,
               size_t /* batch_max_limit */,
               std::chrono::milliseconds /* batch_max_duration */,
               size_t /* num_messages */,
               size_t /* expected_time_ms */,
               size_t /* expected_messages */>;

class RocketeerDeliveriesTest
    : public ::testing::TestWithParam<DeliveryParams> {
 public:
  RocketeerDeliveriesTest() : positive_timeout(1000), env_(Env::Default()) {
    EXPECT_OK(test::CreateLogger(env_, "RocketeerDeliveriesTest", &info_log_));
    const auto params = GetParam();
    RocketeerOptions options;
    options.info_log = info_log_;
    options.env = env_;
    options.port = 0;
    options.enable_throttling = true;
    options.enable_batching = true;
    options.rate_limit = std::get<0>(params);
    options.rate_duration = std::get<1>(params);
    options.batch_max_limit = std::get<2>(params);
    options.batch_max_duration = std::get<3>(params);
    server_ = RocketeerServer::Create(std::move(options));
  }

  virtual ~RocketeerDeliveriesTest() {
    server_.reset();
    env_->WaitForJoin();
  }

 protected:
  typedef std::chrono::steady_clock TestClock;

  const std::chrono::milliseconds positive_timeout;
  Env* const env_;
  std::shared_ptr<rocketspeed::Logger> info_log_;
  std::unique_ptr<RocketeerServer> server_;

  class ClientMock {
   public:
    std::unique_ptr<MsgLoop> msg_loop;

    // Noncopyable, movable
    ClientMock(ClientMock&&) = default;
    ClientMock& operator=(ClientMock&&) = default;

    ClientMock(std::unique_ptr<MsgLoop> _msg_loop, std::thread msg_loop_thread)
    : msg_loop(std::move(_msg_loop))
    , msg_loop_thread_(std::move(msg_loop_thread)) {}

    ~ClientMock() {
      msg_loop->Stop();
      if (msg_loop_thread_.joinable()) {
        msg_loop_thread_.join();
      }
    }

   private:
    std::thread msg_loop_thread_;
  };

  ClientMock MockClient(
      const std::map<MessageType, MsgCallbackType>& callbacks) {
    std::unique_ptr<MsgLoop> client(
        new MsgLoop(env_, EnvOptions(), 0, 1, info_log_, "client"));
    client->RegisterCallbacks(callbacks);
    EXPECT_OK(client->Initialize());
    std::thread thread([&]() { client->Run(); });
    EXPECT_OK(client->WaitUntilRunning());
    return ClientMock(std::move(client), std::move(thread));
  }
};

struct DeliveryRocketeer : public Rocketeer {
  size_t deliveries_;
  const std::string deliver_msg_ = "RandomMessage";

  explicit DeliveryRocketeer(size_t deliveries) : deliveries_(deliveries) {}

  void HandleNewSubscription(Flow* flow,
                             InboundID inbound_id,
                             SubscriptionParameters params) override {
    for (size_t i = 0; i < deliveries_; i++) {
      SequenceNumber num = i + 1;
      Deliver(flow, inbound_id, params.namespace_id, params.topic_name,
          {"", num}, std::string(deliver_msg_));
    }
    AckSubscribe(flow, inbound_id, params);
  }

  void HandleUnsubscribe(Flow*,
                         InboundID inbound_id,
                         NamespaceID namespace_id,
                         Topic topic,
                         TerminationSource source) override {}
};

TEST_P(RocketeerDeliveriesTest, DeliveryThrottlingTest) {
  const auto params = GetParam();
  // const size_t rate_limit = std::get<0>(params);
  //const std::chrono::milliseconds rate_duration = std::get<1>(params);
  const size_t batch_max_limit = std::get<2>(params);
  // const std::chrono::milliseconds batch_max_duration = std::get<3>(params);
  const size_t kNumMessages = std::get<4>(params);
  const size_t expected_time_ms = std::get<5>(params);
  const size_t expected_messages = std::get<6>(params);
  const std::chrono::milliseconds timeout(expected_time_ms +
                                          positive_timeout.count());

  DeliveryRocketeer rocketeer(kNumMessages);
  server_->Register(&rocketeer);
  ASSERT_OK(server_->Start());

  port::Semaphore deliver_sem;

  size_t seq_number = 1;  // To check order of delivery
  size_t batched_messages_received = 0;
  size_t single_messages_received = 0;

  auto checkMessage = [&](const MessageDeliverData& message) {
    SequenceNumber num = seq_number++;
    ASSERT_TRUE(rocketeer.deliver_msg_ == message.GetPayload().ToString());
    ASSERT_TRUE(num == message.GetSequenceNumber());
    if (num == kNumMessages) {
      deliver_sem.Post();
    }
  };

  auto client = MockClient({
      {MessageType::mDeliverData,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto data = static_cast<MessageDeliverData*>(msg.get());
         single_messages_received++;
         checkMessage(*data);
       }},
      {MessageType::mDeliverBatch,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto data = static_cast<MessageDeliverBatch*>(msg.get());
         const auto& messages = data->GetMessages();
         batched_messages_received++;
         ASSERT_GE(batch_max_limit, messages.size());
         for (size_t i = 0; i < messages.size(); ++i) {
           checkMessage(*messages[i]);
         }
       }},
  });

  auto socket = client.msg_loop->CreateOutboundStream(server_->GetHostId(), 0);

  // Subscribe.
  MessageSubscribe subscribe(GuestTenant,
                             GuestNamespace,
                             "DeliveryRocketeer",
                             0,
                             SubscriptionID::Unsafe(2));
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));

  // Wait for the messages
  // All the messages should be delivered within the timeout period
  uint64_t start = env_->NowMicros();
  ASSERT_TRUE(deliver_sem.TimedWait(timeout));
  uint64_t taken = env_->NowMicros() - start;

  if (expected_time_ms) {
    ASSERT_GE(taken / 1000, expected_time_ms);
    ASSERT_LE(taken / 1000, expected_time_ms + positive_timeout.count());
  }

  if (expected_messages) {
    ASSERT_EQ(batched_messages_received + single_messages_received,
              expected_messages);
  }

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

auto params_0 = DeliveryParams{10000,
                               std::chrono::milliseconds(100),
                               1,
                               std::chrono::milliseconds(10),
                               10000,
                               0,
                               10000};

auto params_1 = DeliveryParams{10000,
                               std::chrono::milliseconds(100),
                               1,
                               std::chrono::milliseconds(10),
                               10000 * 10,
                               1000,
                               100000};

auto params_2 = DeliveryParams{10000,
                               std::chrono::milliseconds(100),
                               2,
                               std::chrono::milliseconds(10),
                               10000 * 10,
                               450,
                               50000};

auto params_3 = DeliveryParams{10000,
                               std::chrono::milliseconds(100),
                               10,
                               std::chrono::milliseconds(100),
                               10000 * 10,
                               0,
                               10000};

auto params_4 = DeliveryParams{1,
                               std::chrono::milliseconds(100),
                               1,
                               std::chrono::milliseconds(10),
                               10,
                               900,
                               10};

auto params_5 = DeliveryParams{10000,
                               std::chrono::milliseconds(100),
                               10000,
                               std::chrono::milliseconds(100),
                               10000,
                               0,
                               1};

auto params_6 = DeliveryParams{10000,
                               std::chrono::milliseconds(1),
                               10000,
                               std::chrono::milliseconds(2),
                               10000 * 5,
                               0,
                               0 /* time is small, can't say num of batches */};

auto params_7 = DeliveryParams{1,
                               std::chrono::milliseconds(100),
                               10,
                               std::chrono::milliseconds(10),
                               10,
                               0,
                               1};

INSTANTIATE_TEST_CASE_P(RocketeerDeliveriesTest,
                        RocketeerDeliveriesTest,
                        ::testing::Values(params_0,
                                          params_1,
                                          params_2,
                                          params_3,
                                          params_4,
                                          params_5,
                                          params_6,
                                          params_7));

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
