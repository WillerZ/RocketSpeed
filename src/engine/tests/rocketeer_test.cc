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

class RocketeerTest : public ::testing::Test {
 public:
  RocketeerTest(bool terminate_on_disconnect = true)
  : positive_timeout(1000), negative_timeout(100), env_(Env::Default()) {
    EXPECT_OK(test::CreateLogger(env_, "RocketeerTest", &info_log_));
    RocketeerOptions options;
    options.info_log = info_log_;
    options.env = env_;
    options.port = 0;
    options.enable_throttling = false;
    options.enable_batching = false;
    options.terminate_on_disconnect = terminate_on_disconnect;
    server_.reset(new RocketeerServer(std::move(options)));
  }

  virtual ~RocketeerTest() {
    server_.reset();
    env_->WaitForJoin();
  }

 protected:
  typedef std::chrono::steady_clock TestClock;

  const std::chrono::milliseconds positive_timeout;
  const std::chrono::milliseconds negative_timeout;
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

class RocketeerDisconnectTest : public RocketeerTest {
 public:
  RocketeerDisconnectTest() : RocketeerTest(false) {}
};


struct SubscribeUnsubscribe : public Rocketeer {
  bool is_set_ = false;
  InboundID inbound_id_;
  port::Semaphore terminate_sem_;

  void HandleNewSubscription(
      Flow*, InboundID inbound_id, SubscriptionParameters params) override {
    ASSERT_TRUE(!is_set_);
    is_set_ = true;
    inbound_id_ = inbound_id;
  }

  void HandleTermination(
      Flow*, InboundID inbound_id, TerminationSource source) override {
    ASSERT_TRUE(TerminationSource::Subscriber == source);
    ASSERT_TRUE(is_set_);
    ASSERT_TRUE(inbound_id_ == inbound_id);
    terminate_sem_.Post();
  }
};

TEST_F(RocketeerTest, SubscribeUnsubscribe) {
  SubscribeUnsubscribe rocketeer;
  server_->Register(&rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetHostId();

  auto client = MockClient(std::map<MessageType, MsgCallbackType>());
  auto socket = client.msg_loop->CreateOutboundStream(server_addr, 0);

  auto subid1 = SubscriptionID::Unsafe(1);
  auto subid2 = SubscriptionID::Unsafe(2);
  // Subscribe.
  MessageSubscribe subscribe(
      GuestTenant, GuestNamespace, "SubscribeUnsubscribe", 101, subid2);
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));
  // Send again, to verify that only one will be delivered.
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));

  // Send some broken unsubscribe, that doesn't match anything.
  MessageUnsubscribe unsubscribe1(
      GuestTenant, GuestNamespace, "NotSubscribeUnsubscribe", subid1,
      MessageUnsubscribe::Reason::kRequested);
  ASSERT_OK(client.msg_loop->SendRequest(unsubscribe1, &socket, 0));

  // Send valid unsubscribe.
  MessageUnsubscribe unsubscribe(
      GuestTenant, GuestNamespace, "SubscribeUnsubscribe", subid2,
      MessageUnsubscribe::Reason::kRequested);
  ASSERT_OK(client.msg_loop->SendRequest(unsubscribe, &socket, 0));

  ASSERT_TRUE(rocketeer.terminate_sem_.TimedWait(positive_timeout));

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

struct SubscribeTerminate : public Rocketeer {
  InboundID inbound_id_;
  port::Semaphore terminate_sem_;

  void HandleNewSubscription(
      Flow*, InboundID inbound_id, SubscriptionParameters params) override {
    inbound_id_ = inbound_id;
    Unsubscribe(nullptr, inbound_id, std::move(params.namespace_id),
        std::move(params.topic_name), Rocketeer::UnsubscribeReason::Invalid);
  }

  void HandleTermination(
      Flow*, InboundID inbound_id, TerminationSource source) override {
    ASSERT_TRUE(TerminationSource::Rocketeer == source);
    ASSERT_TRUE(inbound_id_ == inbound_id);
    terminate_sem_.Post();
  }
};

TEST_F(RocketeerTest, SubscribeTerminate) {
  SubscribeTerminate rocketeer;
  server_->Register(&rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetHostId();

  port::Semaphore unsubscribe_sem;
  auto client = MockClient({
      {MessageType::mUnsubscribe,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto unsubscribe = static_cast<MessageUnsubscribe*>(msg.get());
         ASSERT_TRUE(unsubscribe->GetReason() ==
                     MessageUnsubscribe::Reason::kInvalid);
         unsubscribe_sem.Post();
       }},
  });
  auto socket = client.msg_loop->CreateOutboundStream(server_addr, 0);

  // Subscribe.
  MessageSubscribe subscribe(GuestTenant,
                             GuestNamespace,
                             "SubscribeTerminate",
                             101,
                             SubscriptionID::Unsafe(2));
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));

  // Wait for unsubscribe message.
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
  // Rocketeer should also be called.
  ASSERT_TRUE(rocketeer.terminate_sem_.TimedWait(positive_timeout));

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

struct Noop : public Rocketeer {
  Rocketeer* rocketeer_;
  explicit Noop(Rocketeer* rocketeer) {
    rocketeer->SetBelowRocketeer(this);
    rocketeer_ = rocketeer;
  }

  void HandleNewSubscription(Flow* flow, InboundID inbound_id,
      SubscriptionParameters params) override {
    return rocketeer_->HandleNewSubscription(flow, inbound_id, params);
  }

  void HandleTermination(
      Flow* flow, InboundID inbound_id, TerminationSource source) override {
    rocketeer_->HandleTermination(flow, inbound_id, source);
  }
};

struct TopOfStack : public Rocketeer {
  const std::string deliver_msg_ = "RandomMessage";
  const SequenceNumber deliver_msg_seqno_ = 102;
  const SequenceNumber advance_seqno_ = 112;
  const SequenceNumber dataloss_seqno_ = 120;
  const std::vector<RocketeerMessage> messages_ {
    RocketeerMessage(2, 113, "Message 1"),
    RocketeerMessage(2, 114, "Message 2"),
    RocketeerMessage(2, 115, "Message 3"),
  };
  port::Semaphore terminate_sem_;
  InboundID inbound_id_;

  void HandleNewSubscription(
      Flow*, InboundID inbound_id, SubscriptionParameters params) override {
    inbound_id_ = inbound_id;
    Deliver(nullptr, inbound_id, params.namespace_id, params.topic_name,
        deliver_msg_seqno_, deliver_msg_);
    Advance(nullptr, inbound_id, advance_seqno_);
    DeliverBatch(nullptr, inbound_id.stream_id, messages_);
    NotifyDataLoss(nullptr, inbound_id, dataloss_seqno_);
    Unsubscribe(nullptr, inbound_id, std::move(params.namespace_id),
        std::move(params.topic_name), Rocketeer::UnsubscribeReason::Invalid);
  }

  void HandleTermination(
      Flow*, InboundID inbound_id, TerminationSource source) override {
    ASSERT_TRUE(TerminationSource::Rocketeer == source);
    ASSERT_TRUE(inbound_id_ == inbound_id);
    terminate_sem_.Post();
  }
};

TEST_F(RocketeerTest, StackRocketeerTest) {
  TopOfStack topRocketeer;
  Noop* rocketeer = new Noop(new Noop(new Noop(&topRocketeer)));
  server_->Register(rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetHostId();

  port::Semaphore unsubscribe_sem;
  port::Semaphore deliver_sem;
  port::Semaphore advance_sem;
  port::Semaphore batch_sem;
  port::Semaphore dataloss_sem;

  // Ensure that HandleSubscription and HandleTermination calls go up the stack
  // and Deliver/Advance/Terminate go down the stack.

  auto client = MockClient({
      {MessageType::mUnsubscribe,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         unsubscribe_sem.Post();
       }},
      {MessageType::mDeliverData,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto data = static_cast<MessageDeliverData*>(msg.get());
         ASSERT_TRUE(topRocketeer.deliver_msg_ ==
                     data->GetPayload().ToString());
         ASSERT_TRUE(topRocketeer.deliver_msg_seqno_ ==
                     data->GetSequenceNumber());
         deliver_sem.Post();
       }},
      {MessageType::mDeliverGap,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto data = static_cast<MessageDeliverGap*>(msg.get());
         if (data->GetGapType() == GapType::kBenign) {
           ASSERT_TRUE(data->GetFirstSequenceNumber() ==
                       topRocketeer.deliver_msg_seqno_);
           ASSERT_TRUE(data->GetLastSequenceNumber() ==
                       topRocketeer.advance_seqno_);
           advance_sem.Post();
         } else if (data->GetGapType() == GapType::kDataLoss) {
           ASSERT_TRUE(data->GetFirstSequenceNumber() ==
                       topRocketeer.messages_.back().seqno);
           ASSERT_TRUE(data->GetLastSequenceNumber() ==
                       topRocketeer.dataloss_seqno_);
           dataloss_sem.Post();
         } else {
           FAIL() << "Should not be called";
         }
       }},
      {MessageType::mDeliverBatch,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto data = static_cast<MessageDeliverBatch*>(msg.get());
         const auto& messages = data->GetMessages();
         ASSERT_EQ(messages.size(), topRocketeer.messages_.size());
         for (size_t i = 0; i < messages.size(); ++i) {
           ASSERT_TRUE(messages[i]->GetPayload() ==
                       topRocketeer.messages_[i].payload);
           ASSERT_TRUE(messages[i]->GetSequenceNumber() ==
                       topRocketeer.messages_[i].seqno);
         }
         batch_sem.Post();
       }},

  });
  auto socket = client.msg_loop->CreateOutboundStream(server_addr, 0);

  // Subscribe.
  MessageSubscribe subscribe(
      GuestTenant, GuestNamespace, "stackable", 101, SubscriptionID::Unsafe(2));
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));

  ASSERT_TRUE(deliver_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(advance_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(batch_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(dataloss_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(topRocketeer.terminate_sem_.TimedWait(positive_timeout));

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

struct MetadataFlowControl : public Rocketeer {
  // Backoff to use for new subscriptions.
  static const std::chrono::milliseconds kDelay;
  enum : uint32_t { kNumShards = 1 };

  // Semaphore to signal when we are done.
  port::Semaphore terminate_sem_;

  // Number of topics (used to know when we're done).
  const int num_topics_;

  // Next expected topic per shard. Incremented when we do not backoff.
  std::array<int, kNumShards> next_topic_;

  // Should we delay next topic on this shard? This alternates true/false.
  std::array<bool, kNumShards> delay_next_;

  std::chrono::steady_clock::time_point start_;
  std::array<std::chrono::steady_clock::time_point, kNumShards> next_time_;

  explicit MetadataFlowControl(int num_topics)
  : num_topics_(num_topics)
  , start_(std::chrono::steady_clock::now()) {
    std::fill(next_topic_.begin(), next_topic_.end(), 0);
    std::fill(delay_next_.begin(), delay_next_.end(), true);
    std::fill(next_time_.begin(), next_time_.end(), start_);
  }

  BackPressure TryHandleNewSubscription(
      InboundID inbound_id, SubscriptionParameters params) override {
    auto shard = inbound_id.GetShard();
    auto now = std::chrono::steady_clock::now();
    EXPECT_LT(shard, kNumShards);
    EXPECT_GE(now - start_, next_time_[shard] - start_);
    EXPECT_EQ(params.topic_name, std::to_string(next_topic_[shard]));
    auto delay = std::chrono::milliseconds::zero();
    if (delay_next_[shard]) {
      delay = kDelay;
    } else {
      ++next_topic_[shard];
      if (next_topic_[shard] == num_topics_) {
        terminate_sem_.Post();
      }
    }
    delay_next_[shard] = !delay_next_[shard];
    next_time_[shard] = now + delay;
    if (delay.count()) {
      return BackPressure::RetryAfter(delay);
    } else {
      return BackPressure::None();
    }
  }

  BackPressure TryHandleTermination(
      InboundID inbound_id, TerminationSource source) override {
    return BackPressure::None();
  }
};

const std::chrono::milliseconds MetadataFlowControl::kDelay =
    std::chrono::milliseconds(100);

TEST_F(RocketeerTest, MetadataFlowControl) {
  const auto kNumTopics = 10;
  const auto kNumShards = MetadataFlowControl::kNumShards;
  MetadataFlowControl rocketeer(kNumTopics);
  server_->Register(&rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetHostId();

  auto client = MockClient(std::map<MessageType, MsgCallbackType>());

  // Open sockets for each shard.
  // Test relies on the fact that we have a different from per shard.
  StreamSocket socket[kNumShards];
  for (uint32_t shard = 0; shard < kNumShards; ++shard) {
    socket[shard] = client.msg_loop->CreateOutboundStream(server_addr, 0);
  }

  for (int t = 0; t < kNumTopics; ++t) {
    for (uint32_t shard = 0; shard < kNumShards; ++shard) {
      // Subscribe.
      auto subid = SubscriptionID::ForShard(shard, t + 1);
      auto topic = std::to_string(t);
      MessageSubscribe subscribe(GuestTenant, GuestNamespace, topic, 1, subid);
      ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket[shard], 0));
    }
  }

  // Wait for first shard to finish.
  ASSERT_TRUE(rocketeer.terminate_sem_.TimedWait(
      negative_timeout + MetadataFlowControl::kDelay * kNumTopics));

  // Other shards should finish shortly after (unaffected by first)
  for (uint32_t shard = 1; shard < kNumShards; ++shard) {
    ASSERT_TRUE(rocketeer.terminate_sem_.TimedWait(negative_timeout));
  }

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

struct DisconnectHandler : public Rocketeer {
  StreamID stream_id_{0};
  port::Semaphore subscribe_sem_;
  port::Semaphore disconnect_sem_;

  void HandleNewSubscription(
      Flow*, InboundID inbound_id, SubscriptionParameters) override {
    stream_id_ = inbound_id.stream_id;
    subscribe_sem_.Post();
  }

  void HandleTermination(
      Flow*, InboundID inbound_id, TerminationSource source) override {
    // Should never receive a termination.
    ASSERT_TRUE(false);
  }

  void HandleDisconnect(Flow*, StreamID stream_id) override {
    ASSERT_EQ(stream_id, stream_id_);
    ASSERT_NE(stream_id_, 0);
    disconnect_sem_.Post();
  }
};

TEST_F(RocketeerDisconnectTest, DisconnectHandler) {
  DisconnectHandler rocketeer;
  server_->Register(&rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetHostId();

  {
    auto client = MockClient(std::map<MessageType, MsgCallbackType>());
    auto socket = client.msg_loop->CreateOutboundStream(server_addr, 0);
    auto subid1 = SubscriptionID::Unsafe(1);

    // Subscribe.
    MessageSubscribe subscribe(
        GuestTenant, GuestNamespace, "DisconnectHandler", 101, subid1);
    ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));
    ASSERT_TRUE(rocketeer.subscribe_sem_.TimedWait(positive_timeout));
  }
  // Client stopped now, expect disconnect.
  ASSERT_TRUE(rocketeer.disconnect_sem_.TimedWait(positive_timeout));

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
