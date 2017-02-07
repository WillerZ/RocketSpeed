/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS

#include <algorithm>
#include <array>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>

#include "include/Env.h"
#include "include/HostId.h"
#include "include/ProxyServer.h"
#include "include/Types.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/messages.h"
#include "src/messages/stream.h"
#include "src/messages/types.h"
#include "src/port/port.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

static constexpr std::chrono::milliseconds kPositiveTimeout{1000};
static constexpr std::chrono::milliseconds kNegativeTimeout{100};
static constexpr size_t kShard{0};

class MockSharding : public ShardingStrategy {
 public:
  static NamespaceID GetNamespace(size_t shard) {
    return std::to_string(shard);
  }

  size_t GetShard(
      Slice namespace_id, Slice topic_name,
      const IntroParameters&) const override {
    std::istringstream iss(namespace_id.ToString());
    size_t shard;
    iss >> shard;
    return shard;
  }

  size_t GetVersion() override { return version_.load(); }

  HostId GetHost(size_t shard) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return hosts_[shard];
  }

  void UpdateHost(size_t shard, const HostId& host) {
    std::lock_guard<std::mutex> lock(mutex_);
    hosts_[shard] = host;
    version_++;
  }

  void MarkHostDown(const HostId&) override {
  }

 private:
  std::mutex mutex_;
  std::atomic<uint64_t> version_;
  std::unordered_map<size_t, HostId> hosts_;
};

class MockDetector : public HotnessDetector {
 public:
  bool IsHotTopic(Slice namespace_id, Slice topic_name) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return collapsed_topics_.count(topic_name.ToString()) > 0;
  }

  void SetCollapsing(const Topic& topic_name, bool is_on) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_on) {
      collapsed_topics_.insert(topic_name);
    } else {
      collapsed_topics_.erase(topic_name);
    }
  }

 private:
  std::mutex mutex_;
  std::unordered_set<Topic> collapsed_topics_;
};

class MessageBus : public StreamReceiver {
 public:
  explicit MessageBus(EventLoop::Options options) {
    options.event_callback = [&](
        Flow* flow, std::unique_ptr<Message> msg, StreamID stream) {
      this->operator()({flow, stream, std::move(msg)});
    };
    loop_.reset(new EventLoop(options, StreamAllocator()));
    runner_.reset(new EventLoop::Runner(loop_.get()));
    // Can't use Wait as that'd try to inline the call.
    port::Semaphore done;
    Run([&]() {
      thread_check_.Check();
      done.Post();
    });
    EXPECT_TRUE(done.TimedWait(kPositiveTimeout));
  }

  ~MessageBus() {
    Wait([&]() { streams_.clear(); });
    // Relatively quick sanity check that we didn't miss any message.
    EXPECT_TRUE(NotReceived());
  }

  const HostId& GetHost() const { return loop_->GetHostId(); }

  MessageType PeekType(std::chrono::milliseconds timeout = kPositiveTimeout) {
    if (received_sem_.TimedWait(timeout)) {
      std::lock_guard<std::mutex> lock(received_mutex_);
      received_sem_.Post();  // Safe as we're holding a unique lock.
      return received_.front().message->GetMessageType();
    }
    return MessageType::NotInitialized;
  }

  StreamReceiveArg<Message> Receive(
      std::chrono::milliseconds timeout = kPositiveTimeout) {
    StreamReceiveArg<Message> arg = {nullptr, 0, nullptr};
    if (received_sem_.TimedWait(timeout)) {
      std::lock_guard<std::mutex> lock(received_mutex_);
      arg = std::move(received_.front());
      received_.pop_front();
    }
    return arg;
  }

  bool NotReceived(std::chrono::milliseconds timeout = kNegativeTimeout) {
    return !received_sem_.TimedWait(timeout);
  }

  StreamID OpenStream(const HostId& host_id, size_t shard_id = kShard) {
    StreamID stream_id;
    Wait([&]() {
      IntroParameters params;
      params.stream_properties.emplace(PropertyShardID,
                                       std::to_string(shard_id));
      auto new_stream = loop_->OpenStream(host_id, std::move(params));

      stream_id = new_stream->GetLocalID();
      auto result = streams_.emplace(stream_id, std::move(new_stream));
      RS_ASSERT(result.second);
      (void)result;
    });
    return stream_id;
  }

  void Send(StreamID stream_id, const Message& message) {
    LOG_DEBUG(loop_->GetLog(),
              "MessageBus::Send(%" PRIu64 ", %s)",
              stream_id,
              MessageTypeName(message.GetMessageType()));

    Wait([&]() {
      auto msg = Message::Copy(message);
      if (auto stream = loop_->GetInboundStream(stream_id)) {
        stream->Write(msg);
      } else {
        auto it = streams_.find(stream_id);
        ASSERT_TRUE(it != streams_.end());
        it->second->Write(msg);
      }
    });
  }

  void CloseStream(StreamID stream_id) {
    Wait([&]() {
      if (auto stream = loop_->GetInboundStream(stream_id)) {
        std::unique_ptr<Message> msg(
            new MessageGoodbye(Tenant::GuestTenant,
                               MessageGoodbye::Code::Graceful,
                               MessageGoodbye::OriginType::Server));
        stream->Write(msg);
      } else {
        streams_.erase(stream_id);
      }
    });
  }

  void Run(std::function<void()> callback) {
    std::unique_ptr<Command> command(MakeExecuteCommand(std::move(callback)));
    ASSERT_OK(loop_->SendCommand(command));
  }

  void Wait(std::function<void()> callback) {
    if (thread_check_.Ok()) {
      callback();
    } else {
      port::Semaphore done;
      Run([&]() {
        callback();
        done.Post();
      });
      ASSERT_TRUE(done.TimedWait(kPositiveTimeout));
    }
  }

 private:
  std::unique_ptr<EventLoop> loop_;
  std::unique_ptr<EventLoop::Runner> runner_;

  /// @{
  /// Single-threaded.
  ThreadCheck thread_check_;
  std::unordered_map<StreamID, std::unique_ptr<Stream>> streams_;
  /// @}

  /// @{
  /// Thread-safe.
  port::Semaphore received_sem_;
  std::mutex received_mutex_;
  std::deque<StreamReceiveArg<Message>> received_;
  /// @}

  void operator()(StreamReceiveArg<Message> arg) override {
    thread_check_.Check();
    auto type = arg.message->GetMessageType();
    LOG_DEBUG(loop_->GetLog(),
              "MessageBus::operator()(%" PRIu64 ", %s)",
              arg.stream_id,
              MessageTypeName(type));

    if (type == MessageType::mHeartbeat) {
      // these make tests noisy
      return;
    }

    if (type == MessageType::mGoodbye) {
      streams_.erase(arg.stream_id);
    }

    arg.flow = nullptr;
    std::lock_guard<std::mutex> lock(received_mutex_);
    received_.emplace_back(std::move(arg));
    received_sem_.Post();
  }
};

class ProxyServerTest : public ::testing::Test {
 public:
  Env* const env;
  std::shared_ptr<Logger> info_log;
  EventLoop::Options loop_options;

  ProxyServerTest() : env(Env::Default()) {
    EXPECT_OK(test::CreateLogger(env, "ProxyServerTest", &info_log));
    loop_options.info_log = info_log;
    loop_options.listener_port = 0;
  }

  ~ProxyServerTest() { env->WaitForJoin(); }
};

TEST_F(ProxyServerTest, Forwarding) {
  // Create routing and hot topics detection strategies for the proxy.
  auto routing = std::make_shared<MockSharding>();
  auto hot_topics = std::make_shared<MockDetector>();

  std::unique_ptr<ProxyServer> proxy;
  {  // Create the proxy.
    ProxyServerOptions proxy_options;
    proxy_options.info_log = info_log;
    proxy_options.routing = routing;
    proxy_options.hot_topics = hot_topics;
    ASSERT_OK(ProxyServer::Create(proxy_options, &proxy));
  }

  // Create two clients and two servers.
  std::vector<std::unique_ptr<MessageBus>> clients, servers;
  for (size_t i = 0; i < 2; ++i) {
    clients.emplace_back(std::make_unique<MessageBus>(loop_options));
    servers.emplace_back(std::make_unique<MessageBus>(loop_options));
    routing->UpdateHost(i, servers[i]->GetHost());
  }
  // Create one more server, as a replacement of servers[1];
  servers.emplace_back(std::make_unique<MessageBus>(loop_options));

  // Create two streams from clients to servers. We won't learn server's
  // StreamIDs until we receive the first message on each stream.
  // Send a pair of subscribe/unsubscribe messages on each stream.
  std::array<StreamID, 2> client_streams, server_streams;
  for (size_t i = 0; i < 2; ++i) {
    client_streams[i] =
        clients[i]->OpenStream(proxy->GetListenerAddress(), i /* shard_id */);

    auto sub_id = SubscriptionID::Unsafe(100 + i);
    clients[i]->Send(client_streams[i],
                     MessageSubscribe(Tenant::GuestTenant,
                                      MockSharding::GetNamespace(i),
                                      "ColdTopic",
                                      0,
                                      sub_id));
    // First message received by the server would be introduction
    auto received = servers[i]->Receive();
    ASSERT_TRUE(received.message);
    server_streams[i] = received.stream_id;
    ASSERT_EQ(MessageType::mIntroduction, received.message->GetMessageType());
    auto introduction =
        static_cast<MessageIntroduction*>(received.message.get());
    auto props = introduction->GetStreamProperties();
    auto shard = props.find(PropertyShardID);
    ASSERT_TRUE(shard != props.end());
    size_t shard_id = std::stoul(shard->second, nullptr, 0);
    ASSERT_EQ(shard_id, i);

    // Then subscribe
    received = servers[i]->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(MessageType::mSubscribe, received.message->GetMessageType());
    auto subscribe = static_cast<MessageSubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, subscribe->GetSubID());

    clients[i]->Send(
        client_streams[i],
        MessageUnsubscribe(Tenant::GuestTenant,
                           MockSharding::GetNamespace(i),
                           "ColdTopic",
                           sub_id,
                           MessageUnsubscribe::Reason::kRequested));

    received = servers[i]->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(server_streams[i], received.stream_id);
    ASSERT_EQ(MessageType::mUnsubscribe, received.message->GetMessageType());
    auto unsubscribe = static_cast<MessageUnsubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, unsubscribe->GetSubID());
  }

  // Fail over shard 1 to servers[2].
  routing->UpdateHost(1, servers[2]->GetHost());
  {  // servers[1] should have received a goodbye message.
    auto received = servers[1]->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(server_streams[1], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
  {  // clients[1] should have received a goodbye message.
    auto received = clients[1]->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(client_streams[1], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
  // We rename the servers for the rest of the test.
  std::swap(servers[1], servers[2]);

  // Recreate stream from the clients[1], the old one should have been closed.
  client_streams[1] =
      clients[1]->OpenStream(proxy->GetListenerAddress(), 1 /* shard_id */);

  // Send another pair of messages.
  for (size_t i = 0; i < 2; ++i) {
    auto sub_id = SubscriptionID::Unsafe(200 + i);

    clients[i]->Send(client_streams[i],
                     MessageSubscribe(Tenant::GuestTenant,
                                      MockSharding::GetNamespace(i),
                                      "ColdTopic",
                                      0,
                                      sub_id));
    // First message received by the server would be introduction for server 0
    auto received = servers[i]->Receive();
    ASSERT_TRUE(received.message);

    if (i == 1) {
      ASSERT_EQ(MessageType::mIntroduction, received.message->GetMessageType());
      server_streams[i] = received.stream_id;
      auto introduction =
          static_cast<MessageIntroduction*>(received.message.get());
      auto props = introduction->GetStreamProperties();
      auto shard = props.find(PropertyShardID);
      ASSERT_TRUE(shard != props.end());
      size_t shard_id = std::stoul(shard->second, nullptr, 0);
      ASSERT_EQ(shard_id, i);

      // Now a subscribe
      received = servers[i]->Receive();
    } else {
      ASSERT_EQ(server_streams[i], received.stream_id);
    }

    ASSERT_TRUE(received.message);
    ASSERT_EQ(MessageType::mSubscribe, received.message->GetMessageType());
    auto subscribe = static_cast<MessageSubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, subscribe->GetSubID());

    clients[i]->Send(
        client_streams[i],
        MessageUnsubscribe(Tenant::GuestTenant,
                           MockSharding::GetNamespace(i),
                           "ColdTopic",
                           sub_id,
                           MessageUnsubscribe::Reason::kRequested));

    received = servers[i]->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(server_streams[i], received.stream_id);
    ASSERT_EQ(MessageType::mUnsubscribe, received.message->GetMessageType());
    auto unsubscribe = static_cast<MessageUnsubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, unsubscribe->GetSubID());
  }

  {  // Close one stream from the client side.
    clients[0]->CloseStream(client_streams[0]);
    auto received = servers[0]->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(server_streams[0], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
  {  // And the other one from the server side.
    servers[1]->CloseStream(server_streams[1]);
    auto received = clients[1]->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(client_streams[1], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
}

TEST_F(ProxyServerTest, NoRoute) {
  // Create routing and hot topics detection strategies for the proxy.
  // We do not provide route for any shard.
  auto routing = std::make_shared<MockSharding>();
  auto hot_topics = std::make_shared<MockDetector>();

  std::unique_ptr<ProxyServer> proxy;
  {  // Create the proxy.
    ProxyServerOptions proxy_options;
    proxy_options.info_log = info_log;
    proxy_options.routing = routing;
    proxy_options.hot_topics = hot_topics;
    ASSERT_OK(ProxyServer::Create(proxy_options, &proxy));
  }

  // Create a client and no server.
  MessageBus client(loop_options);

  // Create a stream to the proxy and send a message.
  const StreamID client_stream = client.OpenStream(proxy->GetListenerAddress());
  client.Send(client_stream,
              MessageSubscribe(Tenant::GuestTenant,
                               MockSharding::GetNamespace(0),
                               "ColdTopic",
                               0,
                               SubscriptionID::Unsafe(100)));
  // We should immediately see a MessageGoodbye, as the original message sent to
  // the proxy cannot be routed.
  auto received = client.Receive();
  EXPECT_TRUE(received.message);
  EXPECT_EQ(client_stream, received.stream_id);
  ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
}

namespace {

struct ExpectedCall {
  std::string contents;
  SequenceNumber prev_seqno;
  SequenceNumber current_seqno;
};

class SequenceVerifier {
 public:
  explicit SequenceVerifier(std::deque<ExpectedCall> sequence)
  : sequence_(std::move(sequence)) {}

  // Noncopyable
  SequenceVerifier(const SequenceVerifier&) = delete;
  SequenceVerifier& operator=(const SequenceVerifier&) = delete;
  // Movable
  SequenceVerifier(SequenceVerifier&&) = default;
  SequenceVerifier& operator=(SequenceVerifier&&) = default;

  ~SequenceVerifier() { EXPECT_TRUE(sequence_.empty()); }

  bool Call(Slice contents,
            SequenceNumber prev_seqno,
            SequenceNumber current_seqno) {
    EXPECT_TRUE(!sequence_.empty());
    auto& expected = sequence_.front();
    EXPECT_EQ(expected.contents, contents);
    EXPECT_EQ(expected.prev_seqno, prev_seqno);
    EXPECT_EQ(expected.current_seqno, current_seqno);
    sequence_.pop_front();
    return true;
  }

 private:
  std::deque<ExpectedCall> sequence_;
};

class SequencePreparer {
 public:
  void AddExpect(Slice contents,
                 SequenceNumber prev_seqno,
                 SequenceNumber current_seqno) {
    sequence_.emplace_back(
        ExpectedCall{contents.ToString(), prev_seqno, current_seqno});
  }

  void ClearExpectations() { sequence_.clear(); }

  UpdatesAccumulator::ConsumerCb operator()() const {
    auto verifier = std::make_shared<SequenceVerifier>(sequence_);
    return [verifier](Slice contents,
                      SequenceNumber prev_seqno,
                      SequenceNumber current_seqno) {
      return verifier->Call(contents, prev_seqno, current_seqno);
    };
  }

 private:
  std::deque<ExpectedCall> sequence_;
};

}  // namespace

TEST_F(ProxyServerTest, DefaultAccumulator) {
  using Action = UpdatesAccumulator::Action;
  auto acc = UpdatesAccumulator::CreateDefault(2 /* count_limit */);

  SequencePreparer seq;
  // Establish the first subscription.
  ASSERT_EQ(0, acc->BootstrapSubscription(0, seq()));
  // Deliver a snapshot on an upstream subscription.
  ASSERT_TRUE(Action::kNoOp == acc->ConsumeUpdate("snapshot1", 0, 100));
  seq.AddExpect("snapshot1", 0, 100);
  // A new subscription receives a snapshot, as it's stored in the
  // accumulator.
  ASSERT_EQ(101, acc->BootstrapSubscription(0, seq()));
  // Deliver a delta. At the same time, the server is asked to provide a
  // snapshot, as we've run out of buffer for updates. To the server, it looks
  // like a brand new subscription.
  ASSERT_TRUE(Action::kResubscribeUpstream ==
              acc->ConsumeUpdate("delta1", 101, 103));
  seq.AddExpect("delta1", 101, 103);
  // A new subscription receives a snapshot and a delta, as it's stored in the
  // accumulator.
  ASSERT_EQ(104, acc->BootstrapSubscription(0, seq()));
  // The server delivers a snapshot.
  ASSERT_TRUE(Action::kNoOp == acc->ConsumeUpdate("snapshot2", 0, 103));
  seq.ClearExpectations();
  seq.AddExpect("snapshot2", 0, 103);
  ASSERT_EQ(104, acc->BootstrapSubscription(0, seq()));
  // And another, fresher snapshot right after.
  ASSERT_TRUE(Action::kNoOp == acc->ConsumeUpdate("snapshot3", 0, 105));
  seq.ClearExpectations();
  seq.AddExpect("snapshot3", 0, 105);
  ASSERT_EQ(106, acc->BootstrapSubscription(0, seq()));
}

TEST_F(ProxyServerTest, Multiplexing_DefaultAccumulator) {
  const size_t shard = kShard;
  // Create routing and hot topics detection strategies for the proxy.
  auto routing = std::make_shared<MockSharding>();
  auto hot_topics = std::make_shared<MockDetector>();
  // Make one topic hot.
  hot_topics->SetCollapsing("HotTopic", true);
  ASSERT_TRUE(hot_topics->IsHotTopic(std::to_string(shard), "HotTopic"));

  std::unique_ptr<ProxyServer> proxy;
  {  // Create the proxy.
    ProxyServerOptions proxy_options;
    proxy_options.info_log = info_log;
    proxy_options.routing = routing;
    proxy_options.hot_topics = hot_topics;
    proxy_options.accumulator = [&](Slice, Slice) {
      return UpdatesAccumulator::CreateDefault(2 /* count limit */);
    };
    ASSERT_OK(ProxyServer::Create(proxy_options, &proxy));
  }

  // Create a client and a server.
  auto client = std::make_unique<MessageBus>(loop_options);
  auto server = std::make_unique<MessageBus>(loop_options);
  routing->UpdateHost(shard, server->GetHost());

  // All subscriptions will originate from a single client (which doesn't
  // matter) and will refer to the only hot topic. We'll interleave subscribe
  // messages with message deliveries from the server to excersise the code
  // paths around.
  const StreamID client_stream =
      client->OpenStream(proxy->GetListenerAddress());
  StreamID server_stream;
  // A tiny DSL to make the test readable.
  auto issue_subscribe = [&](uint64_t downstream_sub) {
    client->Send(client_stream,
                 MessageSubscribe(Tenant::GuestTenant,
                                  MockSharding::GetNamespace(shard),
                                  "HotTopic",
                                  0,
                                  SubscriptionID::Unsafe(downstream_sub)));
  };
  auto issue_unsubscribe = [&](uint64_t downstream_id) {
    client->Send(client_stream,
                 MessageUnsubscribe(Tenant::GuestTenant,
                                    MockSharding::GetNamespace(shard),
                                    "HotTopic",
                                    SubscriptionID::Unsafe(downstream_id),
                                    MessageUnsubscribe::Reason::kRequested));
  };
  auto receive_introduction = [&]() {
    auto received = server->Receive();
    EXPECT_TRUE(received.message);
    server_stream = received.stream_id;
    auto message = received.message.get();
    EXPECT_EQ(MessageType::mIntroduction, message->GetMessageType());
    auto introduction =
        static_cast<MessageIntroduction*>(received.message.get());
    auto props = introduction->GetStreamProperties();
    auto shardIt = props.find(PropertyShardID);
    ASSERT_TRUE(shardIt != props.end());
    size_t shard_id = std::stoul(shardIt->second, nullptr, 0);
    ASSERT_EQ(shard_id, shard);
  };
  auto receive_subscribe = [&](uint64_t expected_sub_id = 0) {
    auto received = server->Receive();
    EXPECT_TRUE(received.message);
    server_stream = received.stream_id;
    auto message = received.message.get();
    EXPECT_EQ(MessageType::mSubscribe, message->GetMessageType());
    SubscriptionID sub_id = static_cast<MessageSubscribe*>(message)->GetSubID();
    EXPECT_TRUE(sub_id);
    if (expected_sub_id) {
      EXPECT_EQ(expected_sub_id, sub_id);
    }
    return sub_id;
  };
  auto receive_unsubscribe = [&](uint64_t expected_sub_id) {
    auto received = server->Receive();
    ASSERT_TRUE(received.message);
    auto message = received.message.get();
    ASSERT_EQ(MessageType::mUnsubscribe, message->GetMessageType());
    SubscriptionID sub_id =
        static_cast<MessageUnsubscribe*>(message)->GetSubID();
    ASSERT_EQ(expected_sub_id, sub_id);
  };
  auto deliver_data = [&](uint64_t upstream_sub,
                          SequenceNumber prev_seqno,
                          SequenceNumber current_seqno,
                          Slice payload) {
    MessageDeliverData data(Tenant::GuestTenant,
                            MockSharding::GetNamespace(shard),
                            "HotTopic",
                            SubscriptionID::Unsafe(upstream_sub),
                            MsgId(),
                            payload.ToString());
    data.SetSequenceNumbers(prev_seqno, current_seqno);
    server->Send(server_stream, data);
  };
  auto receive_data = [&](uint64_t downstream_sub,
                          SequenceNumber prev_seqno,
                          SequenceNumber current_seqno,
                          Slice payload) {
    auto received = client->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(client_stream, received.stream_id);
    auto message = received.message.get();
    ASSERT_EQ(MessageType::mDeliverData, message->GetMessageType());
    auto data = static_cast<MessageDeliverData*>(message);
    ASSERT_EQ(downstream_sub, data->GetSubID());
    ASSERT_EQ(prev_seqno, data->GetPrevSequenceNumber());
    ASSERT_EQ(current_seqno, data->GetSequenceNumber());
    ASSERT_EQ(payload, data->GetPayload());
  };
  auto counter_value = [&](const std::string& name) -> int64_t {
    struct MockVisitor : public StatisticsVisitor {
      const std::string& name_;
      int64_t value_;
      bool set_{false};

      explicit MockVisitor(const std::string& _name) : name_(_name) {}

      void VisitCounter(const std::string& _name, int64_t value) override {
        if (name_ == _name) {
          value_ = value;
          set_ = true;
        }
      }
    } visitor(name);
    proxy->ExportStatistics(&visitor);
    EXPECT_TRUE(visitor.set_);
    return visitor.value_;
  };
  auto check_counters = [&](
      int64_t streams, int64_t downstreams_subs, int64_t upstream_subs) {
    ASSERT_EQ(streams, counter_value("upstream.num_streams"));
    ASSERT_EQ(1, counter_value("upstream.num_shards"));
    ASSERT_EQ(downstreams_subs,
              counter_value("per_stream.num_downstream_subscriptions"));
    ASSERT_EQ(upstream_subs,
              counter_value("multiplexer.num_upstream_subscriptions"));
  };

  // Establish the first subscription.
  issue_subscribe(1);
  receive_introduction();
  SubscriptionID upstream_sub = receive_subscribe();
  check_counters(1, 1, 1);
  // Deliver a snapshot on an upstream subscription.
  deliver_data(upstream_sub, 0, 100, "snapshot1");
  receive_data(1, 0, 100, "snapshot1");
  // Establish the second subscription.
  issue_subscribe(2);
  // A new subscription receives a snapshot, as it's stored in the
  // accumulator.
  receive_data(2, 0, 100, "snapshot1");
  check_counters(1, 2, 1);
  // Take care to have only one downstream subscription at the time of message
  // delivery, otherwise the assertions get messy, as the order of downstream
  // subscribers receiving a message is non-deterministic.
  issue_unsubscribe(1);
  // That should not trigger any notification to the server.
  ASSERT_TRUE(server->NotReceived());
  // Deliver a delta.
  deliver_data(upstream_sub, 101, 103, "delta2");
  receive_data(2, 101, 103, "delta2");
  check_counters(1, 1, 1);
  // At the same time, the server is asked to provide a snapshot, as we've run
  // out of buffer for updates. To the server, it looks like a brand new
  // subscription.
  if (server->PeekType() == MessageType::mUnsubscribe) {
    receive_unsubscribe(upstream_sub);
    upstream_sub = receive_subscribe();
  } else {
    auto old_upstream_sub = upstream_sub;
    upstream_sub = receive_subscribe();
    receive_unsubscribe(old_upstream_sub);
  }
  // Establish the third subscription.
  issue_subscribe(3);
  // A new subscription receives a snapshot and a delta, as it's stored in the
  // accumulator.
  receive_data(3, 0, 100, "snapshot1");
  receive_data(3, 101, 103, "delta2");
  check_counters(1, 2, 1);
  issue_unsubscribe(2);
  // The server finally delivers a snapshot.
  deliver_data(upstream_sub, 0, 103, "snapshot2");
  // Downstream subscribers won't actuallly receive this message, as it's not
  // newer than previously obtained sequence consisting of a snapshot and a
  // delta.
  ASSERT_TRUE(client->NotReceived());
  // But sending a snapshot that transitions into a fresher state does affect
  // downstream subscriptions.
  deliver_data(upstream_sub, 0, 105, "snapshot3");
  receive_data(3, 0, 105, "snapshot3");
  check_counters(1, 1, 1);
  // Establish a fourth subscription, that one will receive the last snapshot,
  // which evicted the previous snapshot from the accumulator even though the
  // buffer wasn't full.
  issue_subscribe(4);
  receive_data(4, 0, 105, "snapshot3");
  check_counters(1, 2, 1);

  // Shut down the client.
  client.reset();
  // And read a goodbye message on the server.
  auto received = server->Receive();
  ASSERT_TRUE(received.message);
  ASSERT_EQ(server_stream, received.stream_id);
  ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
}

TEST_F(ProxyServerTest, ForwardingAndMultiplexing) {
  const size_t shard = kShard;
  // Create routing and hot topics detection strategies for the proxy.
  auto routing = std::make_shared<MockSharding>();
  auto hot_topics = std::make_shared<MockDetector>();
  // Make one topic hot.
  hot_topics->SetCollapsing("HotTopic", true);
  ASSERT_TRUE(hot_topics->IsHotTopic(std::to_string(shard), "HotTopic"));
  ASSERT_TRUE(!hot_topics->IsHotTopic(std::to_string(shard), "ColdTopic"));

  std::unique_ptr<ProxyServer> proxy;
  {  // Create the proxy.
    ProxyServerOptions proxy_options;
    proxy_options.info_log = info_log;
    proxy_options.routing = routing;
    proxy_options.hot_topics = hot_topics;
    proxy_options.accumulator = [&](Slice, Slice) {
      return UpdatesAccumulator::CreateDefault(2 /* count limit */);
    };
    ASSERT_OK(ProxyServer::Create(proxy_options, &proxy));
  }

  // Create a client and a server.
  auto client = std::make_unique<MessageBus>(loop_options);
  auto server = std::make_unique<MessageBus>(loop_options);
  routing->UpdateHost(shard, server->GetHost());

  // Establish subscriptions on a hot and a cold topic on a single stream.
  const StreamID client_stream =
      client->OpenStream(proxy->GetListenerAddress());
  std::array<StreamID, 2> server_streams;
  std::array<SubscriptionID, 2> server_sub_ids;
  size_t i = 0;
  for (auto* topic_name : {"ColdTopic", "HotTopic"}) {
    // Send a subscribe message.
    auto client_sub_id = SubscriptionID::Unsafe(100 + i);
    client->Send(client_stream,
                 MessageSubscribe(Tenant::GuestTenant,
                                  MockSharding::GetNamespace(shard),
                                  topic_name,
                                  0,
                                  client_sub_id));
    {
      auto received = server->Receive();

      // Introduction message
      ASSERT_TRUE(received.message);
      ASSERT_EQ(MessageType::mIntroduction, received.message->GetMessageType());
      server_streams[i] = received.stream_id;

      received = server->Receive();
      ASSERT_TRUE(received.message);
      ASSERT_EQ(MessageType::mSubscribe, received.message->GetMessageType());
      auto subscribe = static_cast<MessageSubscribe*>(received.message.get());
      ASSERT_EQ(Slice(topic_name), subscribe->GetTopicName());
      server_sub_ids[i] = subscribe->GetSubID();
    }
    // Send back a message on the subscription.
    MessageDeliverData data(
        Tenant::GuestTenant, MockSharding::GetNamespace(shard), topic_name,
        server_sub_ids[i], MsgId(), "Payload");
    data.SetSequenceNumbers(0, 1003);
    server->Send(server_streams[i], data);
    {
      auto received = client->Receive();
      ASSERT_TRUE(received.message);
      ASSERT_EQ(client_stream, received.stream_id);
      ASSERT_EQ(MessageType::mDeliverData, received.message->GetMessageType());
      auto deliver = static_cast<MessageDeliverData*>(received.message.get());
      ASSERT_EQ("Payload", deliver->GetPayload());
    }
    ++i;
  }
  // Since cold topics are not multiplexed, the SubscriptionIDs are passed
  // verbatim.
  ASSERT_EQ(100, server_sub_ids[0]);
  // Currently, proxy uses different streams for hot and cold topics. This
  // might
  // change in the future -- adjust the assertion if that happens.
  ASSERT_TRUE(server_streams[0] != server_streams[1]);

  // Create another stream with a subscription on a cold topic to prevent
  // internal GC from destroying internal structures that keep shard's
  // context,
  // which would lead to the server's multiplexed stream receiving a
  // MessageGoodbye. We will not touch that stream until the end of the test.
  const StreamID another_client_stream =
      client->OpenStream(proxy->GetListenerAddress());
  {
    ASSERT_TRUE(another_client_stream != client_stream);
    client->Send(another_client_stream,
                 MessageSubscribe(Tenant::GuestTenant,
                                  MockSharding::GetNamespace(shard),
                                  "ColdTopic",
                                  0,
                                  SubscriptionID::Unsafe(100)));

    auto received = server->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_TRUE(server_streams.end() == std::find(server_streams.begin(),
                                                  server_streams.end(),
                                                  received.stream_id));
    ASSERT_EQ(MessageType::mIntroduction, received.message->GetMessageType());

    received = server->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(MessageType::mSubscribe, received.message->GetMessageType());
    auto subscribe = static_cast<MessageSubscribe*>(received.message.get());
    ASSERT_EQ("ColdTopic", subscribe->GetTopicName());
    ASSERT_EQ(100, subscribe->GetSubID());
  }

  // Server says goodbye on the stream that forwards a bulk of subscriptions.
  server->Send(server_streams[0],
               MessageGoodbye(Tenant::GuestTenant,
                              MessageGoodbye::Code::Graceful,
                              MessageGoodbye::OriginType::Server));
  {  // Proxy should forward the message...
    auto received = client->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(client_stream, received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
  {  // ...as well as terminate the subscription on hot topic.
    auto received = server->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(server_streams[1], received.stream_id);
    ASSERT_EQ(MessageType::mUnsubscribe, received.message->GetMessageType());
    auto unsubscribe = static_cast<MessageUnsubscribe*>(received.message.get());
    ASSERT_EQ(server_sub_ids[1], unsubscribe->GetSubID());
  }

  // Shut down the client.
  client.reset();
  // And read goodbye messages on the server -- one on the subscriber stream
  // and one on a stream that had only cold subscription on it.
  for (size_t j = 0; j < 2; ++j) {
    auto received = server->Receive();
    ASSERT_TRUE(received.message);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
