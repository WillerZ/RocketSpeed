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

#include "external/folly/Memory.h"

#include "include/Env.h"
#include "include/HostId.h"
#include "include/ProxyServer.h"
#include "include/Types.h"
#include "src/messages/event_loop.h"
#include "src/messages/messages.h"
#include "src/messages/stream.h"
#include "src/messages/types.h"
#include "src/port/port.h"
#include "src/util/common/flow_control.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class MockRouter : public SubscriptionRouter {
 public:
  size_t GetVersion() override { return version_.load(); }

  HostId GetHost() override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto host = host_;
    return host;
  }

  void MarkHostDown(const HostId& host) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (host == host_) {
      marked_down_ = true;
    }
  }

  void SetHost(const HostId& host) {
    std::lock_guard<std::mutex> lock(mutex_);
    host_ = host;
    version_++;
  }

 private:
  std::atomic<uint64_t> version_;
  std::mutex mutex_;
  HostId host_;
  bool marked_down_{false};
};

class MockSharding : public ShardingStrategy {
 public:
  static NamespaceID GetNamespace(size_t shard) {
    return std::to_string(shard);
  }

  size_t GetShard(const NamespaceID& namespace_id,
                  const Topic& topic_name) const override {
    std::istringstream iss(namespace_id);
    size_t shard;
    iss >> shard;
    return shard;
  }

  std::unique_ptr<SubscriptionRouter> GetRouter(size_t shard) override {
    class ProxyRouter : public SubscriptionRouter {
     public:
      std::shared_ptr<SubscriptionRouter> router;

      size_t GetVersion() override { return router->GetVersion(); }

      HostId GetHost() override { return router->GetHost(); }

      void MarkHostDown(const HostId& host) override {
        router->MarkHostDown(host);
      }
    };
    auto router = folly::make_unique<ProxyRouter>();
    router->router = GetMockRouter(shard);
    return std::move(router);
  }

  const std::shared_ptr<MockRouter>& GetMockRouter(size_t shard) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& router = routers_[shard];
    if (!router) {
      router.reset(new MockRouter());
    }
    return router;
  }

 private:
  std::mutex mutex_;
  std::unordered_map<size_t, std::shared_ptr<MockRouter>> routers_;
};

class MockCollapsing : public TopicCollapsingStrategy {
 public:
  bool ShouldCollapse(const Slice& namespace_id,
                      const Slice& topic_name) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return collapsed_topics_.size() > 0;
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
    Run([this]() { thread_check_.Check(); });
  }

  const HostId& GetHost() const { return loop_->GetHostId(); }

  StreamReceiveArg<Message> Receive(std::chrono::milliseconds timeout) {
    StreamReceiveArg<Message> arg;
    ASSERT_TRUE(received_sem_.TimedWait(timeout));
    std::lock_guard<std::mutex> lock(received_mutex_);
    arg = std::move(received_.front());
    received_.pop_front();
    return arg;
  }

  void CheckNotReceived(std::chrono::milliseconds timeout) {
    ASSERT_TRUE(!received_sem_.TimedWait(timeout));
  }

  StreamID OpenStream(const HostId& host_id) {
    StreamID stream_id;
    Wait([&]() {
      auto new_stream = loop_->OpenStream(host_id);
      stream_id = new_stream->GetLocalID();
      auto result = streams_.emplace(stream_id, std::move(new_stream));
      RS_ASSERT(result.second);
      (void)result;
    });
    return stream_id;
  }

  void Send(StreamID stream_id, const Message& message) {
    Wait([&]() {
      if (auto stream = loop_->GetInboundStream(stream_id)) {
        stream->Write(message);
      } else {
        auto it = streams_.find(stream_id);
        if (it != streams_.end()) {
          it->second->Write(message);
        }
      }
    });
  }

  void CloseStream(StreamID stream_id) {
    Wait([&]() {
      if (auto stream = loop_->GetInboundStream(stream_id)) {
        MessageGoodbye message(Tenant::GuestTenant,
                               MessageGoodbye::Code::Graceful,
                               MessageGoodbye::OriginType::Server);
        stream->Write(message);
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
      ASSERT_TRUE(done.TimedWait(std::chrono::seconds(1)));
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
    if (type == MessageType::mGoodbye) {
      streams_.erase(arg.stream_id);
    }

    arg.flow = nullptr;
    std::lock_guard<std::mutex> lock(received_mutex_);
    received_.emplace_back(std::move(arg));
    received_sem_.Post();
  }
};

class ProxyServerTest {
 public:
  const std::chrono::milliseconds positive_timeout;
  const std::chrono::milliseconds negative_timeout;
  Env* const env;
  std::shared_ptr<Logger> info_log;
  EventLoop::Options loop_options;

  ProxyServerTest()
  : positive_timeout(1000), negative_timeout(100), env(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env, "ProxyServerTest", &info_log));
    loop_options.info_log = info_log;
    loop_options.listener_port = 0;
  }

  ~ProxyServerTest() { env->WaitForJoin(); }
};

TEST(ProxyServerTest, TwoClientsTwoServers) {
  // Create routing and collapsing strategies for the proxy.
  auto routing = std::make_shared<MockSharding>();
  auto collapsing = std::make_shared<MockCollapsing>();

  // Create two clients and two servers.
  std::vector<std::unique_ptr<MessageBus>> clients, servers;
  for (size_t i = 0; i < 2; ++i) {
    clients.emplace_back(folly::make_unique<MessageBus>(loop_options));
    servers.emplace_back(folly::make_unique<MessageBus>(loop_options));
    routing->GetMockRouter(i)->SetHost(servers[i]->GetHost());
  }
  // Create one more server, as a replacement of servers[1];
  servers.emplace_back(folly::make_unique<MessageBus>(loop_options));

  std::unique_ptr<ProxyServer> proxy;
  {  // Create the proxy.
    ProxyServerOptions proxy_options;
    proxy_options.info_log = info_log;
    proxy_options.routing = routing;
    proxy_options.collapsing = collapsing;
    ASSERT_OK(ProxyServer::Create(proxy_options, &proxy));
  }

  // Create two streams from clients to servers. We won't learn server's
  // StreamIDs until we receive the first message on each stream.
  // Send a pair of subscribe/unsubscribe messages on each stream.
  std::array<StreamID, 2> client_streams, server_streams;
  for (size_t i = 0; i < 2; ++i) {
    client_streams[i] = clients[i]->OpenStream(proxy->GetListenerAddress());

    SubscriptionID sub_id = 100 + i;
    clients[i]->Send(client_streams[i],
                     MessageSubscribe(Tenant::GuestTenant,
                                      MockSharding::GetNamespace(i),
                                      "TwoClientsTwoServers",
                                      0,
                                      sub_id));
    auto received = servers[i]->Receive(positive_timeout);
    server_streams[i] = received.stream_id;
    ASSERT_EQ(MessageType::mSubscribe, received.message->GetMessageType());
    auto subscribe = static_cast<MessageSubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, subscribe->GetSubID());

    clients[i]->Send(
        client_streams[i],
        MessageUnsubscribe(Tenant::GuestTenant,
                           sub_id,
                           MessageUnsubscribe::Reason::kRequested));

    received = servers[i]->Receive(positive_timeout);
    ASSERT_EQ(server_streams[i], received.stream_id);
    ASSERT_EQ(MessageType::mUnsubscribe, received.message->GetMessageType());
    auto unsubscribe = static_cast<MessageUnsubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, unsubscribe->GetSubID());
  }

  // Fail over shard 1 to servers[2].
  routing->GetMockRouter(1)->SetHost(servers[2]->GetHost());
  {  // servers[1] should have received a goodbye message.
    auto received = servers[1]->Receive(positive_timeout * 10);
    ASSERT_EQ(server_streams[1], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
  {  // clients[1] should have received a goodbye message.
    auto received = clients[1]->Receive(positive_timeout * 10);
    ASSERT_EQ(client_streams[1], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
  // We rename the servers for the rest of the test.
  std::swap(servers[1], servers[2]);

  // Recreate stream from the clients[1], the old one should have been closed.
  client_streams[1] = clients[1]->OpenStream(proxy->GetListenerAddress());

  // Send another pair of messages.
  for (size_t i = 0; i < 2; ++i) {
    SubscriptionID sub_id = 200 + i;

    clients[i]->Send(client_streams[i],
                     MessageSubscribe(Tenant::GuestTenant,
                                      MockSharding::GetNamespace(i),
                                      "TwoClientsTwoServers",
                                      0,
                                      sub_id));
    auto received = servers[i]->Receive(positive_timeout);
    if (i == 1) {
      server_streams[i] = received.stream_id;
    } else {
      ASSERT_EQ(server_streams[i], received.stream_id);
    }
    ASSERT_EQ(MessageType::mSubscribe, received.message->GetMessageType());
    auto subscribe = static_cast<MessageSubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, subscribe->GetSubID());

    clients[i]->Send(
        client_streams[i],
        MessageUnsubscribe(Tenant::GuestTenant,
                           sub_id,
                           MessageUnsubscribe::Reason::kRequested));

    received = servers[i]->Receive(positive_timeout);
    ASSERT_EQ(server_streams[i], received.stream_id);
    ASSERT_EQ(MessageType::mUnsubscribe, received.message->GetMessageType());
    auto unsubscribe = static_cast<MessageUnsubscribe*>(received.message.get());
    ASSERT_EQ(sub_id, unsubscribe->GetSubID());
  }

  {  // Close one stream from the client side.
    clients[0]->CloseStream(client_streams[0]);
    auto received = servers[0]->Receive(positive_timeout);
    ASSERT_EQ(server_streams[0], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }
  {  // And the other one from the server side.
    servers[1]->CloseStream(server_streams[1]);
    auto received = clients[1]->Receive(positive_timeout);
    ASSERT_EQ(client_streams[1], received.stream_id);
    ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());
  }

  for (auto& client : clients) {
    client->CheckNotReceived(negative_timeout);
  }
  for (auto& server : servers) {
    server->CheckNotReceived(negative_timeout);
  }
}

TEST(ProxyServerTest, NoRoute) {
  // Create routing and collapsing strategies for the proxy.
  // We do not provide route for any shard.
  auto routing = std::make_shared<MockSharding>();
  auto collapsing = std::make_shared<MockCollapsing>();

  // Create a client and no server.
  MessageBus client(loop_options);

  std::unique_ptr<ProxyServer> proxy;
  {  // Create the proxy.
    ProxyServerOptions proxy_options;
    proxy_options.info_log = info_log;
    proxy_options.routing = routing;
    proxy_options.collapsing = collapsing;
    ASSERT_OK(ProxyServer::Create(proxy_options, &proxy));
  }

  // Create a stream to the proxy and send a message.
  auto client_stream = client.OpenStream(proxy->GetListenerAddress());
  client.Send(client_stream,
              MessageSubscribe(Tenant::GuestTenant,
                               MockSharding::GetNamespace(0),
                               "TwoClientsTwoServers",
                               0,
                               100));
  // We should immediately see a MessageGoodbye, as the original message sent to
  // the proxy cannot be routed.
  auto received = client.Receive(positive_timeout);
  ASSERT_EQ(client_stream, received.stream_id);
  ASSERT_EQ(MessageType::mGoodbye, received.message->GetMessageType());

  client.CheckNotReceived(negative_timeout);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
