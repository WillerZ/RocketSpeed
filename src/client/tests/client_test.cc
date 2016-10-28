//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "external/folly/Memory.h"

#include "include/Env.h"
#include "include/RocketSpeed.h"
#include "include/ShadowedClient.h"
#include "src/client/single_shard_subscriber.h"
#include "src/client/topic_subscription_map.h"
#include "src/client/tail_collapsing_subscriber.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/common/client_env.h"
#include "src/util/common/random.h"
#include "src/util/random.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class MockShardingStrategy;

class MockPublisherRouter : public PublisherRouter {
 public:
  Status GetPilot(HostId* host_out) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    *host_out = pilot_;
    return !pilot_ ? Status::NotFound("") : Status::OK();
  }

  void SetPilot(HostId host) {
    std::lock_guard<std::mutex> lock(mutex_);
    pilot_ = host;
    ++version_;
  }

  void SetCopilot(HostId host) {
    std::lock_guard<std::mutex> lock(mutex_);
    copilot_ = host;
    ++version_;
  }

 private:
  mutable std::mutex mutex_;
  mutable std::atomic<uint64_t> version_;
  HostId pilot_;
  HostId copilot_;

  HostId GetCopilot() const {
    HostId out;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      out = copilot_;
    }
    return out;
  }

  friend class MockShardingStrategy;
};

class MockShardingStrategy : public ShardingStrategy {
 public:
  explicit MockShardingStrategy(std::shared_ptr<MockPublisherRouter> config)
  : config_(config) {
  }

  size_t GetShard(Slice namespace_id, Slice topic_name) const override {
    return 0;
  }

  size_t GetVersion() override { return config_->version_.load(); }

  HostId GetHost(size_t) override { return config_->GetCopilot(); }

  void MarkHostDown(const HostId& host_id) override {}

 private:
  const std::shared_ptr<MockPublisherRouter> config_;
};

class MockObserver : public Observer {
 public:
  static void StaticReset() {
    active_count_ = 0;
    deleted_count_ = 0;
  }

  explicit MockObserver(SubscriptionHandle handle)
  : handle_(handle), received_count_(0) {
    ++active_count_;
  }

  ~MockObserver() {
    --active_count_;
    ++deleted_count_;
  }

  virtual void OnMessageReceived(Flow*, std::unique_ptr<MessageReceived>& msg) {
    ASSERT_EQ(handle_, msg->GetSubscriptionHandle());
    ++received_count_;
  }

  // How many MockObservers are currently allocated
  static std::atomic<int> active_count_;
  static std::atomic<int> deleted_count_;

  SubscriptionHandle handle_;
  int received_count_;
};
std::atomic<int> MockObserver::active_count_;
std::atomic<int> MockObserver::deleted_count_;

class MockSubscriber : public SubscriberIf
{
  virtual void StartSubscription(SubscriptionID sub_id,
                                 SubscriptionParameters parameters,
                                 std::unique_ptr<Observer> observer) override {
    // MockSubscriber supports only one subscription id
    ASSERT_TRUE(!subscription_state_);

    TenantAndNamespaceFactory factory;
    auto tenant_and_namespace = factory.GetFlyweight(
      {parameters.tenant_id, parameters.namespace_id});
    void* user_data = static_cast<void*>(observer.release());
    subscription_state_ = folly::make_unique<SubscriptionBase>(
      tenant_and_namespace,
      parameters.topic_name,
      sub_id, parameters.start_seqno, user_data);
    sub_id_ = sub_id;
  }

  virtual void Acknowledge(SubscriptionID sub_id,
                           SequenceNumber seqno) override {
  }

  virtual void TerminateSubscription(SubscriptionID sub_id) override {
    ASSERT_TRUE(subscription_state_ && sub_id_ == sub_id);
    using Info = MockSubscriber::Info;
    Info info;
    Select(sub_id, Info::kAll, &info);
    SubscriptionStatusImpl sub_status(sub_id, info.GetTenant(),
        info.GetNamespace(), info.GetTopic(), info.GetSequenceNumber());
    info.GetObserver()->OnSubscriptionStatusChange(sub_status);
    delete info.GetObserver();
    subscription_state_->SetUserData(nullptr);
    subscription_state_ = nullptr;
  }

  virtual bool Empty() const override {
    return subscription_state_.get() == nullptr;
  }

  virtual Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                           size_t worker_id) override {
    return Status::NotSupported("This is a mock subscriber");
  }

  virtual bool Select(
      SubscriptionID sub_id, Info::Flags flags, Info* info) const override {
    if (sub_id == sub_id_ && subscription_state_) {
      if (flags & Info::kTenant) {
        info->SetTenant(subscription_state_->GetTenant());
      }
      if (flags & Info::kNamespace) {
        info->SetNamespace(subscription_state_->GetNamespace().ToString());
      }
      if (flags & Info::kTopic) {
        info->SetTopic(subscription_state_->GetTopicName().ToString());
      }
      if (flags & Info::kSequenceNumber) {
        info->SetSequenceNumber(subscription_state_->GetExpectedSeqno());
      }
      if (flags & Info::kObserver) {
        info->SetObserver(
            static_cast<Observer*>(subscription_state_->GetUserData()));
      }
      return true;
    } else {
      return false;
    }
  }

  virtual void SetUserData(SubscriptionID sub_id, void* user_data) override {
    RS_ASSERT(sub_id == sub_id_);
    RS_ASSERT(subscription_state_);
    subscription_state_->SetUserData(user_data);
  }

  virtual void RefreshRouting() override {}

  virtual void NotifyHealthy(bool) override {}

 private:
  std::unique_ptr<SubscriptionBase> subscription_state_;
  SubscriptionID sub_id_;
};

static std::unique_ptr<ShardingStrategy> MakeShardingStrategyFromConfig(
    std::shared_ptr<MockPublisherRouter> config) {
  return folly::make_unique<MockShardingStrategy>(config);
}

class ClientTest : public ::testing::Test {
 public:
  ClientTest()
  : positive_timeout(5000)
  , negative_timeout(100)
  , env_(Env::Default())
  , config_(std::make_shared<MockPublisherRouter>())
  , shadow_config_(std::make_shared<MockPublisherRouter>()) {
    EXPECT_OK(test::CreateLogger(env_, "ClientTest", &info_log_));
  }

  virtual ~ClientTest() { env_->WaitForJoin(); }

 protected:
  typedef std::chrono::steady_clock TestClock;
  // TODO(stupaq) generalise on next usage
  typedef std::atomic<MsgLoop*> CopilotAtomicPtr;

  const std::chrono::milliseconds positive_timeout;
  const std::chrono::milliseconds negative_timeout;
  Env* const env_;
  const std::shared_ptr<MockPublisherRouter> config_;
  const std::shared_ptr<MockPublisherRouter> shadow_config_;
  std::shared_ptr<rocketspeed::Logger> info_log_;
  MsgLoop::Options msg_loop_options_;

  class ServerMock {
   public:
    // Noncopyable, movable
    ServerMock(ServerMock&&) = default;
    ServerMock& operator=(ServerMock&&) = default;

    ServerMock(std::unique_ptr<MsgLoop> _msg_loop, std::thread msg_loop_thread)
    : msg_loop(std::move(_msg_loop))
    , msg_loop_thread_(std::move(msg_loop_thread)) {}

    ~ServerMock() {
      msg_loop->Stop();
      if (msg_loop_thread_.joinable()) {
        msg_loop_thread_.join();
      }
    }

    std::unique_ptr<MsgLoop> msg_loop;

   private:
    std::thread msg_loop_thread_;
  };

  ServerMock MockServer(std::shared_ptr<MockPublisherRouter> config,
    const std::map<MessageType, MsgCallbackType>& callbacks,
    std::chrono::milliseconds hb_period = std::chrono::milliseconds(100)) {

    msg_loop_options_.event_loop.heartbeat_period = hb_period;

    std::unique_ptr<MsgLoop> server(new MsgLoop(
        env_, EnvOptions(), 0 /* auto */, 1,
        info_log_, "server", msg_loop_options_));
    server->RegisterCallbacks(callbacks);
    EXPECT_OK(server->Initialize());
    std::thread thread([&]() { server->Run(); });
    EXPECT_OK(server->WaitUntilRunning());
    // Set pilot/copilot address in the configuration.
    config->SetCopilot(server->GetHostId());
    config->SetPilot(server->GetHostId());
    return ServerMock(std::move(server), std::move(thread));
  }

  ServerMock MockServer(
    const std::map<MessageType, MsgCallbackType>& callbacks,
    std::chrono::milliseconds hb_period = std::chrono::milliseconds(100)) {
    return MockServer(config_, std::move(callbacks), hb_period);
  }

  ServerMock MockShadowServer(
    const std::map<MessageType, MsgCallbackType>& callbacks,
    std::chrono::milliseconds hb_period = std::chrono::milliseconds(100)) {
    return MockServer(shadow_config_, std::move(callbacks), hb_period);
  }

  void FixOptions(ClientOptions& options,
      std::shared_ptr<MockPublisherRouter> config) {
    if (!options.info_log) {
      options.info_log = info_log_;
    }
    if (!options.publisher) {
      options.publisher = config;
    }
    if (!options.sharding) {
      options.sharding = MakeShardingStrategyFromConfig(config);
    }
  }

  std::unique_ptr<Client> CreateClient(ClientOptions options) {
    FixOptions(options, config_);

    std::unique_ptr<Client> client;
    EXPECT_OK(Client::Create(std::move(options), &client));
    return client;
  }

  std::unique_ptr<Client> CreateShadowedClient(
      ClientOptions client_options,
      ClientOptions shadowed_client_options,
      bool is_internal = false,
      ShouldShadow shadow_predicate =
          [](const SubscriptionParameters& params) { return true; }) {
    FixOptions(client_options, config_);
    FixOptions(shadowed_client_options, shadow_config_);

    std::unique_ptr<Client> client;
    EXPECT_OK(ShadowedClient::Create(std::move(client_options),
                                     std::move(shadowed_client_options),
                                     &client,
                                     is_internal,
                                     shadow_predicate));
    return client;
  }
};

TEST_F(ClientTest, BackOff) {
  const size_t num_attempts = 4;
  std::vector<TestClock::duration> subscribe_attempts;
  port::Semaphore subscribe_sem;
  CopilotAtomicPtr copilot_ptr;
  auto copilot = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          if (subscribe_attempts.size() >= num_attempts) {
            subscribe_sem.Post();
            return;
          }
          subscribe_attempts.push_back(TestClock::now().time_since_epoch());
          // Send back goodbye, so that client will resubscribe.
          MessageGoodbye goodbye(GuestTenant,
                                 MessageGoodbye::Code::Graceful,
                                 MessageGoodbye::OriginType::Server);
          // This is a tad fishy, but Copilot should not receive any message
          // before we perform the assignment to copilot_ptr.
          copilot_ptr.load()->SendResponse(goodbye, origin, 0);
          }}},
      std::chrono::milliseconds(0)); // disable heartbeats
  // disable to prevent heartbeats from notifying the retry mechanism
  // that the connection is healthy
  copilot_ptr = copilot.msg_loop.get();

  // Back-off parameters.
  std::chrono::milliseconds scale(100);

  ClientOptions options;
  options.timer_period = std::chrono::milliseconds(10);
  options.backoff_strategy = [scale](ClientRNG*, size_t retry) {
    return scale * (retry + 1);
  };
  auto client = CreateClient(std::move(options));

  // Subscribe and wait until enough reconnection attempts takes place.
  client->Subscribe(GuestTenant, GuestNamespace, "BackOff", 0);
  std::chrono::milliseconds total(scale * num_attempts * (num_attempts - 1));
  ASSERT_TRUE(subscribe_sem.TimedWait(total));

  // Verify timeouts between consecutive attempts.
  ASSERT_EQ(num_attempts, subscribe_attempts.size());
  std::vector<TestClock::duration> differences(num_attempts);
  std::adjacent_difference(subscribe_attempts.begin(),
                           subscribe_attempts.end(),
                           differences.begin());
  for (size_t i = 1; i < num_attempts; ++i) {
    auto expected = scale * i;
    ASSERT_GE(differences[i], expected - expected / 2);
    ASSERT_LE(differences[i], expected + expected);
  }
}

TEST_F(ClientTest, NotifyShardUnhealthyOnDisconnect) {
  const size_t num_attempts = 1;
  std::atomic<size_t> subscribe_attempts(0);
  port::Semaphore subscribe_sem;
  CopilotAtomicPtr copilot_ptr;

  auto copilot = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          subscribe_attempts++;

          if (subscribe_attempts > num_attempts) {
            subscribe_sem.Post();
            return;
          }
          // Send back goodbye, so that client will resubscribe.
          MessageGoodbye goodbye(GuestTenant,
                                 MessageGoodbye::Code::Graceful,
                                 MessageGoodbye::OriginType::Server);
          // This is a tad fishy, but Copilot should not receive any message
          // before we perform the assignment to copilot_ptr.
          copilot_ptr.load()->SendResponse(goodbye, origin, 0);
        }}});
  copilot_ptr = copilot.msg_loop.get();

  ClientOptions options;
  options.timer_period = std::chrono::milliseconds(1);

  // notify unhealthy as soon as goodbye received
  options.max_silent_reconnects = 0;
  options.backoff_strategy = [](ClientRNG*, size_t) {
    return std::chrono::milliseconds(1);
  };
  auto client = CreateClient(std::move(options));

  class StatusObserver : public Observer {
   public:
    StatusObserver(port::Semaphore& sem) : sem_(sem), ok_(true) {}

    virtual void OnSubscriptionStatusChange(const SubscriptionStatus& status) {
      if (status.GetStatus().IsShardUnhealthy() && ok_) {
        sem_.Post();
        ok_ = false;
      }
      if (status.GetStatus().ok() && !ok_) {
        sem_.Post();
        ok_ = true;
      }
    }
   private:
    port::Semaphore& sem_;
    bool ok_;
  };

  port::Semaphore status_change_sem;

  // Subscribe and wait until enough reconnection attempts takes place.
  client->Subscribe({GuestTenant, GuestNamespace, "BackOff", 0},
                    folly::make_unique<StatusObserver>(status_change_sem));
  ASSERT_TRUE(status_change_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
  ASSERT_EQ(num_attempts + 1, subscribe_attempts);
  // should go back to ok after heartbeat
  ASSERT_TRUE(status_change_sem.TimedWait(positive_timeout));
}

TEST_F(ClientTest, DoNotNotifyShardUnhealthyWhenDisabled) {
  const size_t num_attempts = 4;
  std::atomic<size_t> subscribe_attempts(0);
  port::Semaphore subscribe_sem;
  CopilotAtomicPtr copilot_ptr;

  auto copilot = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          if (subscribe_attempts >= num_attempts) {
            subscribe_sem.Post();
            return;
          }
          subscribe_attempts++;
          // Send back goodbye, so that client will resubscribe.
          MessageGoodbye goodbye(GuestTenant,
                                 MessageGoodbye::Code::Graceful,
                                 MessageGoodbye::OriginType::Server);
          // This is a tad fishy, but Copilot should not receive any message
          // before we perform the assignment to copilot_ptr.
          copilot_ptr.load()->SendResponse(goodbye, origin, 0);
          }}},
      std::chrono::milliseconds(0));
  // disable to prevent heartbeats from notifying the retry mechanism
  // that the connection is healthy
  copilot_ptr = copilot.msg_loop.get();

  ClientOptions options;
  options.should_notify_health = false; // <--
  options.heartbeat_timeout = std::chrono::milliseconds(0);
  options.backoff_strategy = [](ClientRNG*, size_t) {
    return std::chrono::milliseconds(1);
  };
  auto client = CreateClient(std::move(options));

  class StatusObserver : public Observer {
   public:
    StatusObserver(port::Semaphore& sem) : sem_(sem) {}

    virtual void OnSubscriptionStatusChange(const SubscriptionStatus& status) {
      if (status.GetStatus().IsShardUnhealthy()) {
        sem_.Post();
      }
    }
   private:
    port::Semaphore& sem_;
  };

  port::Semaphore status_change_sem;

  // Subscribe and wait until enough reconnection attempts takes place.
  client->Subscribe({GuestTenant, GuestNamespace, "BackOff", 0},
                    folly::make_unique<StatusObserver>(status_change_sem));
  ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
  ASSERT_EQ(num_attempts, subscribe_attempts);
  ASSERT_FALSE(status_change_sem.TimedWait(negative_timeout));
}

TEST_F(ClientTest, NotifyShardUnhealthyOnHBTimeout) {
  port::Semaphore subscribe_sem;
  CopilotAtomicPtr copilot_ptr;

  std::atomic<StreamID> client_stream;
  auto copilot = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
            client_stream = origin;
          }}},
      std::chrono::milliseconds(0)); // disable heartbeats
  copilot_ptr = copilot.msg_loop.get();

  ClientOptions options;
  options.heartbeat_timeout = std::chrono::milliseconds(10);
  auto client = CreateClient(std::move(options));

  class StatusObserver : public Observer {
   public:
    StatusObserver(port::Semaphore& healthy,
                   port::Semaphore& unhealthy)
      : healthy_(healthy), unhealthy_(unhealthy) {}

    virtual void OnSubscriptionStatusChange(const SubscriptionStatus& status) {
      if (status.GetStatus().IsShardUnhealthy()) {
        unhealthy_.Post();
      } else {
        healthy_.Post();
      }
    }
   private:
    port::Semaphore& healthy_;
    port::Semaphore& unhealthy_;
  };

  port::Semaphore now_unhealthy;
  port::Semaphore now_healthy;

  client->Subscribe({GuestTenant, GuestNamespace, "Test", 0},
                    folly::make_unique<StatusObserver>(now_healthy,
                                                       now_unhealthy));

  // we fail due to lack of heartbeat which has been disabled
  ASSERT_TRUE(now_unhealthy.TimedWait(positive_timeout));
}

TEST_F(ClientTest, NotifyNewSubscriptionsUnhealthy) {
  port::Semaphore subscribe_sem;
  CopilotAtomicPtr copilot_ptr;

  std::atomic<StreamID> client_stream;
  auto copilot = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
            client_stream = origin;
          }}},
      std::chrono::milliseconds(0)); // disable heartbeats
  copilot_ptr = copilot.msg_loop.get();

  ClientOptions options;
  options.heartbeat_timeout = std::chrono::milliseconds(10);
  auto client = CreateClient(std::move(options));

  class StatusObserver : public Observer {
   public:
    StatusObserver(port::Semaphore& unhealthy)
      : unhealthy_(unhealthy) {}

    virtual void OnSubscriptionStatusChange(const SubscriptionStatus& status) {
      if (status.GetStatus().IsShardUnhealthy()) {
        unhealthy_.Post();
      }
    }
   private:
    port::Semaphore& unhealthy_;
  };

  port::Semaphore now_unhealthy;

  client->Subscribe({GuestTenant, GuestNamespace, "Test", 0},
                    folly::make_unique<StatusObserver>(now_unhealthy));

  // we fail due to lack of heartbeat which has been disabled
  ASSERT_TRUE(now_unhealthy.TimedWait(positive_timeout));

  port::Semaphore unhealthy;
  client->Subscribe({GuestTenant, GuestNamespace, "Test2", 0},
                    folly::make_unique<StatusObserver>(unhealthy));

  // notify new sub immediately
  ASSERT_TRUE(unhealthy.TimedWait(positive_timeout));
}

TEST_F(ClientTest, HeartbeatsAreSentByServer) {
  // in NotifyShardUnhealthyOnHBTimeout we prove that the client
  // complains when a hb is not received. Here we ensure the server
  // sends them by running for long enough that we would have timed
  // out otherwise.

  StreamID client_stream;
  auto copilot = MockServer(
    {{MessageType::mSubscribe,
          [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          client_stream = origin;
        }}},
    std::chrono::milliseconds(5));

  ClientOptions options;
  options.heartbeat_timeout = std::chrono::milliseconds(20);
  auto client = CreateClient(std::move(options));

  class StatusObserver : public Observer {
   public:
    StatusObserver(port::Semaphore& unhealthy)
      : unhealthy_(unhealthy) {}

    virtual void OnSubscriptionStatusChange(const SubscriptionStatus& status) {
      if (status.GetStatus().IsShardUnhealthy()) {
        unhealthy_.Post();
      }
    }
   private:
    port::Semaphore& unhealthy_;
  };

  port::Semaphore now_unhealthy;

  client->Subscribe({GuestTenant, GuestNamespace, "Test", 0},
                    folly::make_unique<StatusObserver>(now_unhealthy));

  // we should not be marked as unhealthy: server should send hbs
  // every 5 milliseconds
  ASSERT_FALSE(now_unhealthy.TimedWait(negative_timeout));
}

TEST_F(ClientTest, RandomizedTruncatedExponential) {
  std::chrono::seconds value(1), limit(30);
  auto backoff =
      std::bind(backoff::RandomizedTruncatedExponential(value, limit, 2.0, 0.0),
                &ThreadLocalPRNG(),
                std::placeholders::_1);

#define ASSERT_EQD(expected_exp, actual_exp)                               \
  do {                                                                     \
    auto expected = (expected_exp);                                        \
    auto expected_low = expected - expected / 100;                         \
    auto expected_high = expected + expected / 100;                        \
    auto actual =                                                          \
        std::chrono::milliseconds(static_cast<size_t>(actual_exp * 1000)); \
    ASSERT_LE(expected_low, actual);                                       \
    ASSERT_GE(expected_high, actual);                                      \
  } while (0);

  ASSERT_EQD(backoff(0), 1.0);
  ASSERT_EQD(backoff(1), 2.0);
  ASSERT_EQD(backoff(2), 4.0);
  ASSERT_EQD(backoff(3), 8.0);
  ASSERT_EQD(backoff(4), 16.0);
  ASSERT_EQD(backoff(5), 30.0);
  ASSERT_EQD(backoff(6), 30.0);

#undef ASSERT_EQD
}

TEST_F(ClientTest, GetCopilotFailure) {
  port::Semaphore subscribe_sem;
  auto copilot = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          subscribe_sem.Post();
        }}});

  ClientOptions options;
  // Speed up client retries.
  options.timer_period = std::chrono::milliseconds(1);
  options.backoff_strategy = [](ClientRNG*, size_t) {
    return std::chrono::seconds(0);
  };
  auto client = CreateClient(std::move(options));

  // Clear configuration entry for the Copilot.
  config_->SetCopilot(HostId());

  // Subscribe, no call should make it to the Copilot.
  auto sub_handle =
      client->Subscribe(GuestTenant, GuestNamespace, "GetCopilotFailure", 0);
  ASSERT_TRUE(!subscribe_sem.TimedWait(negative_timeout));

  // While disconnected, unsubscribe and subscribe again, this shouldn't affect
  // the scenario.
  client->Unsubscribe(sub_handle);
  client->Subscribe(GuestTenant, GuestNamespace, "GetCopilotFailure", 0);

  // Copilot shouldn't receive anything.
  ASSERT_TRUE(!subscribe_sem.TimedWait(negative_timeout));

  // Set Copilot address in the config.
  config_->SetCopilot(copilot.msg_loop->GetHostId());

  // Copilot should receive the subscribe request.
  ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
}

TEST_F(ClientTest, OfflineOperations) {
  std::unordered_map<Topic, std::pair<SequenceNumber, SubscriptionHandle>>
      subscriptions;
  port::Semaphore unsubscribe_sem, all_ok_sem;
  std::atomic<bool> expects_request(false);
  auto subscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    ASSERT_TRUE(expects_request.load());
    auto subscribe = static_cast<MessageSubscribe*>(msg.get());
    auto it = subscriptions.find(subscribe->GetTopicName().ToString());
    ASSERT_TRUE(it != subscriptions.end());
    ASSERT_EQ(it->second.first, subscribe->GetStartSequenceNumber());
    subscriptions.erase(it);
    if (subscriptions.empty()) {
      all_ok_sem.Post();
    }
  };
  auto copilot = MockServer({{MessageType::mSubscribe, subscribe_cb}});

  ClientOptions options;
  // Speed up client retries.
  options.timer_period = std::chrono::milliseconds(1);
  options.backoff_strategy = [](ClientRNG*, size_t) {
    return std::chrono::seconds(0);
  };
  auto client = CreateClient(std::move(options));

  // Disable communication.
  config_->SetCopilot(HostId());

  auto sub = [&](Topic topic_name, SequenceNumber start_seqno) {
    ASSERT_EQ(0, subscriptions.count(topic_name));
    auto sub_handle = client->Subscribe(
        GuestTenant,
        GuestNamespace,
        topic_name,
        start_seqno,
        nullptr,
        [&](const SubscriptionStatus&) { unsubscribe_sem.Post(); });
    ASSERT_TRUE(sub_handle);
    subscriptions[topic_name] = {start_seqno, sub_handle};
  };

  auto unsub = [&](Topic topic_name) {
    auto it = subscriptions.find(topic_name);
    ASSERT_TRUE(it != subscriptions.end());
    ASSERT_OK(client->Unsubscribe(it->second.second));
    ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
    subscriptions.erase(it);
  };

  // Simulate some subscriptions and unsubscriptions.
  sub("a", 0);
  sub("b", 1);
  unsub("a");
  unsub("b");
  // No subscriptions at this point.
  sub("c", 2);
  sub("a", 3);
  sub("b", 4);
  unsub("a");

  // Finished offline operations.
  expects_request = true;
  // Enable communication and wait for Copilot to verify all received
  // requests.
  config_->SetCopilot(copilot.msg_loop->GetHostId());
  ASSERT_TRUE(all_ok_sem.TimedWait(positive_timeout));
}

TEST_F(ClientTest, CopilotSwap) {
  port::Semaphore subscribe_sem1, subscribe_sem2;
  auto copilot1 = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          subscribe_sem1.Post();
        }}});

  ClientOptions options;
  options.timer_period = std::chrono::milliseconds(1);
  // Make client retries very fast, to make sure that we switch host
  // immediately.
  options.backoff_strategy = [](ClientRNG*, size_t) {
    return std::chrono::seconds(0);
  };
  auto client = CreateClient(std::move(options));

  // Subscribe, call should make it to the copilot.
  client->Subscribe(GuestTenant, GuestNamespace, "CopilotSwap", 0);
  EXPECT_TRUE(subscribe_sem1.TimedWait(positive_timeout));

  // Launch another copilot, this will automatically update configuration.
  auto copilot2 = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          subscribe_sem2.Post();
        }}});
  ASSERT_TRUE(subscribe_sem2.TimedWait(positive_timeout));
}

TEST_F(ClientTest, NoPilot) {
  port::Semaphore publish_sem;
  auto client = CreateClient(ClientOptions());

  // Publish (without pilot), call should invoke callback with error.
  auto ps = client->Publish(GuestTenant,
                            "NoPilot",
                            GuestNamespace,
                            TopicOptions(),
                            "data",
                            [&](std::unique_ptr<ResultStatus> rs) {
                              ASSERT_TRUE(!rs->GetStatus().ok());
                              publish_sem.Post();
                            });
  ASSERT_TRUE(ps.status.ok());
  ASSERT_TRUE(publish_sem.TimedWait(negative_timeout));
}

TEST_F(ClientTest, PublishTimeout) {
  port::Semaphore publish_sem;
  auto pilot = MockServer(
      {{MessageType::mPublish,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          // Do nothing.
        }}});

  ClientOptions opts;
  opts.timer_period = std::chrono::milliseconds(1);
  opts.publish_timeout = negative_timeout / 2;
  auto client = CreateClient(std::move(opts));

  auto ps = client->Publish(GuestTenant,
                            "UselessPilot",
                            GuestNamespace,
                            TopicOptions(),
                            "data",
                            [&](std::unique_ptr<ResultStatus> rs) {
                              ASSERT_TRUE(!rs->GetStatus().ok());
                              ASSERT_TRUE(rs->GetStatus().IsTimedOut());
                              publish_sem.Post();
                            });
  ASSERT_TRUE(ps.status.ok());
  ASSERT_TRUE(publish_sem.TimedWait(negative_timeout));
}

namespace {

class TestSharding2 : public ShardingStrategy {
 public:
  explicit TestSharding2(HostId host0, HostId host1)
  : host0_(host0), host1_(host1) {}

  size_t GetShard(Slice namespace_id, Slice topic_name) const override {
    if (topic_name == "topic0") {
      return 0;
    } else if (topic_name == "topic1") {
      return 1;
    } else {
      EXPECT_TRUE(false);
    }
    return 0;
  }

  size_t GetVersion() override { return 0; }

  HostId GetHost(size_t shard) override {
    EXPECT_LT(shard, 2);
    return shard == 0 ? host0_ : host1_;
  }

  void MarkHostDown(const HostId& host_id) override {}

 private:
  HostId host0_, host1_;
};

}  // namespace

TEST_F(ClientTest, Sharding) {
  port::Semaphore subscribe_sem0, subscribe_sem1;
  // Launch two subscribees for different shards.
  auto copilot0 = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          auto subscribe = static_cast<MessageSubscribe*>(msg.get());
          ASSERT_EQ("topic0", subscribe->GetTopicName());
          subscribe_sem0.Post();
        }}});
  auto copilot1 = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          auto subscribe = static_cast<MessageSubscribe*>(msg.get());
          ASSERT_EQ("topic1", subscribe->GetTopicName());
          subscribe_sem1.Post();
        }}});

  ClientOptions options;
  options.timer_period = std::chrono::milliseconds(1);
  options.thread_selector = [](size_t num_threads, Slice, Slice) -> size_t {
    EXPECT_EQ(num_threads, 1);
    return 0;
  };
  options.sharding.reset(new TestSharding2(copilot0.msg_loop->GetHostId(),
                                           copilot1.msg_loop->GetHostId()));
  auto client = CreateClient(std::move(options));

  SubscriptionParameters params(GuestTenant, GuestNamespace, "", 0);
  // Subscribe on topic0 should get to the owner of the shard 0.
  params.topic_name = "topic0";
  client->Subscribe(params, folly::make_unique<Observer>());
  ASSERT_TRUE(subscribe_sem0.TimedWait(positive_timeout));
  ASSERT_TRUE(!subscribe_sem1.TimedWait(negative_timeout));

  // Subscribe on topic1 should get to the owner of the shard 1.
  params.topic_name = "topic1";
  client->Subscribe(params, folly::make_unique<Observer>());
  ASSERT_TRUE(!subscribe_sem0.TimedWait(negative_timeout));
  ASSERT_TRUE(subscribe_sem1.TimedWait(positive_timeout));
}

TEST_F(ClientTest, ClientSubscriptionLimit) {
  size_t kMaxSubscriptions = 1;

  ClientOptions options;
  options.max_subscriptions = kMaxSubscriptions;
  options.num_workers = 1;
  auto client = CreateClient(std::move(options));

  port::Semaphore unsubscribe_sem;
  port::Semaphore invalid_subscription_sem;

  auto subscribe = [&](Topic topic_name) -> SubscriptionHandle {
    return client->Subscribe(GuestTenant,
                             GuestNamespace,
                             topic_name,
                             1 /* seqno */,
                             nullptr,
                             [&](const SubscriptionStatus& status) {
                               if (status.GetStatus().IsInvalidArgument()) {
                                 invalid_subscription_sem.Post();
                               } else {
                                 unsubscribe_sem.Post();
                               }
                             });
  };

  auto unsubscribe = [&](SubscriptionHandle handle) {
    ASSERT_OK(client->Unsubscribe(handle));
    ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
  };

  // First Subscription (can subscribe)
  Topic topic_name = "1";
  auto sub_handle_1 = subscribe(topic_name);
  ASSERT_TRUE(sub_handle_1);

  // Second Subscription (cannot subscribe)
  topic_name = "2";
  auto sub_handle_2 = subscribe(topic_name);
  ASSERT_TRUE(sub_handle_2);
  ASSERT_TRUE(invalid_subscription_sem.TimedWait(positive_timeout));

  unsubscribe(sub_handle_1);

  // can subscribe now
  topic_name = "2";
  sub_handle_2 = subscribe(topic_name);
  ASSERT_TRUE(sub_handle_2);

  unsubscribe(sub_handle_2);
}

TEST_F(ClientTest, ClientSubscriptionLimitWithTerminateReceivedFromServer) {
  size_t kMaxSubscriptions = 1;

  port::Semaphore server_subscribe_sem;
  port::Semaphore client_unsubscribe_sem;
  port::Semaphore invalid_subscription_sem;
  StreamID stream_id;

  MsgLoop* server_ptr;
  auto server = MockServer({
      {MessageType::mSubscribe,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         stream_id = origin;
         server_subscribe_sem.Post();
       }},
      {MessageType::mUnsubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {}}
  });
  server_ptr = server.msg_loop.get();

  ClientOptions options;
  options.max_subscriptions = kMaxSubscriptions;
  options.num_workers = 1;
  auto client = CreateClient(std::move(options));

  auto subscribe = [&](Topic topic_name) -> SubscriptionHandle {
    return client->Subscribe(GuestTenant,
                             GuestNamespace,
                             topic_name,
                             1 /* seqno */,
                             nullptr,
                             [&](const SubscriptionStatus& status) {
                               if (status.GetStatus().IsInvalidArgument()) {
                                 invalid_subscription_sem.Post();
                               } else {
                                 client_unsubscribe_sem.Post();
                               }
                             });
  };

  auto unsubscribe = [&](SubscriptionHandle handle, bool wait = true) {
    ASSERT_OK(client->Unsubscribe(handle));
    if (wait) {
      ASSERT_TRUE(client_unsubscribe_sem.TimedWait(positive_timeout));
    }
  };

  // First Subscription (can subscribe)
  Topic topic_name = "1";
  auto sub_handle_1 = subscribe(topic_name);
  ASSERT_TRUE(sub_handle_1);
  ASSERT_TRUE(server_subscribe_sem.TimedWait(positive_timeout));

  // Second Subscription (cannot subscribe)
  topic_name = "2";
  auto sub_handle_2 = subscribe(topic_name);
  ASSERT_TRUE(sub_handle_2);
  ASSERT_TRUE(invalid_subscription_sem.TimedWait(positive_timeout));

  // Send an unsubscribe back to the client for the first subscription and wait.
  MessageUnsubscribe unsubscribe_cmd(GuestTenant,
                                     SubscriptionID::Unsafe(sub_handle_1),
                                     MessageUnsubscribe::Reason::kRequested);
  server_ptr->SendResponse(unsubscribe_cmd, stream_id, 0);
  ASSERT_TRUE(client_unsubscribe_sem.TimedWait(positive_timeout));

  // Now the subscription should pass
  sub_handle_2 = subscribe(topic_name);
  ASSERT_TRUE(sub_handle_2);
  ASSERT_TRUE(server_subscribe_sem.TimedWait(positive_timeout));

  // This would have no effect on the counter as this handle has already been
  // unsubscribed.
  unsubscribe(sub_handle_1, false);

  unsubscribe(sub_handle_2);
}

TEST_F(ClientTest, CachedConnectionsWithoutStreams) {
  port::Semaphore subscribe_sem, unsubscribe_sem, global_sem;
  const int kTopics = 100;
  const int kIterations = 2;

  auto copilot1 = MockServer(
      {{MessageType::mSubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
          subscribe_sem.Post();
        }},
       {MessageType::mUnsubscribe,
        [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {}}});

  // Check that connections without streams remain open when the
  // connection_without_streams_keepalive flag is set to a high value (20s)
  // while they close immediately when it is set to zero.
  int64_t prev_accepts = 0;
  for (int iy = 0; iy < kIterations; ++iy) {
    ClientOptions options;
    options.timer_period = std::chrono::milliseconds(1);
    options.connection_without_streams_keepalive =
        std::chrono::milliseconds((iy % 2) ? 0 : 20000);
    options.close_empty_streams = true;
    auto client = CreateClient(std::move(options));
    for (int ix = 0; ix < kTopics; ++ix) {
      auto handle = client->Subscribe(
          GuestTenant,
          GuestNamespace,
          "ConnCache",
          0,
          nullptr,
          [&](const SubscriptionStatus&) { unsubscribe_sem.Post(); });
      ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
      ASSERT_OK(client->Unsubscribe(handle));
      ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
    }
    auto key = copilot1.msg_loop->GetStatsPrefix() + ".accepts";
    auto accepts = copilot1.msg_loop->GetStatisticsSync().GetCounterValue(key);
    if (iy % 2) {
      ASSERT_EQ(accepts - prev_accepts, kTopics);
    } else {
      ASSERT_EQ(accepts - prev_accepts, 1);
    }
    prev_accepts = accepts;
  }

  // Check that connections without streams get garbage collected eventually
  // when the connection_without_streams_keepalive is set.
  ClientOptions options;
  options.timer_period = std::chrono::milliseconds(1);
  options.connection_without_streams_keepalive = std::chrono::milliseconds(1);
  options.close_empty_streams = true;
  auto client = CreateClient(std::move(options));
  auto handle = client->Subscribe(
      GuestTenant,
      GuestNamespace,
      "ConnCache",
      0,
      nullptr,
      [&](const SubscriptionStatus&) { unsubscribe_sem.Post(); });
  ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
  ASSERT_OK(client->Unsubscribe(handle));
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));

  auto server_connections_key =
      copilot1.msg_loop->GetStatsPrefix() + ".all_connections";
  auto server_connections =
      copilot1.msg_loop->GetStatisticsSync().GetCounterValue(
          server_connections_key);
  ASSERT_EQ(server_connections, 1);
  /* sleep override - Wait long enough for the GC to run.
     The GC runs every 100ms, so 120ms is a safe window.
  */
  std::this_thread::sleep_for(std::chrono::milliseconds(120));
  server_connections = copilot1.msg_loop->GetStatisticsSync().GetCounterValue(
      server_connections_key);
  ASSERT_EQ(server_connections, 0);
}

TEST_F(ClientTest, TailCollapsingSubscriber) {
  std::string topic0("TailCollapsingSubscriber0"),
      topic1("TailCollapsingSubscriber1");
  port::Semaphore subscribe_sem, unsubscribe_sem;
  auto copilot = MockServer({
      {MessageType::mSubscribe,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         subscribe_sem.Post();
       }},
      {MessageType::mUnsubscribe,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         unsubscribe_sem.Post();
       }},
      {MessageType::mGoodbye,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         unsubscribe_sem.Post();
       }},
  });

  ClientOptions options;
  options.collapse_subscriptions_to_tail = true;
  auto client = CreateClient(std::move(options));

  auto s0 = client->Subscribe(GuestTenant, GuestNamespace, topic0, 0);
  ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
  auto s1 = client->Subscribe(GuestTenant, GuestNamespace, topic0, 0);
  ASSERT_TRUE(!subscribe_sem.TimedWait(negative_timeout));
  auto s2 = client->Subscribe(GuestTenant, GuestNamespace, topic1, 0);
  ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
  auto s3 = client->Subscribe(GuestTenant, GuestNamespace, topic1, 0);
  ASSERT_TRUE(!subscribe_sem.TimedWait(negative_timeout));

  client->Unsubscribe(s0);
  ASSERT_TRUE(!unsubscribe_sem.TimedWait(negative_timeout));
  client->Unsubscribe(s3);
  ASSERT_TRUE(!unsubscribe_sem.TimedWait(negative_timeout));
  client->Unsubscribe(s2);
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
  client->Unsubscribe(s1);
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));

  auto s4 = client->Subscribe(GuestTenant, GuestNamespace, topic0, 0);
  ASSERT_TRUE(subscribe_sem.TimedWait(positive_timeout));
  client->Unsubscribe(s4);
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
}

// This is exactly the same as single_shard_subscriber's
// MessageReceivedImpl at the time of writing
class MockMessageReceivedImpl : public MessageReceived {
 public:
  explicit MockMessageReceivedImpl(std::unique_ptr<MessageDeliverData> data)
  : data_(std::move(data)) {}

  SubscriptionHandle GetSubscriptionHandle() const override {
    return data_->GetSubID();
  }

  SequenceNumber GetSequenceNumber() const override {
    return data_->GetSequenceNumber();
  }

  Slice GetContents() const override { return data_->GetPayload(); }

 private:
  std::unique_ptr<MessageDeliverData> data_;
};


static void SendMessageDeliver(int& sequence,
                               SubscriptionID sub_id,
                               SubscriberIf* subscriber) {
  // Doesn't really matter what data we initialize this test message with,
  // we just want to make sure the subscription id gets rewritten before
  // delivery
  std::unique_ptr<MessageDeliverData> msg(new MessageDeliverData(
                                            (TenantID)0,
                                            sub_id, MsgId(), Slice("data")));
  msg->SetSequenceNumbers(sequence - 1, sequence);
  ++sequence;
  std::unique_ptr<MessageReceived> received(
    new MockMessageReceivedImpl(std::move(msg)));
  using Info = MockSubscriber::Info;
  Info info;
  subscriber->Select(sub_id, Info::kObserver, &info);
  info.GetObserver()->OnMessageReceived(nullptr, received);
}

static void RunDemultiplexTest(
    std::unique_ptr<TailCollapsingSubscriber>& subscriber,
    SubscriberIf* base_subscriber) {
  // Be careful with these observer pointers, they're actually
  // unique_ptrs that are handed off to internals of the subscriber
  // system.  IE don't copy this pattern outside of unit tests, it's
  // bad.
  static const int observer_count = 3;
  std::vector<MockObserver*> observers;
  std::vector<SubscriptionID> sub_ids;

  int sequence = 2;
  SubscriptionID first_sub_id;
  SubscriptionParameters params;
  params.tenant_id = 0;
  params.namespace_id = "demultiplex";
  params.topic_name = "demultiplex_test";
  params.start_seqno = sequence;

  MockObserver::StaticReset();

  for (int i = 0; i < observer_count; ++i) {
    sub_ids.push_back(SubscriptionID::Unsafe(i + 2));
    if (i == 0) {
      first_sub_id = sub_ids[0];
    }
    observers.push_back(new MockObserver(sub_ids[i]));
    subscriber->StartSubscription(
      sub_ids[i], params, std::unique_ptr<MockObserver>(observers[i]));
    ASSERT_EQ(MockObserver::active_count_, i + 1);
    // Send a message as we add each observer, especially to test the very first
    // observer before multiplexing starts
    SendMessageDeliver(sequence, first_sub_id, base_subscriber);
  }

  // Reset all received counts to 0, they're out of sync due to above sends
  for(int i = 0; i < observer_count; ++i) {
    observers[i]->received_count_ = 0;
  }

  ASSERT_EQ(MockObserver::deleted_count_, 0);

  SendMessageDeliver(sequence, first_sub_id, base_subscriber);
  ASSERT_EQ(observers[0]->received_count_, 1);
  ASSERT_EQ(observers[1]->received_count_, 1);

  subscriber->TerminateSubscription(sub_ids[0]);
  ASSERT_EQ(MockObserver::deleted_count_, 1);

  // Important test: There were 3 observers, now there are two.  They
  // both should receive the message.
  SendMessageDeliver(sequence, first_sub_id, base_subscriber);
  ASSERT_EQ(observers[1]->received_count_, 2);
  ASSERT_EQ(observers[2]->received_count_, 2);

  subscriber->TerminateSubscription(sub_ids[1]);
  ASSERT_EQ(MockObserver::deleted_count_, 2);
  ASSERT_EQ(MockObserver::active_count_, 1);

  // Key test: verify that despite all but one observer having been
  // deleted, the remaining observer receives the message.
  SendMessageDeliver(sequence, first_sub_id, base_subscriber);
  ASSERT_EQ(observers[2]->received_count_, 3);
  subscriber->TerminateSubscription(sub_ids[2]);
  ASSERT_EQ(MockObserver::deleted_count_, 3);
  ASSERT_EQ(MockObserver::active_count_, 0);
}

TEST_F(ClientTest, TailCollapsingSubscriberDemultiplex) {
  // Set up a basic subscriber for the TailCollapsingSubscriber to wrap
  MockSubscriber *base_subscriber = new MockSubscriber;
  std::unique_ptr<TailCollapsingSubscriber> subscriber(
      new TailCollapsingSubscriber(
          std::unique_ptr<SubscriberIf>(base_subscriber)));

  // Run all the tests twice to verify that removing all subscribers
  // and then resubscribing to the same topic works
  RunDemultiplexTest(subscriber, base_subscriber);
  RunDemultiplexTest(subscriber, base_subscriber);
}

TEST_F(ClientTest, TopicToSubscriptionMap) {
  std::unordered_map<SubscriptionID, std::unique_ptr<SubscriptionBase>>
      subscriptions;
  TenantAndNamespaceFactory factory;
  auto add = [&](SubscriptionID sub_id, const Topic& topic_name) {
    subscriptions.emplace(
        sub_id,
        folly::make_unique<SubscriptionBase>(
            factory.GetFlyweight({GuestTenant, GuestNamespace}),
            topic_name,
            sub_id,
            0,
            nullptr));
  };
  auto remove = [&](SubscriptionID sub_id) { subscriptions.erase(sub_id); };
  TopicToSubscriptionMap map(
    [&](SubscriptionID sub_id, NamespaceID* namespace_id, Topic* topic_name) {
      auto it = subscriptions.find(sub_id);
      if (it == subscriptions.end()) {
        return false;
      }
      *namespace_id = it->second->GetNamespace().ToString();
      *topic_name = it->second->GetTopicName().ToString();
      return true;
    });
  auto assert_found = [&](const Topic& topic_name, SubscriptionID sub_id) {
    SubscriptionID found_id = map.Find(GuestNamespace, topic_name);
    ASSERT_EQ(found_id, sub_id);
  };
  auto assert_missing = [&](const Topic& topic_name) {
    SubscriptionID found_id = map.Find(GuestNamespace, topic_name);
    ASSERT_TRUE(!found_id);
  };

  // Test that upsizing and downsizing works.
  std::vector<SubscriptionID> current_set;
  uint64_t next_sub_id_ = 1;
  // Fill up the map.
  for (size_t i = 1; i < 70; ++i) {
    auto sub_id = SubscriptionID::Unsafe(next_sub_id_++);
    auto topic = "TopicToSubscriptionMap" + std::to_string(sub_id);
    add(sub_id, topic);
    map.Insert(GuestNamespace, topic, sub_id);
    current_set.push_back(sub_id);
  }
  // Empty the map.
  std::shuffle(current_set.begin(), current_set.end(), std::mt19937(0x2734169));
  while (!current_set.empty()) {
    {  // Remove the topic and check it's gone.
      auto sub_id = current_set.back();
      current_set.pop_back();
      auto topic = "TopicToSubscriptionMap" + std::to_string(sub_id);
      assert_found(topic, sub_id);
      // Can remove the subscription state before removing an entry from the
      // map.
      remove(sub_id);
      ASSERT_TRUE(map.Remove(GuestNamespace, topic, sub_id));
      assert_missing(topic);
    }
    // All remaining topics should still be there.
    for (auto sub_id : current_set) {
      auto topic = "TopicToSubscriptionMap" + std::to_string(sub_id);
      assert_found(topic, sub_id);
    }
  }
}

TEST_F(ClientTest, ExportStatistics) {
  // Create client, subscribe to a bunch of topics, and sanity check that
  // some statistics exist and have sensible values. Subscribe to 10k topics
  // to get variety in percentiles.
  ClientOptions options;
  auto client = CreateClient(std::move(options));
  for (int i = 0; i < 10000; ++i) {
    client->Subscribe(GuestTenant, GuestNamespace, std::to_string(i), 0);
  }

  // Export stats into maps.
  class TestVisitor : public StatisticsVisitor {
   public:
    void VisitCounter(const std::string& name, int64_t value) override {
      counters[name] = value;
    }

    void VisitHistogram(const std::string& name, double value) override {
      histos[name] = value;
    }

    void Flush() override {
      ++flushed;
    }

    std::unordered_map<std::string, int64_t> counters;
    std::unordered_map<std::string, double> histos;
    size_t flushed = 0;
  };

  TestVisitor visitor;
  client->ExportStatistics(&visitor);

  // At least 1 command should have been processed.
  ASSERT_GT(visitor.counters["rocketspeed.commands_processed"], 0);

  // Command latencies percentiles should be non-zero and increasing.
  auto p50 = visitor.histos["rocketspeed.queues.response_latency.p50"];
  auto p90 = visitor.histos["rocketspeed.queues.response_latency.p90"];
  auto p99 = visitor.histos["rocketspeed.queues.response_latency.p99"];
  auto p999 = visitor.histos["rocketspeed.queues.response_latency.p999"];
  ASSERT_GT(p50, 0.0);
  ASSERT_GT(p90, p50);
  ASSERT_GT(p99, p90);
  ASSERT_GT(p999, p99);
  ASSERT_EQ(visitor.flushed, 1);
}

TEST_F(ClientTest, FailedSubscriptionObserver) {
  // Fail a Subscribe call and ensure observer is preserved.
  // Keep subscribing until at least one fails due to flow control.
  ClientOptions options;
  options.queue_size = 1;
  auto client = CreateClient(std::move(options));
  bool one_failed = false;

  class MyObserver : public Observer {
  };

  for (int i = 0; i < 10000; ++i) {
    // Using unique_ptr<MyObserver> to trigger call to the templated
    // version of Subscribe. This is the most likely way the API is
    // going to be used by apps.
    std::unique_ptr<MyObserver> observer(new MyObserver());
    SubscriptionParameters params(GuestTenant, GuestNamespace, "foo", 0);
    auto handle = client->Subscribe(params, std::move(observer));
    if (handle) {
      ASSERT_TRUE(!observer);
    } else {
      ASSERT_TRUE(!!observer);
      one_failed = true;
      break;
    }
  }
  ASSERT_TRUE(one_failed);
}

class SubscriptionIDTest: public ::testing::Test {};

TEST_F(SubscriptionIDTest, SubscriptionIDHash) {
  // Check we don't use identity function as SubscriptionID hash

  // meaningless value with 1s and 0s throughout whole length
  uint64_t currentId = 0xce0f7906ed89319e;

  for (size_t i = 0; i < 1024; ++i) {
    auto id = SubscriptionID::Unsafe(currentId);
    ASSERT_TRUE(currentId != std::hash<SubscriptionID>()(id));
    currentId += currentId;
  }
}

TEST_F(ClientTest, ShadowedClientSubscribe) {
  port::Semaphore sub_semaphore, shadow_sub_semaphore;
  port::Semaphore unsub_semaphore, shadow_unsub_semaphore;

  // Subscribe callbacks
  auto subscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    sub_semaphore.Post();
  };
  auto shadow_subscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    shadow_sub_semaphore.Post();
  };

  // Unsubscribe callbacks
  auto unsubscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    unsub_semaphore.Post();
  };
  auto shadow_unsubscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    shadow_unsub_semaphore.Post();
  };

  // Mock sever and shadow server
  auto copilot = MockServer({
    {MessageType::mSubscribe, subscribe_cb},
    {MessageType::mGoodbye, unsubscribe_cb},
    {MessageType::mUnsubscribe, unsubscribe_cb}});
  auto copilot2 = MockShadowServer({
    {MessageType::mSubscribe, shadow_subscribe_cb},
    {MessageType::mGoodbye, shadow_unsubscribe_cb},
    {MessageType::mUnsubscribe, shadow_unsubscribe_cb}});

  ClientOptions client_options, shadowed_client_options;

  auto client = CreateShadowedClient(
                  std::move(client_options),
                  std::move(shadowed_client_options));

  // Simulate one subscription
  auto sub_handle = client->Subscribe(
    GuestTenant, /* tenantID */
    GuestNamespace, /* namespaceID */
    "Sample topic name", /* topic name */
    0, /* start sequence number */
    nullptr, /* deliver callback */
    [&](const SubscriptionStatus&) { }); /* subscription callback */

  ASSERT_TRUE(sub_handle);

  // Check Subscribe - Semaphore and shadow semaphore should have value of 1
  ASSERT_TRUE(sub_semaphore.TimedWait(positive_timeout));
  ASSERT_TRUE(shadow_sub_semaphore.TimedWait(positive_timeout));
  ASSERT_FALSE(sub_semaphore.TimedWait(negative_timeout));
  ASSERT_FALSE(shadow_sub_semaphore.TimedWait(negative_timeout));

  // Simulate one unsubscription
  ASSERT_OK(client->Unsubscribe(sub_handle));

  // Check Unsubscribe - Semaphore and shadow semaphore should have value of 1
  ASSERT_TRUE(unsub_semaphore.TimedWait(positive_timeout));
  ASSERT_TRUE(shadow_unsub_semaphore.TimedWait(positive_timeout));
  ASSERT_FALSE(unsub_semaphore.TimedWait(negative_timeout));
  ASSERT_FALSE(shadow_unsub_semaphore.TimedWait(negative_timeout));
}

TEST_F(ClientTest, ShadowClientComparison) {
  constexpr int N = 10; // Number of topics

  std::unordered_map<std::string, port::Semaphore>
    semaphores, shadow_semaphores;

  std::mutex semaphores_mutex, shadow_semaphores_mutex;
  std::set<SubscriptionHandle> handles;
  port::Semaphore unsub_semaphore, shadow_unsub_semaphore;

  // Topics: "a", "aa", "aaa", "aaaa"...
  for (int i = 1; i <= N; i++) {
    semaphores.insert(std::make_pair(std::string(i, 'a'), port::Semaphore()));
    shadow_semaphores.insert(
      std::make_pair(std::string(i, 'a'), port::Semaphore()));
  }

  // Prepare callbacks
  auto subscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    auto subscribe = static_cast<MessageSubscribe*>(msg.get());
    auto topic = subscribe->GetTopicName().ToString();
    std::lock_guard<std::mutex> lock(semaphores_mutex);
    ASSERT_TRUE(semaphores.find(topic) != semaphores.end());
    semaphores[topic].Post();
  };
  auto shadow_subscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    auto subscribe = static_cast<MessageSubscribe*>(msg.get());
    auto topic = subscribe->GetTopicName().ToString();
    std::lock_guard<std::mutex> lock(shadow_semaphores_mutex);
    ASSERT_TRUE(shadow_semaphores.find(topic) != shadow_semaphores.end());
    shadow_semaphores[topic].Post();
  };
  auto unsubscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    unsub_semaphore.Post();
  };
  auto shadow_unsubscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    shadow_unsub_semaphore.Post();
  };

  // Mock sever and shadow server
  auto copilot = MockServer(
    {{MessageType::mSubscribe, subscribe_cb},
    {MessageType::mUnsubscribe, unsubscribe_cb},
    {MessageType::mGoodbye, unsubscribe_cb}});
  auto copilot2 = MockShadowServer(
    {{MessageType::mSubscribe, shadow_subscribe_cb},
    {MessageType::mUnsubscribe, shadow_unsubscribe_cb},
    {MessageType::mGoodbye, shadow_unsubscribe_cb}});

  // Create ShadowedClient
  ClientOptions client_options, shadowed_client_options;

  auto client = CreateShadowedClient(
                  std::move(client_options),
                  std::move(shadowed_client_options));

  // Simulate subscriptions
  for (int i = 1; i <= N; i++) {
    auto sub_handle = client->Subscribe(
      GuestTenant, /* tenantID */
      GuestNamespace, /* namespaceID */
      std::string(i, 'a'), /* topic name */
      i, /* start sequence number */
      nullptr, /* deliver callback */
      [&](const SubscriptionStatus&) {}); /* subscription callback */
    ASSERT_TRUE(sub_handle);
    handles.insert(sub_handle);
  }

  // Check subcribe method results
  for (int i = 1; i <= N; i++) {
    ASSERT_TRUE(semaphores[std::string(i, 'a')].TimedWait(positive_timeout));
    ASSERT_FALSE(semaphores[std::string(i, 'a')].TimedWait(negative_timeout));
    ASSERT_TRUE(
      shadow_semaphores[std::string(i, 'a')].TimedWait(positive_timeout));
    ASSERT_FALSE(
      shadow_semaphores[std::string(i, 'a')].TimedWait(negative_timeout));
  }

  // Number of topic should be equal number of handles
  ASSERT_EQ(N, handles.size());

  // Simulate unsubscriptions and check results
  for (auto handle : handles) {
    ASSERT_OK(client->Unsubscribe(handle));
    ASSERT_TRUE(unsub_semaphore.TimedWait(positive_timeout));
    ASSERT_TRUE(shadow_unsub_semaphore.TimedWait(positive_timeout));
  }

  ASSERT_FALSE(unsub_semaphore.TimedWait(negative_timeout));
  ASSERT_FALSE(shadow_unsub_semaphore.TimedWait(negative_timeout));

}

TEST_F(ClientTest, ShadowedClientPredicate) {
  port::Semaphore sub_semaphore, shadow_sub_semaphore;
  port::Semaphore unsub_semaphore, shadow_unsub_semaphore;

  // Subscribe callbacks
  auto subscribe_cb = [&](Flow* flow,
                          std::unique_ptr<Message> msg,
                          StreamID origin) { sub_semaphore.Post(); };
  auto shadow_subscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    shadow_sub_semaphore.Post();
  };

  // Unsubscribe callbacks
  auto unsubscribe_cb = [&](Flow* flow,
                            std::unique_ptr<Message> msg,
                            StreamID origin) { unsub_semaphore.Post(); };
  auto shadow_unsubscribe_cb = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    shadow_unsub_semaphore.Post();
  };

  // Mock sever and shadow server
  auto copilot = MockServer({{MessageType::mSubscribe, subscribe_cb},
                             {MessageType::mGoodbye, unsubscribe_cb},
                             {MessageType::mUnsubscribe, unsubscribe_cb}});
  auto copilot2 =
      MockShadowServer({{MessageType::mSubscribe, shadow_subscribe_cb},
                        {MessageType::mGoodbye, shadow_unsubscribe_cb},
                        {MessageType::mUnsubscribe, shadow_unsubscribe_cb}});

  ClientOptions client_options, shadowed_client_options;

  auto shadow_predicate = [](const SubscriptionParameters& params) {
    return !params.topic_name.compare("Correct topic");
  };

  auto client = CreateShadowedClient(std::move(client_options),
                                     std::move(shadowed_client_options),
                                     false,
                                     shadow_predicate);

  // Simulate one subscription with correct topic
  auto sub_handle = client->Subscribe(
      GuestTenant,                        /* tenantID */
      GuestNamespace,                     /* namespaceID */
      "Correct topic",                    /* topic name */
      0,                                  /* start sequence number */
      nullptr,                            /* deliver callback */
      [&](const SubscriptionStatus&) {}); /* subscription callback */

  ASSERT_TRUE(sub_handle);

  // Check Subscribe - Semaphore and shadow semaphore should have value of 1
  ASSERT_TRUE(sub_semaphore.TimedWait(positive_timeout));
  ASSERT_TRUE(shadow_sub_semaphore.TimedWait(positive_timeout));
  ASSERT_FALSE(sub_semaphore.TimedWait(negative_timeout));
  ASSERT_FALSE(shadow_sub_semaphore.TimedWait(negative_timeout));

  // Simulate one unsubscription
  ASSERT_OK(client->Unsubscribe(sub_handle));

  // Check Unsubscribe - Semaphore and shadow semaphore should have value of 1
  ASSERT_TRUE(unsub_semaphore.TimedWait(positive_timeout));
  ASSERT_TRUE(shadow_unsub_semaphore.TimedWait(positive_timeout));
  ASSERT_FALSE(unsub_semaphore.TimedWait(negative_timeout));
  ASSERT_FALSE(shadow_unsub_semaphore.TimedWait(negative_timeout));

  // Simulate one subscription with incorrect topic
  sub_handle = client->Subscribe(
      GuestTenant,                        /* tenantID */
      GuestNamespace,                     /* namespaceID */
      "Incorrect topic",                  /* topic name */
      0,                                  /* start sequence number */
      nullptr,                            /* deliver callback */
      [&](const SubscriptionStatus&) {}); /* subscription callback */

  ASSERT_TRUE(sub_handle);

  // Check Subscribe - Semaphore value of 1, shadow semaphore value of 0
  ASSERT_TRUE(sub_semaphore.TimedWait(positive_timeout));
  ASSERT_FALSE(sub_semaphore.TimedWait(negative_timeout));
  ASSERT_FALSE(shadow_sub_semaphore.TimedWait(negative_timeout));

  // Simulate one unsubscription
  ASSERT_OK(client->Unsubscribe(sub_handle));

  // Check Unsubscribe - Semaphore value of 1, shadow semaphore value of 0
  ASSERT_TRUE(unsub_semaphore.TimedWait(positive_timeout));
  ASSERT_FALSE(unsub_semaphore.TimedWait(negative_timeout));
  ASSERT_FALSE(shadow_unsub_semaphore.TimedWait(negative_timeout));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
