#include "src/client/single_shard_subscriber.h"
#include "src/client/subscriber_stats.h"
#include "src/messages/flow_control.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

#include <stack>

namespace rocketspeed {

namespace {

class MockShardingStrategy : public ShardingStrategy {
 public:
  explicit MockShardingStrategy() {}

  size_t GetShard(Slice, Slice, const IntroParameters&) const override {
    return 0;
  }

  size_t GetVersion() override { return 0; }

  HostId GetHost(size_t) override { return HostId(); }

  void MarkHostDown(const HostId& host_id) override {}
};

class BlackholeSink : public Sink<std::unique_ptr<Message>> {
 public:
  bool Write(std::unique_ptr<Message>& value) override { return true; };

  bool FlushPending() override { return true; }

  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
    class NullEventCallback : public EventCallback {
      void Enable() override {}
      void Disable() override {}
    };
    return std::make_unique<NullEventCallback>();
  };
};

class StackSink : public Sink<std::unique_ptr<Message>> {
 public:
  std::stack<std::unique_ptr<Message>> stack;

  bool Write(std::unique_ptr<Message>& value) override {
    stack.push(std::move(value));
    return true;
  };

  bool FlushPending() override { return true; }

  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
    class NullEventCallback : public EventCallback {
      void Enable() override {}
      void Disable() override {}
    };
    return std::make_unique<NullEventCallback>();
  };
};

}

TEST(Subscriber, IgnoresDeliveriesThatAreNotSubcribed) {
  class TestHooks : public SubscriberHooks {
   public:
    virtual void SubscriptionExists(const HookedSubscriptionStatus&) override {}
    virtual void OnStartSubscription() override {}
    virtual void OnAcknowledge(SequenceNumber seqno) override {}
    virtual void OnTerminateSubscription() override {}
    virtual void OnReceiveTerminate() override{};
    virtual void OnMessageReceived(const MessageReceived*) override {
      called_ = true;
    }
    virtual void OnSubscriptionStatusChange(
        const HookedSubscriptionStatus&) override {}
    virtual void OnDataLoss(const DataLossInfo&) override {}
    bool Called() const { return called_; }

   private:
    bool called_ = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    HooksParameters hook_params(7, "namespace", "topic");
    auto hooks = std::make_shared<TestHooks>();
    subscriber.InstallHooks(hook_params, hooks);
    loop.Initialize();

    MessageDeliverData* msg =
        new MessageDeliverData(7,
                               "namespace",
                               "topic",
                               SubscriptionID::ForShard(4992, 1),
                               GUID(),
                               "payload");
    StreamReceiveArg<Message> arg;
    SourcelessFlow flow(loop.GetFlowControl());
    arg.flow = &flow;
    arg.stream_id = 11111;
    arg.message = std::unique_ptr<Message>(msg);
    subscriber(std::move(arg));
    EXPECT_FALSE(hooks->Called());
  }
  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, DeliverToAckdSubscriptions) {
  class TestHooks : public SubscriberHooks {
   public:
    virtual void SubscriptionExists(const HookedSubscriptionStatus&) override {}
    virtual void OnStartSubscription() override {}
    virtual void OnAcknowledge(SequenceNumber seqno) override {}
    virtual void OnTerminateSubscription() override {}
    virtual void OnReceiveTerminate() override{};
    virtual void OnMessageReceived(const MessageReceived*) override {
      called_ = true;
    }
    virtual void OnSubscriptionStatusChange(
        const HookedSubscriptionStatus&) override {}
    virtual void OnDataLoss(const DataLossInfo&) override {}
    bool Called() const { return called_; }

   private:
    bool called_ = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    HooksParameters hook_params(7, "namespace", "topic");
    auto hooks = std::make_shared<TestHooks>();
    subscriber.InstallHooks(hook_params, hooks);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("foo", 0)}),
        std::make_unique<Observer>());

    loop.RunOnce();  // transfer pending to synced

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("foo", 0)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    EXPECT_TRUE(hooks->Called());  // could have used the observer
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterDeliverNoAck) {
  // sub 2, deliver 3, disconnect, connect, resub 2
  class TestHooks : public SubscriberHooks {
   public:
    virtual void SubscriptionExists(const HookedSubscriptionStatus&) override {}
    virtual void OnStartSubscription() override {}
    virtual void OnAcknowledge(SequenceNumber seqno) override {}
    virtual void OnTerminateSubscription() override {}
    virtual void OnReceiveTerminate() override{};
    virtual void OnMessageReceived(const MessageReceived* mr) override {
      called_ = true;
    }
    virtual void OnSubscriptionStatusChange(
        const HookedSubscriptionStatus&) override {}
    virtual void OnDataLoss(const DataLossInfo&) override {}
    bool Called() const { return called_; }

   private:
    bool called_ = false;
  };

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 2}};
        RS_ASSERT(cv == expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };


  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    HooksParameters hook_params(7, "namespace", "topic");
    auto hooks = std::make_shared<TestHooks>();
    subscriber.InstallHooks(hook_params, hooks);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();  // transfer pending to synced

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 3);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    EXPECT_FALSE(hooks->Called());

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();  // transfer pending to synced
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterDeliverAck) {
  // sub 2, deliver 3, ack 2, disconnect, connect, resub 2
  class TestHooks : public SubscriberHooks {
   public:
    virtual void SubscriptionExists(const HookedSubscriptionStatus&) override {}
    virtual void OnStartSubscription() override {}
    virtual void OnAcknowledge(SequenceNumber seqno) override {}
    virtual void OnTerminateSubscription() override {}
    virtual void OnReceiveTerminate() override{};
    virtual void OnMessageReceived(const MessageReceived* mr) override {
      called_ = true;
    }
    virtual void OnSubscriptionStatusChange(
        const HookedSubscriptionStatus&) override {}
    virtual void OnDataLoss(const DataLossInfo&) override {}
    bool Called() const { return called_; }

   private:
    bool called_ = false;
  };

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 2}};
        RS_ASSERT(cv == expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };


  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    HooksParameters hook_params(7, "namespace", "topic");
    auto hooks = std::make_shared<TestHooks>();
    subscriber.InstallHooks(hook_params, hooks);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();  // transfer pending to synced

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 3);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    EXPECT_FALSE(hooks->Called());

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();  // transfer pending to synced
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterDeliverAckDeliver) {
  // sub 2, deliver 7, ack 2, deliver 4, disconnect, connect, resub 5
  class TestHooks : public SubscriberHooks {
   public:
    virtual void SubscriptionExists(const HookedSubscriptionStatus&) override {}
    virtual void OnStartSubscription() override {}
    virtual void OnAcknowledge(SequenceNumber seqno) override {}
    virtual void OnTerminateSubscription() override {}
    virtual void OnReceiveTerminate() override{};
    virtual void OnMessageReceived(const MessageReceived* mr) override {
      ASSERT_EQ(mr->GetContents().ToString(), "payload");
      ASSERT_EQ(mr->GetDataSource().ToString(), "a");
      ASSERT_EQ(mr->GetSequenceNumber(), 4);
      called_ = true;
    }
    virtual void OnSubscriptionStatusChange(
        const HookedSubscriptionStatus&) override {}
    virtual void OnDataLoss(const DataLossInfo&) override {}
    bool Called() const { return called_; }

   private:
    bool called_ = false;
  };

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 5}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    HooksParameters hook_params(7, "namespace", "topic");
    auto hooks = std::make_shared<TestHooks>();
    subscriber.InstallHooks(hook_params, hooks);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();  // transfer pending to synced

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 7);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 4);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    EXPECT_TRUE(hooks->Called());

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();  // transfer pending to synced
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubSub) {
// sub2,unsub2,sub2|resub2

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 2}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();  // transfer pending to synced
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubAckUnsub) {
// sub2,ack2,unsub2|noresub

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      all_good = false;         // we expect nothing to be written
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = true;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubAck) {
// sub2,unsub2,ack2|noresub

  class NoResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      all_good = false;         // we expect nothing to be written
      return true;
    };

    ~NoResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = true;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<NoResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubSubAckDeliverAck) {
// sub2,unsub2,sub2,ack2,d5,ack2|resub5+1

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 6}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }
    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 5);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }
    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubAckUnsubSubDeliver) {
// sub2,ack2,unsub2,sub2,d5|resub2

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 2}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 5);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubAckSubDeliver) {
// sub2,unsub2,ack2,sub2,d5|resub2

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 2}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 5);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubAckSubDeliverAck) {
// sub2,unsub2,ack2,sub2,d5,ack2|resub2

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 2}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 5);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubSubAckDeliver) {
// sub2,unsub2,sub2,ack2,d5|resub6

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 6}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 5);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubSubdiffAckold) {
// sub2,unsub,sub7,ack2|resub7

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 7}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 7)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubSubdiffAckoldDeliver) {
// sub2,unsub,sub7,ack2,d5|resub7

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 7}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 7)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 5);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }


    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}

TEST(Subscriber, ReconnectAfterSubUnsubSubdiffAckoldDeliverAcknew) {
// sub2,unsub,sub7,ack2,d5,ack7|resub7

  class ResubCheckSink : public Sink<std::unique_ptr<Message>> {
   public:
    bool Write(std::unique_ptr<Message>& value) override {
      if (value->GetMessageType() == MessageType::mSubscribe) {
        auto msg = static_cast<MessageSubscribe*>(value.get());
        auto cv = msg->GetStart();
        CursorVector expected = {{"a", 7}};
        EXPECT_EQ(cv, expected);
        all_good = true;
      }
      return true;
    };

    ~ResubCheckSink() {
      RS_ASSERT(all_good);
    }

    bool FlushPending() override { return true; }

    std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
      class NullEventCallback : public EventCallback {
        void Enable() override {}
        void Disable() override {}
      };
      return std::make_unique<NullEventCallback>();
    };
   private:
    bool all_good = false;
  };

  ClientOptions opts;
  opts.sharding = std::make_shared<MockShardingStrategy>();
  auto stats = std::make_shared<SubscriberStats>("prefix");
  auto num_subs = std::make_shared<size_t>(0);
  auto intro_params = std::make_shared<const IntroParameters>();
  EventLoop::Options options;
  StreamAllocator stream_allocator;
  EventLoop loop(options, std::move(stream_allocator));
  {
    Subscriber subscriber(opts, &loop, stats, 4992, 30, num_subs, intro_params);
    loop.Initialize();
    subscriber.ConnectionCreated(std::make_unique<BlackholeSink>());

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 2)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    subscriber.TerminateSubscription(
      "namespace", "topic",
      SubscriptionID::ForShard(4992, 1));

    loop.RunOnce();

    subscriber.StartSubscription(
        SubscriptionID::ForShard(4992, 1),
        SubscriptionParameters(7, "namespace", "topic", {Cursor("a", 7)}),
        std::make_unique<Observer>());

    loop.RunOnce();

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 2)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }

    {
      MessageDeliverData* msg =
          new MessageDeliverData(7,
                                 "namespace",
                                 "topic",
                                 SubscriptionID::ForShard(4992, 1),
                                 GUID(),
                                 "payload");
      msg->SetSequenceNumbers("a", 1, 5);
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // should deliver the message
    }

    {
      MessageSubAck* msg = new MessageSubAck(7,
                                             "namespace",
                                             "topic",
                                             {Cursor("a", 7)});
      StreamReceiveArg<Message> arg;
      SourcelessFlow flow(loop.GetFlowControl());
      arg.flow = &flow;
      arg.stream_id = 11111;
      arg.message = std::unique_ptr<Message>(msg);
      subscriber(std::move(arg));  // sub now ack'd
    }


    subscriber.ConnectionDropped();
    subscriber.ConnectionCreated(std::make_unique<ResubCheckSink>());
    loop.RunOnce();
  }

  loop.Stop();  // idiom for gracefully terminating
  loop.Run();
}
}

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
