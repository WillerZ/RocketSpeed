// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include <chrono>
#include <memory>
#include <vector>

#include "include/RocketSpeed.h"
#include "src/client/client.h"
#include "src/controltower/log_tailer.h"
#include "src/port/port.h"
#include "src/util/common/guid_generator.h"
#include "src/util/common/thread_check.h"
#include "src/util/control_tower_router.h"
#include "src/util/testharness.h"
#include "src/test/test_cluster.h"

namespace rocketspeed {

class IntegrationTest : public ::testing::Test {
 public:
  std::chrono::seconds timeout;

  IntegrationTest() : timeout(60), env_(Env::Default()) {
    EXPECT_OK(test::CreateLogger(env_, "IntegrationTest", &info_log));
  }

  int64_t GetNumOpenLogs(ControlTower* ct) const {
    return ct->GetStatisticsSync().GetCounterValue(
        "tower.log_tailer.open_logs");
  }

  // Helper class to avoid code duplication between some of the tests that
  // rely on all the Subscription's callbacks being called.
  // Publishes N messages and trims the first K from a sender client.
  class SubscriptionCallbacksTestContext;

 protected:
  Env* env_;
  std::shared_ptr<rocketspeed::Logger> info_log;
};

TEST_F(IntegrationTest, OneMessage) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message read semaphore.
  port::Semaphore msg_received;

  // Message setup.
  Topic topic = "OneMessage";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // RocketSpeed callbacks;
  auto publish_callback = [&](std::unique_ptr<ResultStatus> rs) {};

  auto subscription_callback = [&](const SubscriptionStatus& ss) {
    ASSERT_TRUE(ss.GetTopicName() == topic);
    ASSERT_TRUE(ss.GetNamespace() == namespace_id);
  };

  auto receive_callback = [&](std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Send a message.
  auto ps = client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            publish_callback,
                            message_id);
  ASSERT_TRUE(ps.status.ok());
  ASSERT_TRUE(ps.msgid == message_id);

  // Listen for the message.
  ASSERT_TRUE(client->Subscribe(GuestTenant,
                                namespace_id,
                                topic,
                                1,
                                receive_callback,
                                subscription_callback));

  // Wait for the message.
  bool result = msg_received.TimedWait(timeout);
  ASSERT_TRUE(result);
}

/**
 * Publishes 1 message. Trims message. Attempts to read
 * message and ensures that one gap is received.
 */
TEST_F(IntegrationTest, TrimAll) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message read semaphore.
  port::Semaphore publish_sem;
  port::Semaphore read_sem;

  // Message setup.
  Topic topic = "TrimAll";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // Callbacks.
  auto publish_callback = [&](std::unique_ptr<ResultStatus> rs) {
    // Number of gaps received from reader. Should be 1.
    int num_gaps = 0;

    // Trim setup.
    auto router = cluster.GetLogRouter();
    LogID log_id;
    auto st = router->GetLogID(Slice(namespace_id), Slice(topic), &log_id);
    ASSERT_OK(st);

    // Trim whole file.
    auto seqno = rs->GetSequenceNumber();
    auto storage = cluster.GetLogStorage();
    st = storage->Trim(log_id, seqno);
    ASSERT_OK(st);

    // Reader callbacks
    auto record_cb = [&](LogRecord& r) {
      // We should not be reading any records as they have been trimmed.
      EXPECT_TRUE(false);
      return true;
    };

    auto gap_cb = [&](const GapRecord& r) {
      // We expect a Gap, and we expect the low seqno should be our
      // previously published publish lsn.
      EXPECT_EQ(r.from, seqno);
      num_gaps++;
      read_sem.Post();
      return true;
    };

    // Create readers.
    std::vector<AsyncLogReader*> readers;
    st = storage->CreateAsyncReaders(1, record_cb, gap_cb, &readers);
    ASSERT_OK(st);

    // Attempt to read trimmed message.
    auto reader = readers.front();
    st = reader->Open(log_id, seqno, seqno);
    ASSERT_OK(st);

    // Wait on read.
    ASSERT_TRUE(read_sem.TimedWait(timeout));
    ASSERT_TRUE(num_gaps == 1);

    // Delete readers
    for (auto r : readers) {
      delete r;
    }
    publish_sem.Post();
  };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Send a message.
  auto ps = client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            publish_callback,
                            message_id);
  ASSERT_OK(ps.status);
  ASSERT_TRUE(ps.msgid == message_id);

  // Wait on publish
  ASSERT_TRUE(publish_sem.TimedWait(timeout));
}

class IntegrationTest::SubscriptionCallbacksTestContext {
 public:
  static const size_t kTotalMessagesToSend = 10;
  static const size_t kTotalMessagesToTrim = 3;

  Topic topic = "SubscriptionCallbacksTestContext_Topic";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "SubscriptionCallbacksTestContext_Message";
  GUIDGenerator msgid_generator;

  SequenceNumber first_seqno = -1;

  LocalTestCluster cluster;
  std::unique_ptr<ClientImpl> sender;
  std::unique_ptr<ClientImpl> receiver;

  port::Semaphore publish_sem;

  explicit SubscriptionCallbacksTestContext(IntegrationTest* parent)
  : cluster(parent->info_log) {
    EXPECT_OK(cluster.GetStatus());

    // PART 1 - Create a sender client, send N messages and trim the first K.

    // Create RocketSpeed sender client.
    cluster.CreateClient(&sender, true);

    size_t call_count = 0;
    auto publish_callback = [&](std::unique_ptr<ResultStatus> rs) {

      // Do the trimming magic only when all records have been published.
      if (++call_count < kTotalMessagesToSend) {
        return;
      }

      // Trim setup.
      auto router = cluster.GetLogRouter();
      LogID log_id;
      auto st = router->GetLogID(Slice(namespace_id), Slice(topic), &log_id);
      ASSERT_OK(st);

      // Write down the first sequence number for the receiver.
      first_seqno = rs->GetSequenceNumber() - kTotalMessagesToSend + 1;

      for (size_t i = 1; i <= kTotalMessagesToTrim; ++i) {
        // Trim message sent in the i'th place.
        auto seqno = rs->GetSequenceNumber() - kTotalMessagesToSend + i;
        auto storage = cluster.GetLogStorage();
        st = storage->Trim(log_id, seqno);
        ASSERT_OK(st);
      }

      publish_sem.Post();
    };

    // Send all messages.
    for (size_t i = 0; i < kTotalMessagesToSend; ++i) {
      MsgId message_id = msgid_generator.Generate();
      auto ps = sender->Publish(GuestTenant,
                                topic,
                                namespace_id,
                                topic_options,
                                Slice(data),
                                publish_callback,
                                message_id);
      EXPECT_OK(ps.status);
      EXPECT_TRUE(ps.msgid == message_id);
    }

    // Wait on the last message to be published.
    EXPECT_TRUE(publish_sem.TimedWait(parent->timeout));

    // PART 2 - Create a receiver client, sync to the first seqno and
    // confirm received and lost messages.

    // Create RocketSpeed receiver client.
    cluster.CreateClient(&receiver, true);
  }
};

/**
 * Publishes N messages and trims the first K from a sender client.
 * After this a receiver client validates DataLoss callback is correctly called.
 */
TEST_F(IntegrationTest, TestDataLossCallback) {
  SubscriptionCallbacksTestContext ctx(this);

  // Receiver flow control semaphores.
  port::Semaphore receive_sem, data_loss_sem;

  auto receive_callback = [&](std::unique_ptr<MessageReceived>& mr) {
    static size_t received_messages = 0;
    if (++received_messages ==
        ctx.kTotalMessagesToSend - ctx.kTotalMessagesToTrim) {
      receive_sem.Post();
    }
  };

  auto data_loss_callback = [&](const DataLossInfo& msg) {
    ASSERT_EQ(DataLossType::kRetention, msg.GetLossType());
    ASSERT_EQ(ctx.first_seqno, msg.GetFirstSequenceNumber());
    ASSERT_EQ(ctx.first_seqno + ctx.kTotalMessagesToTrim - 1,
              msg.GetLastSequenceNumber());
    data_loss_sem.Post();
  };

  SubscriptionParameters sub_params = SubscriptionParameters(
      GuestTenant, ctx.namespace_id, ctx.topic, ctx.first_seqno);
  auto rec_handle = ctx.receiver->Subscribe(
      sub_params, receive_callback, nullptr, data_loss_callback);
  ASSERT_TRUE(rec_handle);
  ASSERT_TRUE(receive_sem.TimedWait(timeout));
  ASSERT_TRUE(data_loss_sem.TimedWait(timeout));
}

/**
 * Variation of TestDataLossCallback tests that uses
 * Observer interface instead of std::functions and checks that
 * all the callbacks have been called.
 */
TEST_F(IntegrationTest, ObserverInterfaceUsage) {
  SubscriptionCallbacksTestContext ctx(this);

  // Semaphores for callbacks to fire.
  port::Semaphore msg_received, substat_received, dataloss_received;

  class TestObserver : public Observer {
   public:
    void OnMessageReceived(Flow* flow,
                           std::unique_ptr<MessageReceived>& mr) override {
      ASSERT_TRUE(mr->GetContents().ToString() == ctx_.data);
      msg_received_.Post();
    }

    void OnSubscriptionStatusChange(const SubscriptionStatus& ss) override {
      ASSERT_TRUE(ss.GetTopicName() == ctx_.topic);
      ASSERT_TRUE(ss.GetNamespace() == ctx_.namespace_id);
      substat_received_.Post();
    }

    void OnDataLoss(Flow* flow, const DataLossInfo& msg) override {
      ASSERT_EQ(DataLossType::kRetention, msg.GetLossType());
      ASSERT_EQ(ctx_.first_seqno, msg.GetFirstSequenceNumber());
      ASSERT_EQ(ctx_.first_seqno + ctx_.kTotalMessagesToTrim - 1,
                msg.GetLastSequenceNumber());
      dataloss_received_.Post();
    }

    TestObserver(SubscriptionCallbacksTestContext& _ctx,
                 port::Semaphore& _msg_received,
                 port::Semaphore& _substat_received,
                 port::Semaphore& _dataloss_received)
    : ctx_(_ctx)
    , msg_received_(_msg_received)
    , substat_received_(_substat_received)
    , dataloss_received_(_dataloss_received) {}

   private:
    SubscriptionCallbacksTestContext& ctx_;
    port::Semaphore& msg_received_;
    port::Semaphore& substat_received_;
    port::Semaphore& dataloss_received_;
  };

  SubscriptionParameters sub_params = SubscriptionParameters(
      GuestTenant, ctx.namespace_id, ctx.topic, ctx.first_seqno);

  // Listen for the message.
  auto sub_handle = ctx.receiver->Subscribe(
      sub_params,
      std::make_unique<TestObserver>(
          ctx, msg_received, substat_received, dataloss_received));
  ASSERT_TRUE(sub_handle != 0);

  ASSERT_TRUE(msg_received.TimedWait(timeout));
  ASSERT_TRUE(dataloss_received.TimedWait(timeout));
  ASSERT_TRUE(!substat_received.TimedWait(std::chrono::seconds(0)));

  // Unsubscribe
  ASSERT_OK(ctx.receiver->Unsubscribe(sub_handle));

  ASSERT_TRUE(substat_received.TimedWait(timeout));
}

/**
 * Publishes n messages. Trims the first. Attempts to read
 * range [0, n-1] and ensures that one gap is received and
 * n-1 records are read.
 */
TEST_F(IntegrationTest, TrimFirst) {
  int num_publish = 3;

  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message read semaphore.
  port::Semaphore publish_sem;
  port::Semaphore read_sem;

  // Message setup.
  Topic topic = "TrimFirst";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;

  // Callbacks.
  // Basic callback simply notifies that publish was received.
  auto norm_pub_cb =
      [&](std::unique_ptr<ResultStatus> rs) { publish_sem.Post(); };

  // Last callback trims first log and attempts to read from deleted
  // log to final log published. Gap should be received as well as
  // n-1 messages.
  auto last_pub_cb = [&](std::unique_ptr<ResultStatus> rs) {
    // Number of gaps and logs received from reader. Should be
    // 1 and n - 1 respectively.
    int num_gaps = 0;
    int num_logs = 0;

    // Trim setup.
    auto router = cluster.GetLogRouter();
    LogID log_id;
    auto st = router->GetLogID(Slice(namespace_id), Slice(topic), &log_id);
    ASSERT_OK(st);

    // Trim first log.
    auto seqno = rs->GetSequenceNumber();
    auto first_seqno = seqno - num_publish + 1;
    auto storage = cluster.GetLogStorage();
    // Trim only the first log.
    // This is assuming logids will increment by one with every publish.
    st = storage->Trim(log_id, first_seqno);
    ASSERT_OK(st);

    // Reader callbacks.
    auto record_cb = [&](LogRecord& r) {
      // We should not be reading any records as they have been trimmed.
      num_logs++;
      read_sem.Post();
      return true;
    };

    auto gap_cb = [&](const GapRecord& r) {
      // We expect a Gap, and we expect the low seqno should be our
      // previously published publish lsn.
      num_gaps++;
      read_sem.Post();
      return true;
    };

    // Create readers.
    std::vector<AsyncLogReader*> readers;
    st = storage->CreateAsyncReaders(1, record_cb, gap_cb, &readers);
    ASSERT_OK(st);

    // Attempt to read trimmed message.
    auto reader = readers.front();
    st = reader->Open(log_id, first_seqno);
    ASSERT_OK(st);

    // Wait on n reads.
    for (int i = 0; i < num_publish; ++i) {
      ASSERT_TRUE(read_sem.TimedWait(timeout));
    }
    // Assert that num gaps and num logs is what we expected.
    ASSERT_TRUE(num_gaps == 1);
    ASSERT_TRUE(num_logs == num_publish - 1);

    // Delete readers
    for (auto r : readers) {
      delete r;
    }

    publish_sem.Post();
  };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Publish messages.
  for (int i = 1; i < num_publish; ++i) {
    auto msgid = msgid_generator.Generate();
    auto ps = client->Publish(GuestTenant,
                              topic,
                              namespace_id,
                              topic_options,
                              Slice(data),
                              norm_pub_cb,
                              msgid);
    ASSERT_TRUE(ps.status.ok());
    ASSERT_TRUE(ps.msgid == msgid);
  }

  auto msgid = msgid_generator.Generate();
  auto ps = client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            last_pub_cb,
                            msgid);
  ASSERT_TRUE(ps.status.ok());
  ASSERT_TRUE(ps.msgid == msgid);

  // Wait on published messages.
  for (int i = 0; i < num_publish; ++i) {
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }
}

TEST_F(IntegrationTest, TrimGapHandling) {
  // Test to ensure that correct records are received when subscribing to
  // trimmed sections of log.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.single_log = true;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Constants.
  const NamespaceID ns = GuestNamespace;
  const Topic topics[2] = {"TrimGapHandling1", "TrimGapHandling2"};
  const int num_messages = 10;

  port::Semaphore recv_sem[2];

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Publish messages.
  SequenceNumber seqnos[num_messages];
  for (int i = 0; i < num_messages; ++i) {
    port::Semaphore publish_sem;
    auto publish_callback = [&, i](std::unique_ptr<ResultStatus> rs) {
      seqnos[i] = rs->GetSequenceNumber();
      publish_sem.Post();
    };
    // Even messages to topics[0], odd to topics[1].
    auto ps = client->Publish(GuestTenant,
                              topics[i & 1],
                              ns,
                              TopicOptions(),
                              std::to_string(i),
                              publish_callback);
    ASSERT_TRUE(ps.status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  // Find topic log (for trimming).
  LogID log_id;
  LogID log_id2;
  ASSERT_OK(cluster.GetLogRouter()->GetLogID(ns, topics[0], &log_id));
  ASSERT_OK(cluster.GetLogRouter()->GetLogID(ns, topics[1], &log_id2));
  ASSERT_EQ(log_id, log_id2);  // opts.single_log = true; should ensure this.

  auto test_receipt = [&](int topic, SequenceNumber seqno, int expected) {
    auto receive_callback = [&recv_sem, topic](
        std::unique_ptr<MessageReceived>&) { recv_sem[topic].Post(); };
    // Tests that subscribing to topic@seqno results in 'expected' messages.
    auto handle = client->Subscribe(
        GuestTenant, ns, topics[topic], seqno, receive_callback);
    ASSERT_TRUE(handle);
    while (expected--) {
      EXPECT_TRUE(recv_sem[topic].TimedWait(timeout));
    }
    ASSERT_TRUE(!recv_sem[topic].TimedWait(std::chrono::milliseconds(100)));
    ASSERT_OK(client->Unsubscribe(std::move(handle)));
  };

  test_receipt(0, seqnos[0], 5);
  test_receipt(1, seqnos[0], 5);

  // Trim first 6, so should be 2 of each left.
  ASSERT_OK(cluster.GetLogStorage()->Trim(log_id, seqnos[5]));
  cluster.GetControlTower()->SetInfoSync({"cache", "clear"});
  test_receipt(0, seqnos[0], 2);
  test_receipt(1, seqnos[0], 2);

  // Check at edges of gap.
  test_receipt(0, seqnos[5], 2);
  test_receipt(1, seqnos[5], 2);
  test_receipt(0, seqnos[6], 2);
  test_receipt(1, seqnos[6], 2);
}

TEST_F(IntegrationTest, SequenceNumberZero) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message read semaphore.
  port::Semaphore message_sem;
  port::Semaphore publish_sem;
  port::Semaphore subscribe_sem;

  // Message setup.
  Topic topic = "SequenceNumberZero";
  NamespaceID ns = GuestNamespace;
  TopicOptions opts;

  // RocketSpeed callbacks.
  std::vector<std::string> received;
  ThreadCheck thread_check;
  auto receive_callback = [&](std::unique_ptr<MessageReceived>& mr) {
    // Messages from the same topic will always be received on the same thread.
    thread_check.Check();
    received.push_back(mr->GetContents().ToString());
    message_sem.Post();
  };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Witness for publishes to ensure all are released.
  port::Semaphore witness;
  client->Subscribe(GuestTenant, ns, topic, 1,
    [&](std::unique_ptr<MessageReceived>& mr) {
      witness.Post();
    });

  // Send some messages and wait for the acks.
  for (int i = 0; i < 3; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(
        client->Publish(
                    GuestTenant, topic, ns, opts, Slice(data), nullptr)
            .status.ok());
    ASSERT_TRUE(witness.TimedWait(timeout));
  }

  // Subscribe using seqno 0.
  auto handle = client->Subscribe(GuestTenant, ns, topic, 0, receive_callback);
  ASSERT_TRUE(handle);
  ASSERT_TRUE(!message_sem.TimedWait(std::chrono::milliseconds(100)));

  // Should not receive any of the last three messages.
  // Send 3 more different messages.
  for (int i = 3; i < 6; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(
        client->Publish(
                    GuestTenant, topic, ns, opts, Slice(data), nullptr)
            .status.ok());
    ASSERT_TRUE(witness.TimedWait(timeout));
    ASSERT_TRUE(message_sem.TimedWait(timeout));
  }

  {
    std::vector<std::string> expected = {"3", "4", "5"};
    ASSERT_TRUE(received == expected);
  }

  // Unsubscribe from previously subscribed topic.
  ASSERT_OK(client->Unsubscribe(std::move(handle)));

  // Send some messages and wait for the acks.
  for (int i = 6; i < 9; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(
        client->Publish(
                    GuestTenant, topic, ns, opts, Slice(data), nullptr)
            .status.ok());
    ASSERT_TRUE(witness.TimedWait(timeout));
  }

  // Subscribe using seqno 0.
  ASSERT_TRUE(client->Subscribe(GuestTenant, ns, topic, 0, receive_callback));
  ASSERT_TRUE(!message_sem.TimedWait(std::chrono::milliseconds(100)));

  // Send 3 more messages again.
  for (int i = 9; i < 12; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(
        client->Publish(
                    GuestTenant, topic, ns, opts, Slice(data), nullptr)
            .status.ok());
    ASSERT_TRUE(witness.TimedWait(timeout));
    ASSERT_TRUE(message_sem.TimedWait(timeout));
  }

  {  // Should not receive any of the messages sent while it unsubscribed.
    std::vector<std::string> expected = {"3", "4", "5", "9", "10", "11"};
    ASSERT_EQ(received, expected);
  }
}

/*
 * Verify that we do not leak any threads
 */
TEST_F(IntegrationTest, ThreadLeaks) {
  // Setup local RocketSpeed cluster with default environment.
  Env* env = Env::Default();

  // First create a cluster to initialize any cached static instances.
  {
    LocalTestCluster cluster(info_log, true, true, true, "", env);
  }

  // Get the initial steady state number of threads.
  auto init = env->GetNumberOfThreads();

  // Create and destroy a cluster with this env and then
  // verify that we do not have threads associated with this env
  // Create control tower,  pilot and copilot
  {
    LocalTestCluster cluster(info_log, true, true, true, "", env);
    ASSERT_GE(env->GetNumberOfThreads(), init);
  }
  ASSERT_EQ(env->GetNumberOfThreads(), init);

  // Create control tower and pilot
  {
    LocalTestCluster cluster(info_log, true, true, false, "", env);
    ASSERT_GE(env->GetNumberOfThreads(), init);
  }
  ASSERT_EQ(env->GetNumberOfThreads(), init);

  // Create control tower and copilot
  {
    LocalTestCluster cluster(info_log, true, false, true, "", env);
    ASSERT_GE(env->GetNumberOfThreads(), init);
  }
  ASSERT_EQ(env->GetNumberOfThreads(), init);
}

/**
 * Check that sending goodbye message removes subscriptions.
 */
TEST_F(IntegrationTest, UnsubscribeOnGoodbye) {
  LocalTestCluster cluster(info_log, true, true, true);
  ASSERT_OK(cluster.GetStatus());

  port::Semaphore received_data;

  // Start client loop.
  MsgLoop client(env_, EnvOptions(), 0, 1, info_log, "client");
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mDeliver] =
      [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
        received_data.Post();
      };
  callbacks[MessageType::mGap] =
      [](Flow* flow, std::unique_ptr<Message>, StreamID) {};
  callbacks[MessageType::mDataAck] =
      [](Flow* flow, std::unique_ptr<Message>, StreamID) {};
  client.RegisterCallbacks(callbacks);
  StreamSocket socket(
      client.CreateOutboundStream(cluster.GetCopilot()->GetHostId(), 0));
  ASSERT_OK(client.Initialize());
  auto tid = env_->StartThread([&]() { client.Run(); }, "client");
  client.WaitUntilRunning();

  // Subscribe.
  NamespaceID ns = GuestNamespace;
  Topic topic = "UnsubscribeOnGoodbye";
  MessageSubscribe sub(
      Tenant::GuestTenant, ns, topic, 1, SubscriptionID::Unsafe(1));
  ASSERT_OK(client.SendRequest(sub, &socket, 0));

  // Now say goodbye.
  MessageGoodbye goodbye(Tenant::GuestTenant,
                         MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);
  ASSERT_OK(client.SendRequest(goodbye, &socket, 0));
  env_->SleepForMicroseconds(100 * 1000);  // allow goodbye to process

  // Now publish to pilot.
  // We shouldn't get the message.
  MessageData publish(
      MessageType::mPublish, Tenant::GuestTenant, topic, ns, "data");
  ASSERT_OK(client.SendRequest(publish, &socket, 0));
  ASSERT_TRUE(!received_data.TimedWait(std::chrono::milliseconds(100)));

  client.Stop();
  env_->WaitForJoin(tid);
}

TEST_F(IntegrationTest, LostConnection) {
  // Tests that client can be used after it loses connection to the cloud.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.copilot.rollcall_enabled = false;
  opts.info_log = info_log;
  std::unique_ptr<LocalTestCluster> cluster(new LocalTestCluster(opts));
  ASSERT_OK(cluster->GetStatus());
  auto port = cluster->GetCockpitLoop()->GetHostId().GetPort();

  // Message read semaphore.
  port::Semaphore msg_received;

  // Message setup.
  Topic topic = "LostConnection";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";

  // RocketSpeed callbacks;
  auto ok_callback =
      [&](std::unique_ptr<ResultStatus> rs) { ASSERT_OK(rs->GetStatus()); };

  port::Semaphore error_sem;
  auto disconnected_callback = [&](std::unique_ptr<ResultStatus> rs) {
    ASSERT_TRUE(!rs->GetStatus().ok());
    error_sem.Post();
  };

  auto receive_callback = [&](std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  // We do this to save some time waiting for client to reconnect.
  options.timer_period = std::chrono::milliseconds(1);
  options.backoff_strategy = [](ClientRNG*, size_t) {
    return std::chrono::seconds(0);
  };
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster->CreateClient(&client, std::move(options)));
  client->SetDefaultCallbacks(nullptr, receive_callback);

  // Listen on a topic.
  ASSERT_TRUE(client->Subscribe(GuestTenant, namespace_id, topic, 0));
  env_->SleepForMicroseconds(200000);

  // Send a message.
  ASSERT_OK(client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            ok_callback).status);

  // Wait for the message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));

  // Restart the cluster, which kills connection.
  opts.cluster = cluster->GetLogCluster();  // use same log cluster
  cluster.reset();
  env_->SleepForMicroseconds(200000);

  // Send another message while disconnected.
  ASSERT_OK(client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            disconnected_callback).status);
  ASSERT_TRUE(error_sem.TimedWait(timeout));

  // Start a cluster on the same port.
  opts.cockpit_port = port;
  cluster.reset(new LocalTestCluster(opts));
  ASSERT_OK(cluster->GetStatus());

  // We do NOT reissue subscription requests, this is done automatically by the
  // client.

  // Send another message.
  ASSERT_OK(client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            ok_callback).status);

  // Wait for message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));
}

TEST_F(IntegrationTest, OneMessageWithoutRollCall) {
  // This test is a duplicate of OneMessage, just with RollCall disabled.
  // The intention is just to be a quick check to ensure that things still
  // function without RollCall.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.copilot.rollcall_enabled = false;
  opts.info_log = info_log;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Message setup.
  Topic topic = "OneMessageWithoutRollCall";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Send a message.
  auto ps = client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            nullptr,
                            message_id);
  ASSERT_TRUE(ps.status.ok());
  ASSERT_TRUE(ps.msgid == message_id);

  // Listen for the message.
  port::Semaphore msg_received1;
  auto handle =
      client->Subscribe(GuestTenant,
                        namespace_id,
                        topic,
                        1,
                        [&](std::unique_ptr<MessageReceived>& mr) {
                          ASSERT_TRUE(mr->GetContents().ToString() == data);
                          msg_received1.Post();
                        });
  ASSERT_TRUE(handle);

  // Wait for the message.
  ASSERT_TRUE(msg_received1.TimedWait(timeout));

  // Should have received backlog records, and no tail records.
  auto stats1 = cluster.GetControlTower()->GetStatisticsSync();
  auto backlog1 =
      stats1.GetCounterValue("tower.topic_tailer.backlog_records_received");
  auto tail1 =
      stats1.GetCounterValue("tower.topic_tailer.tail_records_received");
  ASSERT_GE(backlog1, 1);
  ASSERT_EQ(tail1, 0);
  client->Unsubscribe(handle);

  // Now subscribe at tail, and publish 1.
  port::Semaphore msg_received2;
  handle =
      client->Subscribe(GuestTenant,
                        namespace_id,
                        topic,
                        0,
                        [&](std::unique_ptr<MessageReceived>& mr) {
                          ASSERT_TRUE(mr->GetContents().ToString() == data);
                          msg_received2.Post();
                        });
  ASSERT_TRUE(handle);

  env_->SleepForMicroseconds(100000);

  client->Publish(GuestTenant,
                  topic,
                  namespace_id,
                  topic_options,
                  Slice(data),
                  nullptr,
                  message_id);
  ASSERT_TRUE(msg_received2.TimedWait(timeout));

  // Should have received no more backlog records, and just 1 tail record.
  auto stats2 = cluster.GetControlTower()->GetStatisticsSync();
  auto backlog2 =
      stats2.GetCounterValue("tower.topic_tailer.backlog_records_received");
  auto tail2 =
      stats2.GetCounterValue("tower.topic_tailer.tail_records_received");
  ASSERT_EQ(backlog2, backlog1);
  ASSERT_EQ(tail2, 1);
}

TEST_F(IntegrationTest, NewControlTower) {
  // Will send a message to one control tower. Stop it, bring up a new one,
  // inform copilot of the change, then try to send another.

  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message setup.
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;

  // RocketSpeed callbacks
  port::Semaphore msg_received;
  auto receive_callback =
      [&](std::unique_ptr<MessageReceived>& mr) { msg_received.Post(); };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));
  client->SetDefaultCallbacks(nullptr, receive_callback);

  // Send a message.
  ASSERT_OK(client->Publish(GuestTenant,
                            "NewControlTower1",
                            namespace_id,
                            topic_options,
                            "message1").status);

  // Listen for the message.
  ASSERT_TRUE(
      client->Subscribe(GuestTenant, namespace_id, "NewControlTower1", 1));

  // Wait for the message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));

  // Stop Control Tower
  cluster.GetControlTowerLoop()->Stop();
  cluster.GetControlTower()->Stop();

  // Send another message to a different topic.
  ASSERT_OK(client->Publish(GuestTenant,
                            "NewControlTower2",
                            namespace_id,
                            topic_options,
                            "message2").status);

  // Start new control tower (only).
  LocalTestCluster::Options new_opts;
  new_opts.info_log = info_log;
  new_opts.start_controltower = true;
  new_opts.start_copilot = false;
  new_opts.start_pilot = false;
  new_opts.cluster = cluster.GetLogCluster();  // use same log cluster
  LocalTestCluster new_cluster(new_opts);
  ASSERT_OK(new_cluster.GetStatus());

  // Inform copilot of new control tower.
  std::unordered_map<uint64_t, HostId> new_towers = {
      {0, new_cluster.GetControlTower()->GetHostId()}};
  auto new_router = std::make_shared<RendezvousHashTowerRouter>(new_towers, 1);
  ASSERT_OK(cluster.GetCopilot()->UpdateTowerRouter(std::move(new_router)));

  // Listen for the message.
  // This subscription request should be routed to the new control tower.
  ASSERT_TRUE(
      client->Subscribe(GuestTenant, namespace_id, "NewControlTower2", 1));

  // Wait for the message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));

  // Send a message on the old topic again.
  ASSERT_OK(client->Publish(GuestTenant,
                            "NewControlTower1",
                            namespace_id,
                            topic_options,
                            "message3").status);

  // The copilot should have re-subscribed us to the new control tower.
  ASSERT_TRUE(msg_received.TimedWait(timeout));
}

class MessageReceivedMock : public MessageReceived {
 public:
  MessageReceivedMock(SubscriptionHandle sub_id,
                      SequenceNumber seqno,
                      Slice payload)
  : sub_id_(sub_id), seqno_(seqno), payload_(std::move(payload)) {}

  SubscriptionHandle GetSubscriptionHandle() const override { return sub_id_; }

  SequenceNumber GetSequenceNumber() const override { return seqno_; }

  Slice GetContents() const override { return payload_; }

 private:
  SubscriptionHandle sub_id_;
  SequenceNumber seqno_;
  Slice payload_;
};

TEST_F(IntegrationTest, SubscriptionStorage) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  std::string file_path =
      test::TmpDir() + "/SubscriptionStorage-file_storage_data";
  // Create RocketSpeed client.
  ClientOptions options;
  ASSERT_OK(SubscriptionStorage::File(
      options.env, options.info_log, file_path, &options.storage));
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client, std::move(options)));

  // Create some subscriptions.
  std::vector<SubscriptionParameters> expected = {
      SubscriptionParameters(
          Tenant::GuestTenant, GuestNamespace, "SubscriptionStorage_0", 123),
      SubscriptionParameters(
          Tenant::GuestTenant, GuestNamespace, "SubscriptionStorage_1", 0),
  };

  std::vector<SubscriptionHandle> handles;
  for (const auto& params : expected) {
    handles.emplace_back(client->Subscribe(params));
  }

  // Acknowledge some messages.
  MessageReceivedMock message(handles[0], 125, Slice("payload"));
  ASSERT_OK(client->Acknowledge(message));
  // We now expect the higher seqno.
  expected[0].start_seqno = message.GetSequenceNumber() + 1;

  // No need to wait for any callbacks.

  port::Semaphore save_sem;
  auto save_callback = [&save_sem](Status status) {
    ASSERT_OK(status);
    save_sem.Post();
  };
  client->SaveSubscriptions(save_callback);
  ASSERT_TRUE(save_sem.TimedWait(timeout));

  // Restore subscriptions.
  std::vector<SubscriptionParameters> restored;
  ASSERT_OK(client->RestoreSubscriptions(&restored));
  std::sort(restored.begin(),
            restored.end(),
            [](SubscriptionParameters a, SubscriptionParameters b) {
              return a.topic_name < b.topic_name;
            });
  ASSERT_TRUE(expected == restored);
}

TEST_F(IntegrationTest, SubscriptionManagement) {
  // Tests various sub/unsub combinations with multiple clients on the same
  // copilot on the same topic.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.single_log = true;  // for testing topic tailer
  opts.tower.max_subscription_lag = 3;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed clients.
  enum { kNumClients = 2 };
  std::vector<std::string> inbox[kNumClients];
  std::mutex inbox_lock[kNumClients];
  port::Semaphore checkpoint[kNumClients];
  std::unique_ptr<Client> client[kNumClients];

  for (int i = 0; i < kNumClients; ++i) {
    ASSERT_OK(cluster.CreateClient(&client[i]));
    client[i]->SetDefaultCallbacks(
        nullptr,
        [&, i](std::unique_ptr<MessageReceived>& mr) {
          {
            std::lock_guard<std::mutex> lock(inbox_lock[i]);
            inbox[i].push_back(mr->GetContents().ToString());
          }
          checkpoint[i].Post();
        });
  }

  // Publish a message and wait.
  auto pub = [&](int c, Topic topic, Slice data) {
    port::Semaphore sem;
    SequenceNumber pub_seqno;
    client[c]->Publish(GuestTenant,
                       topic,
                       GuestNamespace,
                       TopicOptions(),
                       data,
                       [&sem, &pub_seqno](std::unique_ptr<ResultStatus> rs) {
                         ASSERT_OK(rs->GetStatus());
                         pub_seqno = rs->GetSequenceNumber();
                         sem.Post();
                       });
    EXPECT_TRUE(sem.TimedWait(timeout));
    return pub_seqno;
  };

  // Subscribe to a topic.
  auto sub = [&](int c,
                 Topic topic,
                 SequenceNumber seqno) -> SubscriptionHandle {
    if (seqno == 0) {
      env_->SleepForMicroseconds(100000);  // allow latest seqno to propagate.
    }
    // Leave null callbacks, it'll fall back to the ones set when starting the
    // client.
    auto handle =
        client[c]->Subscribe(GuestTenant, GuestNamespace, topic, seqno);
    if (seqno == 0) {
      env_->SleepForMicroseconds(100000);  // allow latest seqno to propagate.
    }
    return std::move(handle);
  };

  // Unsubscribe from a topic.
  auto unsub = [&](int c, SubscriptionHandle handle) {
    ASSERT_OK(client[c]->Unsubscribe(std::move(handle)));
  };

  // Receive messages.
  auto recv = [&](int c, std::vector<std::string> expected) {
    for (size_t i = 0; i < expected.size(); ++i) {
      // Wait for expected messages.
      checkpoint[c].TimedWait(timeout);
    }
    // No more.
    env_->SleepForMicroseconds(10000);
    std::lock_guard<std::mutex> lock(inbox_lock[c]);
    ASSERT_EQ(inbox[c], expected);
    inbox[c].clear();
  };

  // Checks number of subscriptions and topics subscribed on the Copilot.
  auto check_cp_stats = [&](uint64_t num_subs, uint64_t num_topics) {
    auto stats = cluster.GetCopilot()->GetStatisticsSync();
    ASSERT_EQ(num_subs,
              stats.GetCounterValue("copilot.incoming_subscriptions"));
    ASSERT_EQ(num_topics, stats.GetCounterValue("copilot.subscribed_topics"));
  };

  // Empty inboxes.
  recv(0, {});
  recv(1, {});

  // No subscriptions.
  check_cp_stats(0, 0);

  // Subscribe client 0 to start, publish 3, receive 3.
  SequenceNumber a0 = pub(0, "a", "a0");
  auto h_0a = sub(0, "a", a0);
  SequenceNumber a1 = pub(0, "a", "a1");
  pub(0, "a", "a2");
  recv(0, {"a0", "a1", "a2"});
  check_cp_stats(1, 1);

  // Subscribe client 1 at message 2/3, receive messages 2 + 3
  auto h_1a = sub(1, "a", a1);
  recv(0, {});
  recv(1, {"a1", "a2"});
  check_cp_stats(2, 1);

  // Publish one more, both receive.
  pub(0, "a", "a3");
  recv(0, {"a3"});
  recv(1, {"a3"});

  // Unsubscribe client 0, publish, only client 1 should receive.
  unsub(0, h_0a);
  pub(0, "a", "a4");
  recv(0, {});
  recv(1, {"a4"});
  check_cp_stats(1, 1);

  // Unsubscribe client 1, publish, neither should receive.
  unsub(1, h_1a);
  pub(0, "a", "a5");
  recv(0, {});
  recv(1, {});
  check_cp_stats(0, 0);

  // Subscribe both at tail, publish, both should receive.
  sub(0, "a", 0);
  h_1a = sub(1, "a", 0);
  pub(0, "a", "a6");
  recv(0, {"a6"});
  recv(1, {"a6"});

  // Unsubscribe client 1, publish, resubscribe at tail, publish again.
  // Client 0 should receive both, while client 1 only last one.
  unsub(1, h_1a);
  pub(0, "a", "a7");
  sub(1, "a", 0);
  pub(0, "a", "a8");
  recv(0, {"a7", "a8"});
  recv(1, {"a8"});

  // Test topic tailer handling of intertwined topic subscriptions.
  // Subscribe both to b and publish b0.
  sub(0, "b", a0);
  auto h_1b = sub(1, "b", a0);
  pub(0, "b", "b0");
  recv(0, {"b0"});
  recv(1, {"b0"});

  // Unsubscribe b then advance the log with topic c.
  unsub(1, h_1b);
  pub(0, "c", "c0");
  SequenceNumber c1 = pub(0, "c", "c1");
  pub(0, "c", "c2");

  // Subsribe client1 to c1.
  // This must cause log tailer to rewind.
  auto h_1c = sub(1, "c", c1);
  recv(1, {"c1", "c2"});
  unsub(1, h_1c);

  // Check that client 0 can still read b's (gapless topics mean that we still
  // need to keep track of last b).
  SequenceNumber after_c = pub(0, "b", "b1");
  recv(0, {"b1"});

  sub(0, "c", after_c);
  sub(1, "c", c1);
  recv(1, {"c1", "c2"});
  recv(0, {});
  pub(0, "c", "c3");
  recv(0, {"c3"});
  recv(1, {"c3"});

  // Staggered before and after.
  sub(0, "d", c1);
  sub(1, "d", c1 - 1);
  pub(0, "d", "d0");
  recv(0, {"d0"});
  recv(1, {"d0"});

  sub(0, "e", c1);
  sub(1, "e", c1 + 1);
  pub(0, "e", "e0");
  recv(0, {"e0"});
  recv(1, {"e0"});

  // Topic rewind.
  sub(0, "f", a0);
  SequenceNumber f0 = pub(0, "f", "f0");
  pub(0, "!f", "!f0");
  recv(0, {"f0"});
  sub(1, "f", f0 - 1);
  recv(1, {"f0"});
  pub(0, "f", "f1");
  recv(0, {"f1"});
  recv(1, {"f1"});

  // Topic rewind on empty topic.
  sub(0, "g", a0);  // will go to tail
  sub(1, "g", f0);
  pub(0, "g", "g0");
  recv(0, {"g0"});
  recv(1, {"g0"});

  // Multiple subscriptions.
  sub(0, "h", 0);
  auto h0 = pub(1, "h", "h0");
  auto h1 = pub(1, "h", "h1");
  auto h2 = pub(1, "h", "h2");
  recv(0, {"h0", "h1", "h2"});
  sub(0, "h", h2);
  recv(0, {"h2"});
  sub(0, "h", h1);
  recv(0, {"h1", "h2"});
  sub(0, "h", h0);
  recv(0, {"h0", "h1", "h2"});
  // Since we have 4 subscriptions at the tail, whatever we publish now arrives
  // in as many copies, one per subscription.
  pub(1, "h", "h3");
  recv(0, {"h3", "h3", "h3", "h3"});

  SequenceNumber i0 = pub(0, "i", "i0");
  pub(0, "j", "j0");
  pub(0, "i", "i1");
  pub(0, "j", "j1");
  pub(0, "j", "j2");
  sub(0, "i", i0);
  recv(0, {"i0", "i1"});
  sub(1, "i", i0);
  recv(1, {"i0", "i1"});

  // Subscribe to the future then 0.
  SequenceNumber k0 = pub(0, "k", "k0");
  sub(0, "k", k0 + 100000000);
  sub(1, "k", 0);
  pub(0, "k", "k1");
  recv(0, {});
  recv(1, {"k1"});
}

TEST_F(IntegrationTest, LogAvailability) {
  // Tests the availability of a single log after control tower failure.
  // Requires copilot talks to at least 2 control towers for each log.
  // Will test CT failure by stopping the CT loop, without closing connections.

  // Setup local RocketSpeed cluster (pilot + copilot + tower).
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.start_controltower = true;
  opts.start_copilot = true;
  opts.start_pilot = true;
  opts.copilot.rollcall_enabled = false;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Start new control tower (only).
  LocalTestCluster::Options ct_opts[2];
  std::unique_ptr<LocalTestCluster> ct_cluster[2];
  for (int i = 0; i < 2; ++i) {
    ct_opts[i].info_log = info_log;
    ct_opts[i].start_controltower = true;
    ct_opts[i].start_copilot = false;
    ct_opts[i].start_pilot = false;
    ct_opts[i].cluster = cluster.GetLogCluster();  // use same log cluster
    ct_cluster[i].reset(new LocalTestCluster(ct_opts[i]));
    ASSERT_OK(ct_cluster[i]->GetStatus());
  }

  // Inform copilot of origin control tower, and second control tower
  // (but not third -- we'll use that later).
  std::unordered_map<uint64_t, HostId> new_towers = {
      {0, cluster.GetControlTower()->GetHostId()},
      {1, ct_cluster[0]->GetControlTower()->GetHostId()}};
  auto new_router = std::make_shared<RendezvousHashTowerRouter>(new_towers, 2);
  ASSERT_OK(cluster.GetCopilot()->UpdateTowerRouter(std::move(new_router)));

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Listen on many topics (to ensure at least one goes to each tower).
  port::Semaphore msg_received;
  std::vector<SubscriptionHandle> subscriptions;
  enum { kNumTopics = 10 };
  for (int i = 0; i < kNumTopics; ++i) {
    auto handle = client->Subscribe(
        GuestTenant,
        GuestNamespace,
        "LogAvailability" + std::to_string(i),
        1,
        [&](std::unique_ptr<MessageReceived>& mr) { msg_received.Post(); });
    subscriptions.push_back(handle);
  }

  // Stop the first control tower to ensure it cannot deliver records.
  cluster.GetControlTowerLoop()->Stop();
  cluster.GetControlTower()->Stop();

  // Publish to all topics.
  for (int i = 0; i < kNumTopics; ++i) {
    ASSERT_OK(client->Publish(GuestTenant,
                              "LogAvailability" + std::to_string(i),
                              GuestNamespace,
                              TopicOptions(),
                              "data").status);
  }

  // Check all messages were received despite one control tower down.
  for (int i = 0; i < kNumTopics; ++i) {
    ASSERT_TRUE(msg_received.TimedWait(timeout));
  }

  // Update the control tower mapping to be just the third tower.
  new_towers = {
      {2, ct_cluster[1]->GetControlTower()->GetHostId()},
  };
  new_router = std::make_shared<RendezvousHashTowerRouter>(new_towers, 1);
  ASSERT_OK(cluster.GetCopilot()->UpdateTowerRouter(std::move(new_router)));
  env_->SleepForMicroseconds(200000);

  // Resend subscriptions.
  port::Semaphore msg_received2;
  for (int i = 0; i < kNumTopics; ++i) {
    auto handle = client->Subscribe(
        GuestTenant,
        GuestNamespace,
        "LogAvailability" + std::to_string(i),
        0,
        [&](std::unique_ptr<MessageReceived>& mr) { msg_received2.Post(); });
    subscriptions.push_back(handle);
  }
  env_->SleepForMicroseconds(200000);

  // Publish to all topics.
  for (int i = 0; i < kNumTopics; ++i) {
    ASSERT_OK(client->Publish(GuestTenant,
                              "LogAvailability" + std::to_string(i),
                              GuestNamespace,
                              TopicOptions(),
                              "data").status);
  }

  // This should resubscribe using the third tower.
  // Check that all messages come through.
  for (int i = 0; i < kNumTopics; ++i) {
    ASSERT_TRUE(msg_received2.TimedWait(timeout));
  }

  // Check that unsubscribes work.
  // We would have still had some lingering subscriptions on ct_cluster[0],
  // so this tests that they have been cleaned up correctly despite not being
  // the active subscription.
  for (auto handle : subscriptions) {
    client->Unsubscribe(handle);
  }
  env_->SleepForMicroseconds(200000);

  ASSERT_EQ(GetNumOpenLogs(ct_cluster[1]->GetControlTower()), 0);
  ASSERT_EQ(GetNumOpenLogs(ct_cluster[0]->GetControlTower()), 0);
}

TEST_F(IntegrationTest, TowerDeathReconnect) {
  // Connect to a tower, kill it, bring back up at same host ID, and check that
  // we reconnect and resub.
  const size_t kNumTopics = 100;

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.start_controltower = true;
  opts.start_pilot = true;
  opts.start_copilot = true;
  opts.copilot.timer_interval_micros = 100000;
  opts.copilot.resubscriptions_per_second = kNumTopics;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());
  auto original_port = cluster.GetControlTowerLoop()->GetHostId().GetPort();

  // RocketSpeed callbacks
  port::Semaphore msg_received;
  auto receive_callback =
      [&](std::unique_ptr<MessageReceived>& mr) { msg_received.Post(); };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Listen for messages.
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_TRUE(client->Subscribe(GuestTenant,
                                  GuestNamespace,
                                  "TowerDeathReconnect" + std::to_string(t),
                                  1,
                                  receive_callback));
  }

  // Send a message.
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_OK(client->Publish(GuestTenant,
                              "TowerDeathReconnect" + std::to_string(t),
                              GuestNamespace,
                              TopicOptions(),
                              "message1").status);
  }

  for (size_t t = 0; t < kNumTopics; ++t) {
    // Wait for the message.
    ASSERT_TRUE(msg_received.TimedWait(timeout));
  }
  ASSERT_TRUE(!msg_received.TimedWait(std::chrono::milliseconds(100)));

  // Stop Control Tower
  cluster.GetControlTowerLoop()->Stop();
  cluster.GetControlTower()->Stop();

  // Copilot should eventually orphan all topics since there are no CTs left.
  ASSERT_EVENTUALLY_TRUE(
    cluster.GetCopilot()->GetStatisticsSync()
      .GetCounterValue("copilot.orphaned_topics") == kNumTopics);

  // Start new control tower (only) with same host:port.
  LocalTestCluster::Options new_opts;
  new_opts.info_log = info_log;
  new_opts.start_controltower = true;
  new_opts.start_copilot = false;
  new_opts.start_pilot = false;
  new_opts.controltower_port = original_port;
  new_opts.cluster = cluster.GetLogCluster();  // use same log cluster

  uint64_t start = env_->NowMicros();
  LocalTestCluster new_cluster(new_opts);
  ASSERT_OK(new_cluster.GetStatus());

  // Send another message.
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_OK(client->Publish(GuestTenant,
                              "TowerDeathReconnect" + std::to_string(t),
                              GuestNamespace,
                              TopicOptions(),
                              "message2").status);
  }

  // Wait for the messages.
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_TRUE(msg_received.TimedWait(timeout));
  }

  uint64_t end = env_->NowMicros();

  // Should have taken ~1 second to resubscribe all topics.
  ASSERT_GE(end - start, 900000);
  ASSERT_LE(end - start, 2000000);

  // Check that there are no more orhpaned topics.
  auto stats2 = cluster.GetCopilot()->GetStatisticsSync();
  ASSERT_EQ(0, stats2.GetCounterValue("copilot.orphaned_topics"));
}

TEST_F(IntegrationTest, CopilotDeath) {
  const size_t kNumTopics = 10;

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts1;
  opts1.info_log = info_log;
  opts1.start_controltower = true;
  opts1.start_pilot = false;
  opts1.start_copilot = true;
  opts1.copilot.timer_interval_micros = 100000;
  opts1.copilot.resubscriptions_per_second = kNumTopics;
  LocalTestCluster cluster(opts1);
  ASSERT_OK(cluster.GetStatus());

  // Separate cluster for pilot (since we need to stop the copilot
  // independently from the pilot).
  LocalTestCluster::Options opts2;
  opts2.info_log = info_log;
  opts2.start_controltower = false;
  opts2.start_pilot = true;
  opts2.start_copilot = false;
  opts2.cluster = cluster.GetLogCluster();  // use same log cluster
  LocalTestCluster pilot_cluster(opts2);
  ASSERT_OK(pilot_cluster.GetStatus());

  // Create RocketSpeed clients.
  std::unique_ptr<Client> sub_client;
  ASSERT_OK(cluster.CreateClient(&sub_client));

  std::unique_ptr<Client> pub_client;
  ASSERT_OK(cluster.CreateClient(&pub_client));

  // Listen for messages.
  port::Semaphore msg_received;
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_TRUE(sub_client->Subscribe(
        GuestTenant,
        GuestNamespace,
        "CopilotDeath" + std::to_string(t),
        1,
        [&](std::unique_ptr<MessageReceived>& mr) { msg_received.Post(); }));
  }

  // Send a more messages.
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_OK(pub_client->Publish(GuestTenant,
                                  "CopilotDeath" + std::to_string(t),
                                  GuestNamespace,
                                  TopicOptions(),
                                  "message1").status);
  }

  for (size_t t = 0; t < kNumTopics; ++t) {
    // Wait for the message.
    ASSERT_TRUE(msg_received.TimedWait(timeout));
  }
  ASSERT_TRUE(!msg_received.TimedWait(std::chrono::milliseconds(100)));

  // Should have some logs open now.
  ASSERT_NE(GetNumOpenLogs(cluster.GetControlTower()), 0);

  // Stop Copilot
  cluster.GetCockpitLoop()->Stop();
  cluster.GetCopilot()->Stop();

  env_->SleepForMicroseconds(200000);

  // All logs should be closed now.
  ASSERT_EQ(GetNumOpenLogs(cluster.GetControlTower()), 0);
}

TEST_F(IntegrationTest, ControlTowerCache) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.tower.topic_tailer.cache_size = 1024 * 1024;
  opts.info_log = info_log;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // check that we have a non-zero cache capacity
  std::string capacity_str =
      cluster.GetControlTower()->GetInfoSync({"cache", "capacity"});
  size_t capacity = std::stol(capacity_str);
  ASSERT_GT(capacity, 0);

  // check that we initially have no cache usage
  std::string usage_str =
      cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  size_t usage = std::stol(usage_str);
  ASSERT_EQ(usage, 0);

  // Message write semaphore.
  port::Semaphore msg_written;

  // Sequence number of first written message.
  SequenceNumber msg_seqno;

  // Message read semaphore.
  port::Semaphore msg_received, msg_received2, msg_received3, msg_received4,
      msg_received5;

  // Message setup.
  Topic topic = "ControlTowerCache";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // RocketSpeed callbacks;
  auto publish_callback = [&](std::unique_ptr<ResultStatus> rs) {
    msg_seqno = rs->GetSequenceNumber();
    msg_written.Post();
  };

  auto subscription_callback = [&](const SubscriptionStatus& ss) {
    ASSERT_TRUE(ss.GetTopicName() == topic);
    ASSERT_TRUE(ss.GetNamespace() == namespace_id);
  };

  auto receive_callback = [&](std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  auto receive_callback2 = [&](std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received2.Post();
  };

  auto receive_callback3 = [&](std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received3.Post();
  };

  auto receive_callback4 = [&](std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received4.Post();
  };

  auto receive_callback5 = [&](std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received5.Post();
  };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Send a message.
  auto ps = client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            publish_callback,
                            message_id);
  ASSERT_TRUE(ps.status.ok());
  ASSERT_TRUE(ps.msgid == message_id);
  // Wait for the message to be written
  bool result = msg_written.TimedWait(timeout);
  ASSERT_TRUE(result);

  // Listen for the message.
  ASSERT_TRUE(client->Subscribe(GuestTenant,
                                namespace_id,
                                topic,
                                msg_seqno,
                                receive_callback,
                                subscription_callback));
  // Wait for the message to arrive
  result = msg_received.TimedWait(timeout);
  ASSERT_TRUE(result);

  // The first read should not hit any data in the cache
  auto stats1 = cluster.GetControlTower()->GetStatisticsSync();
  auto cached_hits =
      stats1.GetCounterValue("tower.topic_tailer.records_served_from_cache");
  ASSERT_EQ(cached_hits, 0);
  auto cache_misses = stats1.GetCounterValue("tower.data_cache.cache_misses");
  ASSERT_EQ(cache_misses, 2);

  // Resubscribe to the same topic
  ASSERT_TRUE(client->Subscribe(GuestTenant,
                                namespace_id,
                                topic,
                                msg_seqno,
                                receive_callback2,
                                subscription_callback));
  // Wait for the message to arrive
  result = msg_received2.TimedWait(timeout);
  ASSERT_TRUE(result);

  // The second read should fetch data from the cache
  auto stats2 = cluster.GetControlTower()->GetStatisticsSync();
  auto cached_hits2 =
      stats2.GetCounterValue("tower.topic_tailer.records_served_from_cache");
  ASSERT_GE(cached_hits2, 1);

  // check that we have some cache usage
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_GT(usage, 0);

  // clear cache
  cluster.GetControlTower()->SetInfoSync({"cache", "clear"});

  // check that we do not have any cache usage
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_EQ(usage, 0);

  // check that capacity remains the same as before
  std::string new_capacity_str =
      cluster.GetControlTower()->GetInfoSync({"cache", "capacity"});
  size_t new_capacity = std::stol(new_capacity_str);
  ASSERT_EQ(new_capacity, capacity);

  // resubscribe and re-read. The cache should get repopulated.
  ASSERT_TRUE(client->Subscribe(GuestTenant,
                                namespace_id,
                                topic,
                                msg_seqno,
                                receive_callback3,
                                subscription_callback));
  // Wait for the message to arrive
  result = msg_received3.TimedWait(timeout);
  ASSERT_TRUE(result);

  // check that we do have some cache usage
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_GT(usage, 0);

  // Set the capacity to zero. This should disable the cache and
  // purge existing entries.
  new_capacity_str =
      cluster.GetControlTower()->SetInfoSync({"cache", "capacity", "0"});
  ASSERT_EQ(new_capacity_str, "");

  // verify that cache is purged
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_EQ(usage, 0);

  // verify that cache is disabled
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "capacity"});
  usage = std::stol(usage_str);
  ASSERT_EQ(usage, 0);

  // Since the cache is now disabled, resubscribing and re-reading data
  // should not populate it
  ASSERT_TRUE(client->Subscribe(GuestTenant,
                                namespace_id,
                                topic,
                                msg_seqno,
                                receive_callback4,
                                subscription_callback));
  // Wait for the message to arrive
  result = msg_received4.TimedWait(timeout);
  ASSERT_TRUE(result);

  // verify that cache is still empty
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_EQ(usage, 0);

  // remember cache statistics
  auto stats3 = cluster.GetControlTower()->GetStatisticsSync();
  auto cached_hits3 =
      stats3.GetCounterValue("tower.topic_tailer.records_served_from_cache");

  // set a  1 MB cache capacity
  size_t set_capacity = 1024000;
  new_capacity_str = cluster.GetControlTower()->SetInfoSync(
      {"cache", "capacity", std::to_string(set_capacity)});
  ASSERT_EQ(new_capacity_str, "");
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "capacity"});
  usage = std::stol(usage_str);
  ASSERT_EQ(usage, set_capacity);

  // re-read data into cache
  ASSERT_TRUE(client->Subscribe(GuestTenant,
                                namespace_id,
                                topic,
                                msg_seqno,
                                receive_callback5,
                                subscription_callback));
  // Wait for the message to arrive
  result = msg_received5.TimedWait(timeout);
  ASSERT_TRUE(result);

  // verify that cache has some data now
  usage_str = cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_GT(usage, 0);

  // Verify that there were no additional cache hits
  auto stats4 = cluster.GetControlTower()->GetStatisticsSync();
  auto cached_hits4 =
      stats4.GetCounterValue("tower.topic_tailer.records_served_from_cache");
  ASSERT_EQ(cached_hits4, cached_hits3);
}

TEST_F(IntegrationTest, ReadingFromCache) {
  // Run this test twice, the first time with cache enabled and the
  // second time with cache disabled.
  bool cache_enabled = true;
  for (int iteration = 0; iteration < 2; iteration++) {
    if (iteration == 1) {
      cache_enabled = false;
    }
    // Create cluster with rollcall disabled.
    LocalTestCluster::Options opts;
    opts.copilot.rollcall_enabled = false;
    if (cache_enabled) {
      opts.tower.topic_tailer.cache_size = 1024 * 1024;
    } else {
      opts.tower.topic_tailer.cache_size = 0;  // cache disabled
    }
    opts.info_log = info_log;
    LocalTestCluster cluster(opts);
    ASSERT_OK(cluster.GetStatus());

    // Message setup.
    Topic topic = "ReadingFromCache" + std::to_string(iteration);
    NamespaceID namespace_id = GuestNamespace;
    TopicOptions topic_options;
    std::string data1 = "test_message1";
    std::string data2 = "test_message2";
    GUIDGenerator msgid_generator;
    MsgId message_id = msgid_generator.Generate();

    // Create RocketSpeed client.
    std::unique_ptr<Client> client;
    ASSERT_OK(cluster.CreateClient(&client));

    // Send a message.
    port::Semaphore msg_written;
    SequenceNumber msg_seqno = 0;
    auto ps = client->Publish(GuestTenant,
                              topic,
                              namespace_id,
                              topic_options,
                              Slice(data1),
                              [&](std::unique_ptr<ResultStatus> rs) {
                                msg_seqno = rs->GetSequenceNumber();
                                msg_written.Post();
                              },
                              message_id);
    ASSERT_TRUE(ps.status.ok());
    ASSERT_TRUE(ps.msgid == message_id);
    ASSERT_TRUE(msg_written.TimedWait(timeout));

    // Listen for the message.
    port::Semaphore msg_received1;
    SequenceNumber lastReceived;
    auto handle =
        client->Subscribe(GuestTenant,
                          namespace_id,
                          topic,
                          msg_seqno,
                          [&](std::unique_ptr<MessageReceived>& mr) {
                            ASSERT_TRUE(mr->GetContents().ToString() == data1);
                            lastReceived = mr->GetSequenceNumber();
                            ASSERT_EQ(lastReceived, msg_seqno);
                            msg_received1.Post();
                          });
    ASSERT_TRUE(handle);

    // Wait for the message.
    ASSERT_TRUE(msg_received1.TimedWait(timeout));

    // Should have received backlog records, and no tail records.
    auto stats1 = cluster.GetControlTower()->GetStatisticsSync();
    auto backlog1 =
        stats1.GetCounterValue("tower.topic_tailer.backlog_records_received");
    auto tail1 =
        stats1.GetCounterValue("tower.topic_tailer.tail_records_received");
    ASSERT_EQ(backlog1, 1);
    ASSERT_EQ(tail1, 0);
    client->Unsubscribe(handle);

    // Now subscribe at tail, and publish 1.
    port::Semaphore msg_received2;
    handle = client->Subscribe(
        GuestTenant,
        namespace_id,
        topic,
        0,
        [&](std::unique_ptr<MessageReceived>& mr) {
          ASSERT_TRUE((mr->GetContents().ToString() == data2));
          ASSERT_EQ(lastReceived + 1, mr->GetSequenceNumber());
          lastReceived = mr->GetSequenceNumber();
          msg_received2.Post();
        });
    ASSERT_TRUE(handle);

    env_->SleepForMicroseconds(100000);

    client->Publish(GuestTenant,
                    topic,
                    namespace_id,
                    topic_options,
                    Slice(data2),
                    nullptr,
                    message_id);
    ASSERT_TRUE(msg_received2.TimedWait(timeout));

    // Should have received no more backlog records, and just 1 tail record.
    auto stats2 = cluster.GetControlTower()->GetStatisticsSync();
    auto backlog2 =
        stats2.GetCounterValue("tower.topic_tailer.backlog_records_received");
    auto tail2 =
        stats2.GetCounterValue("tower.topic_tailer.tail_records_received");
    ASSERT_EQ(backlog2, backlog1);
    ASSERT_EQ(tail2, 1);
    client->Unsubscribe(handle);

    // At this point, there are two records in the topic and both these
    // two records should be in the cache. Reseek to beginning of the
    // topic and re-read both these two records. If the cache was enabled,
    // then this should not enable backlog reading.
    int numRecords = 2;
    port::Semaphore msg_received3;
    handle = client->Subscribe(
        GuestTenant,
        namespace_id,
        topic,
        msg_seqno,
        [&](std::unique_ptr<MessageReceived>& mr) { msg_received3.Post(); });
    ASSERT_TRUE(handle);
    for (int i = 0; i < numRecords; i++) {
      ASSERT_TRUE(msg_received3.TimedWait(timeout));
    }

    // collect current statistics
    auto stats3 = cluster.GetControlTower()->GetStatisticsSync();
    auto backlog3 =
        stats3.GetCounterValue("tower.topic_tailer.backlog_records_received");

    // If the cache was enabled, then there should not have been any
    // backlog reading. if the cache was disabled, then this should
    // trigger backlog reading.
    if (cache_enabled) {
      ASSERT_EQ(backlog3, backlog2);
    } else {
      ASSERT_EQ(backlog3, backlog2 + numRecords);
    }
    client->Unsubscribe(handle);
  }
}

#ifndef USE_LOGDEVICE
// This test doesn't work with the LogDevice integration test utils since they
// only support one log, meaning there is no way to balance.
TEST_F(IntegrationTest, TowerRebalance) {
  // Tests that subscriptions are rebalanced across control towers gradually
  // when new towers become available.

  // Setup local RocketSpeed cluster (pilot + copilot + tower).
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.start_controltower = true;
  opts.start_copilot = true;
  opts.start_pilot = true;
  opts.copilot.rollcall_enabled = false;
  opts.copilot.timer_interval_micros = 100000;  // 100ms
  opts.copilot.tower_subscriptions_check_period = std::chrono::seconds(1);
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Start new control towers (only).
  // Copilots doesn't know about these yet.
  LocalTestCluster::Options ct_opts[2];
  std::unique_ptr<LocalTestCluster> ct_cluster[2];
  for (int i = 0; i < 2; ++i) {
    ct_opts[i].info_log = info_log;
    ct_opts[i].start_controltower = true;
    ct_opts[i].start_copilot = false;
    ct_opts[i].start_pilot = false;
    ct_cluster[i].reset(new LocalTestCluster(ct_opts[i]));
    ASSERT_OK(ct_cluster[i]->GetStatus());
  }

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Listen on many topics (to ensure at least one goes to each tower).
  enum { kNumTopics = 40 };
  for (int i = 0; i < kNumTopics; ++i) {
    client->Subscribe(GuestTenant,
                      GuestNamespace,
                      "TowerRebalance" + std::to_string(i),
                      0,
                      [](std::unique_ptr<MessageReceived>&) {});
  }

  // The copilot should be subscribed to all topics the first control tower.
  env_->SleepForMicroseconds(200000);

  // Only the first tower should have logs open.
  ASSERT_NE(GetNumOpenLogs(cluster.GetControlTower()), 0);
  ASSERT_EQ(GetNumOpenLogs(ct_cluster[0]->GetControlTower()), 0);
  ASSERT_EQ(GetNumOpenLogs(ct_cluster[1]->GetControlTower()), 0);
  auto initial_logs_open = GetNumOpenLogs(cluster.GetControlTower());

  // Update the control tower mapping to include all towers.
  std::unordered_map<uint64_t, HostId> new_towers = {
      {0, cluster.GetControlTower()->GetHostId()},
      {1, ct_cluster[0]->GetControlTower()->GetHostId()},
      {2, ct_cluster[1]->GetControlTower()->GetHostId()},
  };
  auto new_router = std::make_shared<RendezvousHashTowerRouter>(new_towers, 1);
  ASSERT_OK(cluster.GetCopilot()->UpdateTowerRouter(std::move(new_router)));
  env_->SleepForMicroseconds(2000000);

  // Now all should have logs open, and first should have fewer logs open.
  ASSERT_NE(GetNumOpenLogs(cluster.GetControlTower()), 0);
  ASSERT_NE(GetNumOpenLogs(ct_cluster[0]->GetControlTower()), 0);
  ASSERT_NE(GetNumOpenLogs(ct_cluster[1]->GetControlTower()), 0);
  ASSERT_LT(GetNumOpenLogs(cluster.GetControlTower()), initial_logs_open);
}
#endif

TEST_F(IntegrationTest, InvalidSubscription) {
  // Subscribes to InvalidNamespace, expects bad subscription status.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  std::unique_ptr<ClientImpl> client;
  ASSERT_OK(cluster.CreateClient(&client, false));

  port::Semaphore sem;
  ASSERT_TRUE(client->Subscribe(
      GuestTenant,
      InvalidNamespace,
      "InvalidSubscription",
      1,
      [&](std::unique_ptr<MessageReceived>& mr) { ASSERT_TRUE(false); },
      [&](const SubscriptionStatus& ss) {
        ASSERT_EQ(ss.GetNamespace(), InvalidNamespace);
        ASSERT_EQ(ss.GetTopicName(), "InvalidSubscription");
        ASSERT_EQ(ss.GetSequenceNumber(), 1);
        ASSERT_TRUE(!ss.GetStatus().ok());
        sem.Post();
      }));
  ASSERT_TRUE(sem.TimedWait(timeout));
}

TEST_F(IntegrationTest, ReaderRestarts) {
  // Test that messages are still received with frequent reader restarts.
  const size_t kNumTopics = 10;
  const size_t kNumMessages = 100;
  const std::chrono::milliseconds kMessageDelay(100);

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.tower.timer_interval = std::chrono::microseconds(10000);  // 10ms
  opts.tower.log_tailer.min_reader_restart_duration =
      std::chrono::milliseconds(200);
  opts.tower.log_tailer.max_reader_restart_duration =
      std::chrono::milliseconds(400);
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Subscribe to all topics.
  port::Semaphore msg_received;
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_TRUE(client->Subscribe(
        GuestTenant,
        GuestNamespace,
        "ReaderRestarts" + std::to_string(t),
        1,
        [&](std::unique_ptr<MessageReceived>& mr) { msg_received.Post(); }));
  }

  // Send messages.
  for (size_t i = 0; i < kNumMessages; ++i) {
    size_t t = i % kNumTopics;
    ASSERT_OK(client->Publish(GuestTenant,
                              "ReaderRestarts" + std::to_string(t),
                              GuestNamespace,
                              TopicOptions(),
                              std::to_string(i)).status);
    /* sleep override */
    std::this_thread::sleep_for(kMessageDelay);
  }

  // Wait for all messages.
  for (size_t t = 0; t < kNumMessages; ++t) {
    ASSERT_TRUE(msg_received.TimedWait(timeout));
  }
  ASSERT_TRUE(!msg_received.TimedWait(std::chrono::milliseconds(10)));

  auto stats = cluster.GetControlTower()->GetStatisticsSync();
  auto restarts = stats.GetCounterValue("tower.log_tailer.forced_restarts");
  size_t min_expected = kNumTopics * kNumMessages * kMessageDelay /
                        opts.tower.log_tailer.max_reader_restart_duration;
  size_t max_expected = kNumTopics * kNumMessages * kMessageDelay /
                        opts.tower.log_tailer.min_reader_restart_duration;
  ASSERT_GE(restarts, min_expected - 1);  // - 1 for timer tolerance
  ASSERT_LE(restarts, max_expected + 1);  // + 1 for timer tolerance
}

TEST_F(IntegrationTest, VirtualReaderMerge) {
  // Tests the virtual reader merge path.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.single_log = true;
  opts.tower.readers_per_room = 2;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  ASSERT_OK(cluster.CreateClient(&client));

  // Publish a message to find the current seqno for the topic.
  SequenceNumber seqno;
  port::Semaphore pub_sem;
  ASSERT_OK(client->Publish(GuestTenant,
                            "VirtualReaderMerge",
                            GuestNamespace,
                            TopicOptions(),
                            "test",
                            [&](std::unique_ptr<ResultStatus> rs) {
                              seqno = rs->GetSequenceNumber();
                              pub_sem.Post();
                            }).status);
  ASSERT_TRUE(pub_sem.TimedWait(timeout));

  // Subscribe to three topics (on same log).
  // One at seqno + 200.
  // One at seqno + 100.
  // One at seqno.
  // Since they are in the future, we ensure that none of the readers will
  // receive messages and merge.
  port::Semaphore recv[3];
  SubscriptionHandle sub[3];
  for (int i = 0; i < 3; ++i) {
    sub[i] = client->Subscribe(
        GuestTenant,
        GuestNamespace,
        "VirtualReaderMerge" + std::to_string(i),
        seqno + (2 - i) * 100,
        [&, i](std::unique_ptr<MessageReceived>& mr) { recv[i].Post(); });
    env_->SleepForMicroseconds(100000);
  }

  // Now publish messages to VirtualReaderMerge2.
  for (int i = 0; i < 200; ++i) {
    ASSERT_OK(client->Publish(GuestTenant,
                              "VirtualReaderMerge2",
                              GuestNamespace,
                              TopicOptions(),
                              "test").status);
  }

  // Check all messages received.
  for (int i = 0; i < 200; ++i) {
    ASSERT_TRUE(recv[2].TimedWait(timeout));
  }

  // Check that no more message came through, then close the subscription to
  // test the stop reading path.
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(!recv[i].TimedWait(std::chrono::milliseconds(10)));
    client->Unsubscribe(sub[i]);
  }
  env_->SleepForMicroseconds(100000);
}

TEST_F(IntegrationTest, SmallRoomQueues) {
  // Tests control tower with room queue size 1.
  const size_t kNumMessages = 1000;

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.copilot.rollcall_enabled = false;
  opts.tower.room_to_client_queue_size = 1;
  opts.tower.topic_tailer.cache_size = 1 << 20;  // 1MB
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed sender client.
  std::unique_ptr<Client> client;
  cluster.CreateClient(&client);

  // Publish a message to find the current seqno for the topic.
  port::Semaphore pub_sem;
  SequenceNumber seqnos[kNumMessages];
  for (size_t i = 0; i < kNumMessages; ++i) {
    ASSERT_OK(client->Publish(GuestTenant,
                              "SmallRoomQueues",
                              GuestNamespace,
                              TopicOptions(),
                              "#" + std::to_string(i),
                              [&, i](std::unique_ptr<ResultStatus> rs) {
                                ASSERT_OK(rs->GetStatus());
                                pub_sem.Post();
                                seqnos[i] = rs->GetSequenceNumber();
                              }).status);
    pub_sem.TimedWait(timeout);
  }
  const size_t kPhase1Messages = kNumMessages - 1;
  const SequenceNumber seqno_phase1 = seqnos[kNumMessages - kPhase1Messages];
  port::Semaphore sem1;
  client->Subscribe(GuestTenant,
                    GuestNamespace,
                    "SmallRoomQueues",
                    seqno_phase1,
                    [&](std::unique_ptr<MessageReceived>& mr) { sem1.Post(); });
  for (size_t i = 0; i < kPhase1Messages; ++i) {
    ASSERT_TRUE(sem1.TimedWait(timeout));
  }

  auto stats1 = cluster.GetControlTower()->GetStatisticsSync();
  stats1.Aggregate(cluster.GetControlTowerLoop()->GetStatisticsSync());
  auto applied1 =
      stats1.GetCounterValue("tower.flow_control.backpressure_applied");
  auto lifted1 =
      stats1.GetCounterValue("tower.flow_control.backpressure_lifted");
  auto received1 =
      stats1.GetCounterValue("tower.topic_tailer.log_records_received");
  ASSERT_GE(applied1,
            lifted1);  // ensure that it was lifted as often as applied
  ASSERT_LE(applied1, lifted1 + 1);  // could be 1 less if not lifted yet.
  ASSERT_EQ(received1, kPhase1Messages);

  // Now cache is filled, so try again to test cache flow.
  port::Semaphore sem2;
  client->Subscribe(GuestTenant,
                    GuestNamespace,
                    "SmallRoomQueues",
                    seqno_phase1,
                    [&](std::unique_ptr<MessageReceived>& mr) { sem2.Post(); });
  for (size_t i = 0; i < kPhase1Messages; ++i) {
    ASSERT_TRUE(sem2.TimedWait(timeout));
  }

  auto stats2 = cluster.GetControlTower()->GetStatisticsSync();
  stats2.Aggregate(cluster.GetControlTowerLoop()->GetStatisticsSync());
  auto applied2 =
      stats2.GetCounterValue("tower.flow_control.backpressure_applied");
  auto lifted2 =
      stats2.GetCounterValue("tower.flow_control.backpressure_lifted");
  auto received2 =
      stats2.GetCounterValue("tower.topic_tailer.log_records_received");
  auto cache2 =
      stats2.GetCounterValue("tower.topic_tailer.records_served_from_cache");
  auto cache_backoff2 =
      stats2.GetCounterValue("tower.topic_tailer.cache_reader_backoff");
  ASSERT_GE(applied2,
            lifted2);  // ensure that it was lifted as often as applied
  ASSERT_LE(applied2, lifted2 + 1);       // could be 1 less if not lifted yet.
  ASSERT_EQ(received2, kPhase1Messages);  // no more messages received
  ASSERT_EQ(cache2, kPhase1Messages);
  ASSERT_GE(cache_backoff2, 0);

  // This time, read from before the cache, to test cache re-entry flow control.
  port::Semaphore sem3;
  client->Subscribe(GuestTenant,
                    GuestNamespace,
                    "SmallRoomQueues",
                    seqnos[0],
                    [&](std::unique_ptr<MessageReceived>& mr) { sem3.Post(); });
  for (size_t i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem3.TimedWait(timeout));
  }

  auto stats3 = cluster.GetControlTower()->GetStatisticsSync();
  stats3.Aggregate(cluster.GetControlTowerLoop()->GetStatisticsSync());
  auto applied3 =
      stats3.GetCounterValue("tower.flow_control.backpressure_applied");
  auto lifted3 =
      stats3.GetCounterValue("tower.flow_control.backpressure_lifted");
  auto received3 =
      stats3.GetCounterValue("tower.topic_tailer.log_records_received");
  auto cache3 =
      stats3.GetCounterValue("tower.topic_tailer.records_served_from_cache");
  auto cache_backoff3 =
      stats3.GetCounterValue("tower.topic_tailer.cache_reader_backoff");
  ASSERT_GE(applied3,
            lifted3);  // ensure that it was lifted as often as applied
  ASSERT_LE(applied3, lifted3 + 1);  // could be 1 less if not lifted yet.
  ASSERT_GE(received3, kNumMessages);
  ASSERT_EQ(cache3, kPhase1Messages + kPhase1Messages);
  ASSERT_EQ(cache_backoff3, cache_backoff2);
}

TEST_F(IntegrationTest, CacheReentrance) {
  // This tests that a reader subscribed outside of the cached data, can
  // read into the cache.
  LocalTestCluster::Options opts;
  opts.copilot.rollcall_enabled = false;
  opts.tower.topic_tailer.cache_size = 1024 * 1024;
  opts.tower.readers_per_room = 2;
  opts.info_log = info_log;
  opts.single_log = true;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  auto get_topic_tailer_stats = [&]() {
    std::string tt_names[] = {"log_records_with_subscriptions",
                              "records_served_from_cache",
                              "reader_merges",
                              "cache_reentries"};
    std::string lt_names[] = {
        "readers_started", "readers_restarted", "readers_stopped"};
    auto stats = cluster.GetControlTower()->GetStatisticsSync();
    std::unordered_map<std::string, int64_t> counters;
    for (auto& name : tt_names) {
      counters[name] = stats.GetCounterValue("tower.topic_tailer." + name);
    }
    for (auto& name : lt_names) {
      counters[name] = stats.GetCounterValue("tower.log_tailer." + name);
    }
    return counters;
  };

  // Create RocketSpeed client.
  std::unique_ptr<Client> client;
  cluster.CreateClient(&client);

  // Send a few messages.
  const size_t kNumMessages = 10;
  port::Semaphore pub_sem;
  SequenceNumber msg_seqnos[kNumMessages];
  for (size_t i = 0; i < kNumMessages; ++i) {
    auto ps = client->Publish(GuestTenant,
                              "CacheReentrance1",
                              GuestNamespace,
                              TopicOptions(),
                              std::to_string(i),
                              [&, i](std::unique_ptr<ResultStatus> rs) {
                                msg_seqnos[i] = rs->GetSequenceNumber();
                                pub_sem.Post();
                              });
    ASSERT_TRUE(pub_sem.TimedWait(timeout));
  }

  // Read half the messages (into the cache).
  const size_t kNumInCache = 4;
  port::Semaphore sub_sem;
  auto handle1 = client->Subscribe(
      GuestTenant,
      GuestNamespace,
      "CacheReentrance1",
      msg_seqnos[kNumMessages - kNumInCache],
      [&](std::unique_ptr<MessageReceived>& mr) { sub_sem.Post(); });
  for (size_t i = 0; i < kNumInCache; ++i) {
    ASSERT_TRUE(sub_sem.TimedWait(timeout));
  }
  client->Unsubscribe(handle1);
  env_->SleepForMicroseconds(100000);

  // Should have received backlog records, none from cache, and no readers
  // should have merged or restarted.
  auto stats1 = get_topic_tailer_stats();
  ASSERT_EQ(stats1["log_records_with_subscriptions"], kNumInCache);
  ASSERT_EQ(stats1["records_served_from_cache"], 0);
  ASSERT_EQ(stats1["reader_merges"], 0);
  ASSERT_EQ(stats1["cache_reentries"], 0);
  ASSERT_EQ(stats1["readers_started"], 1);
  ASSERT_EQ(stats1["readers_restarted"], 0);
  ASSERT_EQ(stats1["readers_stopped"], 1);

  // Now subscribe to the second message.
  // Some records should come from backlog, but some should come from cache.
  // The new reader should have restarted at the tail.
  client->Subscribe(
      GuestTenant,
      GuestNamespace,
      "CacheReentrance1",
      msg_seqnos[1],
      [&](std::unique_ptr<MessageReceived>& mr) { sub_sem.Post(); });
  for (size_t i = 0; i < kNumMessages - 1; ++i) {
    ASSERT_TRUE(sub_sem.TimedWait(timeout));
  }

  auto stats2 = get_topic_tailer_stats();
  ASSERT_EQ(stats2["log_records_with_subscriptions"], kNumMessages - 1);
  ASSERT_EQ(stats2["records_served_from_cache"], kNumInCache);
  ASSERT_EQ(stats2["reader_merges"], 0);
  ASSERT_EQ(stats2["cache_reentries"], 1);
  ASSERT_EQ(stats2["readers_started"], 3);
  ASSERT_EQ(stats2["readers_restarted"], 0);
  ASSERT_EQ(stats2["readers_stopped"], 2);

  // Finally, without closing existing subscription, start a new subscription
  // on a different topic (but same log, due to single_log flag) at the first
  // message. This should open a new reader, which will read one message
  // from backlog, re-enter the cache, then merge with existing reader without
  // restarting.
  client->Subscribe(
      GuestTenant,
      GuestNamespace,
      "CacheReentrance2",
      msg_seqnos[0],
      [&](std::unique_ptr<MessageReceived>& mr) { sub_sem.Post(); });
  env_->SleepForMicroseconds(100000);

  auto stats3 = get_topic_tailer_stats();
  ASSERT_EQ(stats3["log_records_with_subscriptions"], kNumMessages - 1);
  ASSERT_EQ(stats3["records_served_from_cache"], kNumInCache);
  ASSERT_EQ(stats3["reader_merges"], 1);
  ASSERT_EQ(stats3["cache_reentries"], 2);
  ASSERT_EQ(stats3["readers_started"], 4);
  ASSERT_EQ(stats3["readers_restarted"], 0);
  ASSERT_EQ(stats3["readers_stopped"], 3);
}

TEST_F(IntegrationTest, CopilotTailSubscribeFast) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed sender client.
  std::unique_ptr<Client> client;
  cluster.CreateClient(&client);

  auto fast_subs = [&]() {
    auto stats = cluster.GetCopilot()->GetStatisticsSync();
    return stats.GetCounterValue("copilot.tail_subscribe_fast_path");
  };

  auto total_subs = [&]() {
    auto stats = cluster.GetCopilot()->GetStatisticsSync();
    return stats.GetCounterValue("copilot.incoming_subscriptions");
  };

  auto delivered_gaps = [&]() {
    auto stats = cluster.GetCockpitLoop()->GetStatisticsSync();
    return stats.GetCounterValue("cockpit.messages_received.deliver_gap");
  };

  // First subscribe should not be a fast subscription.
  client->Subscribe(
      GuestTenant, GuestNamespace, "CopilotTailSubscribeFast1", 0, nullptr);
  ASSERT_EVENTUALLY_TRUE(total_subs() == 1);
  ASSERT_EQ(fast_subs(), 0);

  // Wait for tail seqno on copilot so that next sub is fast.
  ASSERT_EVENTUALLY_TRUE(delivered_gaps() == 1);

  // Second subscribe should be a fast subscription.
  client->Subscribe(
      GuestTenant, GuestNamespace, "CopilotTailSubscribeFast1", 0, nullptr);
  ASSERT_EVENTUALLY_TRUE(total_subs() == 2);
  ASSERT_EQ(fast_subs(), 1);
  ASSERT_EQ(delivered_gaps(), 1);  // should still be 1

  // Different topic now.
  // First subscribe should not be a fast subscription.
  port::Semaphore sem;
  client->Subscribe(
      GuestTenant, GuestNamespace, "CopilotTailSubscribeFast2", 1,
      [&](std::unique_ptr<MessageReceived>&) { sem.Post(); });
  ASSERT_EVENTUALLY_TRUE(total_subs() == 3);
  ASSERT_EQ(fast_subs(), 1);
  client->Publish(GuestTenant, "CopilotTailSubscribeFast2", GuestNamespace,
      TopicOptions(), "data");
  ASSERT_TRUE(sem.TimedWait(timeout));
  auto gaps_now = delivered_gaps();

  // Second subscribe should not be a fast subscription since we don't have
  // tail subscription.
  // Note: this could change with better tail subscription logic.
  client->Subscribe(
      GuestTenant, GuestNamespace, "CopilotTailSubscribeFast2", 0, nullptr);
  ASSERT_EVENTUALLY_TRUE(total_subs() == 4);
  ASSERT_EQ(fast_subs(), 1);

  // Wait for tail seqno on copilot so that next sub is fast.
  ASSERT_EVENTUALLY_TRUE(delivered_gaps() == gaps_now + 1);

  // Third subscribe should be a fast subscription.
  // Note: this could change with better tail subscription logic.
  client->Subscribe(
      GuestTenant, GuestNamespace, "CopilotTailSubscribeFast2", 0, nullptr);
  ASSERT_EVENTUALLY_TRUE(total_subs() == 5);
  ASSERT_EQ(fast_subs(), 2);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
