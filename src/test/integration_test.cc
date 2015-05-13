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
#include "src/port/port.h"
#include "src/util/common/guid_generator.h"
#include "src/util/common/thread_check.h"
#include "src/util/testharness.h"
#include "src/test/test_cluster.h"

namespace rocketspeed {

class IntegrationTest {
 public:
  std::chrono::seconds timeout;

  IntegrationTest()
      : timeout(5)
      , env_(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env_, "IntegrationTest", &info_log));
  }

 protected:
  Env* env_;
  std::shared_ptr<rocketspeed::Logger> info_log;
};

TEST(IntegrationTest, OneMessage) {
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
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {
  };

  auto subscription_callback = [&] (SubscriptionStatus ss) {
    ASSERT_TRUE(ss.topic_name == topic);
    ASSERT_TRUE(ss.namespace_id == namespace_id);
  };

  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    ASSERT_TRUE(mr->GetTopicName().ToString() == topic);
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(nullptr, nullptr));

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
  ASSERT_OK(client->Subscribe(GuestTenant,
                              namespace_id,
                              topic,
                              1,
                              subscription_callback,
                              receive_callback));

  // Wait for the message.
  bool result = msg_received.TimedWait(timeout);
  ASSERT_TRUE(result);
}

/**
 * Publishes 1 message. Trims message. Attempts to read
 * message and ensures that one gap is received.
 */
TEST(IntegrationTest, TrimAll) {
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
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {
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
    auto record_cb = [&] (std::unique_ptr<LogRecord> r) {
      // We should not be reading any records as they have been trimmed.
      ASSERT_TRUE(false);
    };

    auto gap_cb = [&] (const GapRecord &r) {
      // We expect a Gap, and we expect the low seqno should be our
      // previously published publish lsn.
      ASSERT_TRUE(r.from == seqno);
      num_gaps++;
      read_sem.Post();
    };

    // Create readers.
    std::vector<AsyncLogReader *> readers;
    st = storage->CreateAsyncReaders(1, record_cb, gap_cb, &readers);
    ASSERT_OK(st);

    // Attempt to read trimmed message.
    auto reader = readers.front();
    st = reader->Open(log_id, seqno, seqno);
    ASSERT_OK(st);

    // Wait on read.
    ASSERT_TRUE(read_sem.TimedWait(std::chrono::milliseconds(100)));
    ASSERT_TRUE(num_gaps == 1);

    // Delete readers
    for (auto r : readers) {
      delete r;
    }
    publish_sem.Post();
  };

  auto subscription_callback = [&] (SubscriptionStatus ss) {};
  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {};

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

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

/**
 * Publishes n messages. Trims the first. Attempts to read
 * range [0, n-1] and ensures that one gap is received and
 * n-1 records are read.
 */
TEST(IntegrationTest, TrimFirst) {
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
  auto norm_pub_cb = [&] (std::unique_ptr<ResultStatus> rs) {
    publish_sem.Post();
  };

  // Last callback trims first log and attempts to read from deleted
  // log to final log published. Gap should be received as well as
  // n-1 messges.
  auto last_pub_cb = [&] (std::unique_ptr<ResultStatus> rs) {
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
    auto record_cb = [&] (std::unique_ptr<LogRecord> r) {
      // We should not be reading any records as they have been trimmed.
      num_logs++;
      read_sem.Post();
    };

    auto gap_cb = [&] (const GapRecord &r) {
      // We expect a Gap, and we expect the low seqno should be our
      // previously published publish lsn.
      num_gaps++;
      read_sem.Post();
    };

    // Create readers.
    std::vector<AsyncLogReader *> readers;
    st = storage->CreateAsyncReaders(1, record_cb, gap_cb, &readers);
    ASSERT_OK(st);

    // Attempt to read trimmed message.
    auto reader = readers.front();
    st = reader->Open(log_id, first_seqno);
    ASSERT_OK(st);

    // Wait on n reads.
    for (int i = 0; i < num_publish; ++i) {
      ASSERT_TRUE(read_sem.TimedWait(std::chrono::milliseconds(100)));
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

  auto subscription_callback = [&] (SubscriptionStatus ss) {};
  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {};

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

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

TEST(IntegrationTest, TrimGapHandling) {
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
  const Topic topics[2] = { "TrimGapHandling1", "TrimGapHandling2" };
  const int num_messages = 10;

  auto subscription_callback = [&] (SubscriptionStatus ss) {
  };

  port::Semaphore recv_sem[2];
  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    int index = mr->GetTopicName().ToString() == topics[0] ? 0 : 1;
    recv_sem[index].Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

  // Publish messages.
  SequenceNumber seqnos[num_messages];
  for (int i = 0; i < num_messages; ++i) {
    port::Semaphore publish_sem;
    auto publish_callback = [&, i] (std::unique_ptr<ResultStatus> rs) {
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
    ASSERT_TRUE(publish_sem.TimedWait(std::chrono::seconds(1)));
  }

  // Find topic log (for trimming).
  LogID log_id;
  LogID log_id2;
  ASSERT_OK(cluster.GetLogRouter()->GetLogID(ns, topics[0], &log_id));
  ASSERT_OK(cluster.GetLogRouter()->GetLogID(ns, topics[1], &log_id2));
  ASSERT_EQ(log_id, log_id2);  // opts.single_log = true; should ensure this.

  auto test_receipt = [&] (int topic, SequenceNumber seqno, int expected) {
    // Tests that subscribing to topic@seqno results in 'expected' messages.
    client->ListenTopics(GuestTenant, {{ ns, topics[topic], true, seqno }});
    while (expected--) {
      ASSERT_TRUE(recv_sem[topic].TimedWait(std::chrono::seconds(1)));
    }
    ASSERT_TRUE(!recv_sem[topic].TimedWait(std::chrono::milliseconds(100)));
    client->ListenTopics(GuestTenant, {{ ns, topics[topic], false, seqno }});
  };

  test_receipt(0, seqnos[0], 5);
  test_receipt(1, seqnos[0], 5);

  // Trim first 6, so should be 2 of each left.
  ASSERT_OK(cluster.GetLogStorage()->Trim(log_id, seqnos[5]));
  test_receipt(0, seqnos[0], 2);
  test_receipt(1, seqnos[0], 2);

  // Check at edges of gap.
  test_receipt(0, seqnos[5], 2);
  test_receipt(1, seqnos[5], 2);
  test_receipt(0, seqnos[6], 2);
  test_receipt(1, seqnos[6], 2);
}

TEST(IntegrationTest, SequenceNumberZero) {
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

  // RocketSpeed callbacks;
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {
    publish_sem.Post();
  };

  auto subscription_callback = [&] (SubscriptionStatus ss) {
  };

  std::vector<std::string> received;
  ThreadCheck thread_check;
  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    // Messages from the same topic will always be received on the same thread.
    thread_check.Check();
    received.push_back(mr->GetContents().ToString());
    message_sem.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

  // Send some messages and wait for the acks.
  for (int i = 0; i < 3; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(GuestTenant, topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  { // Subscribe using seqno 0.
    std::vector<SubscriptionRequest> subscriptions = {
        SubscriptionRequest(ns, topic, true, 0)
    };
    client->ListenTopics(GuestTenant, subscriptions);
    ASSERT_TRUE(!message_sem.TimedWait(std::chrono::milliseconds(100)));
  }

  // Should not receive any of the last three messages.
  // Send 3 more different messages.
  for (int i = 3; i < 6; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(GuestTenant, topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
    ASSERT_TRUE(message_sem.TimedWait(timeout));
  }

  {
    std::vector<std::string> expected = {"3", "4", "5"};
    ASSERT_TRUE(received == expected);
  }

  { // Unsubscribe from previously subscribed topic.
    std::vector<SubscriptionRequest> subscriptions = {
      SubscriptionRequest(ns, topic, false, 0)
    };
    client->ListenTopics(GuestTenant, subscriptions);
  }

  // Send some messages and wait for the acks.
  for (int i = 6; i < 9; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(GuestTenant, topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  { // Subscribe using seqno 0.
    std::vector<SubscriptionRequest> subscriptions = {
        SubscriptionRequest(ns, topic, true, 0)
    };
    client->ListenTopics(GuestTenant, subscriptions);
    ASSERT_TRUE(!message_sem.TimedWait(std::chrono::milliseconds(100)));
  }

  // Send 3 more messages again.
  for (int i = 9; i < 12; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(GuestTenant, topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
    ASSERT_TRUE(message_sem.TimedWait(timeout));
  }

  { // Should not receive any of the messages sent while it unsubscribed.
    std::vector<std::string> expected = {"3", "4", "5", "9", "10", "11"};
    ASSERT_TRUE(received == expected);
  }
}

/*
 * Verify that we do not leak any threads
 */
TEST(IntegrationTest, ThreadLeaks) {
  // Setup local RocketSpeed cluster with default environment.
  Env* env = Env::Default();

  // Verify that there are no threads associated with this env
  ASSERT_EQ(env->GetNumberOfThreads(), 0);

  // Create and destroy a cluster with this env and then
  // verify that we do not have threads associated with this env
  // Create control tower,  pilot and copilot
  {
    LocalTestCluster cluster(info_log, true, true, true, "", env);
    ASSERT_GE(env->GetNumberOfThreads(), 0);
  }
  ASSERT_EQ(env->GetNumberOfThreads(), 0);

  // Create control tower and pilot
  {
    LocalTestCluster cluster(info_log, true, true, false, "", env);
    ASSERT_GE(env->GetNumberOfThreads(), 0);
  }
  ASSERT_EQ(env->GetNumberOfThreads(), 0);

  // Create control tower and copilot
  {
    LocalTestCluster cluster(info_log, true, false, true, "", env);
    ASSERT_GE(env->GetNumberOfThreads(), 0);
  }
  ASSERT_EQ(env->GetNumberOfThreads(), 0);
}

/**
 * Check that sending goodbye message removes subscriptions.
 */
TEST(IntegrationTest, UnsubscribeOnGoodbye) {
  LocalTestCluster cluster(info_log, true, true, true);
  ASSERT_OK(cluster.GetStatus());

  port::Semaphore received_data;

  // Start client loop.
  MsgLoop client(env_, EnvOptions(), 58499, 1, info_log, "client");
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mMetadata] = [](std::unique_ptr<Message>, StreamID) {};
  callbacks[MessageType::mDeliver] =
    [&] (std::unique_ptr<Message> msg, StreamID origin) {
      received_data.Post();
    };
  callbacks[MessageType::mGap] = [](std::unique_ptr<Message>, StreamID) {};
  callbacks[MessageType::mDataAck] = [] (std::unique_ptr<Message>, StreamID) {};
  client.RegisterCallbacks(callbacks);
  StreamSocket socket(client.CreateOutboundStream(
      cluster.GetCopilot()->GetClientId(), 0));
  ASSERT_OK(client.Initialize());
  env_->StartThread([&] () { client.Run(); }, "client");
  client.WaitUntilRunning();

  // Subscribe.
  NamespaceID ns = GuestNamespace;
  Topic topic = "UnsubscribeOnGoodbye";
  MessageMetadata sub(Tenant::GuestTenant,
                      MessageMetadata::MetaType::Request,
                      { TopicPair(1, topic, MetadataType::mSubscribe, ns) });
  ASSERT_OK(client.SendRequest(sub, &socket, 0));

  // Now say goodbye.
  MessageGoodbye goodbye(Tenant::GuestTenant,
                         MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);
  ASSERT_OK(client.SendRequest(goodbye, &socket, 0));
  env_->SleepForMicroseconds(100 * 1000);  // allow goodbye to process

  // Now publish to pilot.
  // We shouldn't get the message.
  MessageData publish(MessageType::mPublish,
                      Tenant::GuestTenant,
                      topic,
                      ns,
                      Slice("data"));
  ASSERT_OK(client.SendRequest(publish, &socket, 0));
  ASSERT_TRUE(!received_data.TimedWait(std::chrono::milliseconds(100)));
}

TEST(IntegrationTest, LostConnection) {
  // Tests that client can be used after it loses connection to the cloud.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.copilot.rollcall_enabled = false;
  opts.info_log = info_log;
  std::unique_ptr<LocalTestCluster> cluster(new LocalTestCluster(opts));
  ASSERT_OK(cluster->GetStatus());

  // Message read semaphore.
  port::Semaphore msg_received;

  // Message setup.
  Topic topic = "LostConnection";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";

  // RocketSpeed callbacks;
  auto publish_callback = [&](std::unique_ptr<ResultStatus> rs) {};

  auto subscription_callback = [&](SubscriptionStatus ss) {
  };

  auto receive_callback = [&](std::unique_ptr<MessageReceived> mr) {
    ASSERT_TRUE(mr->GetTopicName().ToString() == topic);
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster->GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

  // Send a message.
  ASSERT_OK(client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            topic_options,
                            Slice(data),
                            publish_callback).status);

  // Listen on a topic.
  std::vector<SubscriptionRequest> subscriptions = {
      SubscriptionRequest(namespace_id, topic, true, 1)};
  client->ListenTopics(GuestTenant, subscriptions);

  // Wait for the message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));

  // Restart the cluster, which kills connection.
  cluster.reset();
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
                            publish_callback).status);

  // Wait for message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));
}

TEST(IntegrationTest, OneMessageWithoutRollCall) {
  // This test is a duplicate of OneMessage, just with RollCall disabled.
  // The intention is just to be a quick check to ensure that things still
  // function without RollCall.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.copilot.rollcall_enabled = false;
  opts.info_log = info_log;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Message read semaphore.
  port::Semaphore msg_received;

  // Message setup.
  Topic topic = "OneMessageWithoutRollCall";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // RocketSpeed callbacks;
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {
  };

  auto subscription_callback = [&] (SubscriptionStatus ss) {
  };

  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    ASSERT_TRUE(mr->GetTopicName().ToString() == topic);
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

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
  std::vector<SubscriptionRequest> subscriptions = {
    SubscriptionRequest(namespace_id, topic, true, 1)
  };
  client->ListenTopics(GuestTenant, subscriptions);

  // Wait for the message.
  bool result = msg_received.TimedWait(timeout);
  ASSERT_TRUE(result);
}

TEST(IntegrationTest, NewControlTower) {
  // Will send a message to one control tower. Stop it, bring up a new one,
  // inform copilot of the change, then try to send another.

  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message setup.
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;

  // RocketSpeed callbacks
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {};
  auto subscription_callback = [&] (SubscriptionStatus ss) {};

  port::Semaphore msg_received;
  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

  // Send a message.
  ASSERT_OK(client->Publish(GuestTenant,
                            "NewControlTower1",
                            namespace_id,
                            topic_options,
                            "message1",
                            publish_callback).status);

  // Listen for the message.
  client->ListenTopics(GuestTenant,
    { SubscriptionRequest(namespace_id, "NewControlTower1", true, 1) });

  // Wait for the message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));

  // Stop Control Tower
  cluster.GetControlTower()->Stop();
  cluster.GetControlTowerLoop()->Stop();

  // Send another message to a different topic.
  ASSERT_OK(client->Publish(GuestTenant,
                            "NewControlTower2",
                            namespace_id,
                            topic_options,
                            "message2",
                            publish_callback).status);

  // Start new control tower (only).
  LocalTestCluster::Options new_opts;
  new_opts.info_log = info_log;
  new_opts.start_controltower = true;
  new_opts.start_copilot = false;
  new_opts.start_pilot = false;
  new_opts.controltower_port = ControlTower::DEFAULT_PORT + 1;
  LocalTestCluster new_cluster(new_opts);

  // Inform copilot of new control tower.
  std::unordered_map<uint64_t, HostId> new_towers = {
    { 0, new_cluster.GetControlTower()->GetHostId() }
  };
  ASSERT_OK(cluster.GetCopilot()->UpdateControlTowers(std::move(new_towers)));

  // Listen for the message.
  // This subscription request should be routed to the new control tower.
  client->ListenTopics(GuestTenant,
    { SubscriptionRequest(namespace_id, "NewControlTower2", true, 1) });

  // Wait for the message.
  ASSERT_TRUE(msg_received.TimedWait(timeout));

  // Send a message on the old topic again.
  ASSERT_OK(client->Publish(GuestTenant,
                            "NewControlTower1",
                            namespace_id,
                            topic_options,
                            "message3",
                            publish_callback).status);

  // The copilot should have re-subscribed us to the new control tower.
  ASSERT_TRUE(msg_received.TimedWait(timeout));
}

TEST(IntegrationTest, SubscriptionStorage) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  std::string file_path =
      test::TmpDir() + "/SubscriptionStorage-file_storage_data";
  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  ASSERT_OK(SubscriptionStorage::File(options.env,
                                      options.info_log,
                                      file_path,
                                      &options.storage));
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(nullptr, nullptr));

  // Create some subscriptions.
  std::vector<SubscriptionParameters> expected = {
      SubscriptionParameters(Tenant::GuestTenant, GuestNamespace,
                             "SubscriptionStorage_0", 123),
      SubscriptionParameters(Tenant::GuestTenant, GuestNamespace,
                             "SubscriptionStorage_1", 0), };

  for (const auto& params : expected) {
    ASSERT_OK(client->Subscribe(params));
  }
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
  std::sort(restored.begin(), restored.end(),
            [](SubscriptionParameters a, SubscriptionParameters b) {
    return a.topic_name < b.topic_name;
  });
  ASSERT_TRUE(expected == restored);
}

TEST(IntegrationTest, SubscriptionManagement) {
  // Tests various sub/unsub combinations with multiple clients on the same
  // copilot on the same topic.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.single_log = true;   // for testing topic tailer
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed client.
  enum { kNumClients = 2 };
  ClientOptions options[kNumClients];
  std::vector<std::string> inbox[kNumClients];
  std::mutex inbox_lock[kNumClients];
  port::Semaphore checkpoint[kNumClients];
  std::unique_ptr<Client> client[kNumClients];

  for (int i = 0; i < kNumClients; ++i) {
    options[i].config = cluster.GetConfiguration();
    options[i].info_log = info_log;
    ASSERT_OK(Client::Create(std::move(options[i]), &client[i]));
    ASSERT_OK(client[i]->Start(nullptr,
      [&, i] (std::unique_ptr<MessageReceived> mr) {
        {
          std::lock_guard<std::mutex> lock(inbox_lock[i]);
          inbox[i].push_back(mr->GetContents().ToString());
        }
        checkpoint[i].Post();
      }));
  }

  // Publish a message and wait.
  auto pub = [&] (int c, Topic topic, Slice data) {
    port::Semaphore sem;
    SequenceNumber pub_seqno;
    client[c]->Publish(GuestTenant, topic, GuestNamespace, TopicOptions(), data,
      [&sem, &pub_seqno] (std::unique_ptr<ResultStatus> rs) {
        ASSERT_OK(rs->GetStatus());
        pub_seqno = rs->GetSequenceNumber();
        sem.Post();
      });
    ASSERT_TRUE(sem.TimedWait(timeout));
    return pub_seqno;
  };

  // Subscribe to a topic.
  auto sub = [&] (int c, Topic topic, SequenceNumber seqno) {
    if (seqno == 0) {
      env_->SleepForMicroseconds(100000);  // allow latest seqno to propagate.
    }
    client[c]->ListenTopics(GuestTenant,
      {{ GuestNamespace, topic, true, seqno }});
    if (seqno == 0) {
      env_->SleepForMicroseconds(100000);  // allow latest seqno to propagate.
    }
  };

  // Unsubscribe from a topic.
  auto unsub = [&] (int c, Topic topic) {
    client[c]->ListenTopics(GuestTenant,
      {{ GuestNamespace, topic, false, 1 }});
  };

  // Receive messages.
  auto recv = [&] (int c, std::vector<std::string> expected) {
    for (size_t i = 0; i < expected.size(); ++i) {
      // Wait for expected messages.
      if (!checkpoint[c].TimedWait(timeout)) {
        fprintf(stderr, "Client %d failed to receive msg %zu/%zu\n",
          c, i, expected.size());
        ASSERT_TRUE(false);
      }
    }
    // No more.
    ASSERT_TRUE(!checkpoint[c].TimedWait(std::chrono::milliseconds(10)));
    std::lock_guard<std::mutex> lock(inbox_lock[c]);
    if (inbox[c] != expected) {
      fprintf(stderr, "Inbox %d contains:\n", c);
      for (auto s : inbox[c]) {
        fprintf(stderr, "%s\n", s.c_str());
      }
      fprintf(stderr, "\nExpected:\n");
      for (auto s : expected) {
        fprintf(stderr, "%s\n", s.c_str());
      }
    }
    ASSERT_TRUE(inbox[c] == expected);
    inbox[c].clear();
  };

  // Empty inboxes.
  recv(0, {});
  recv(1, {});

  // Subscribe client 0 to start, publish 3, receive 3.
  sub(0, "a", 1);
  pub(0, "a", "a0");
  SequenceNumber a1 = pub(0, "a", "a1");
  pub(0, "a", "a2");
  recv(0, {"a0", "a1", "a2"});

  // Subscribe client 1 at message 2/3, receive messages 2 + 3
  sub(1, "a", a1);
  recv(0, {});
  recv(1, {"a1", "a2"});

  // Publish one more, both receive.
  pub(0, "a", "a3");
  recv(0, {"a3"});
  recv(1, {"a3"});

  // Unsubscribe client 0, publish, only client 1 should receive.
  unsub(0, "a");
  pub(0, "a", "a4");
  recv(0, {});
  recv(1, {"a4"});

  // Unsubscribe client 1, publish, neither should receive.
  unsub(1, "a");
  pub(0, "a", "a5");
  recv(0, {});
  recv(1, {});

  // Subscribe both at tail, publish, both should receive.
  sub(0, "a", 0);
  sub(1, "a", 0);
  pub(0, "a", "a6");
  recv(0, {"a6"});
  recv(1, {"a6"});

  // Unsubscribe client 1, publish, resubscribe at tail, publish again.
  // Client 0 should receive both, while client 1 only last one.
  unsub(1, "a");
  pub(0, "a", "a7");
  sub(1, "a", 0);
  pub(0, "a", "a8");
  recv(0, {"a7", "a8"});
  recv(1, {"a8"});

  // Test topic tailer handling of intertwined topic subscriptions.
  // Subscribe both to b and publish b0.
  sub(0, "b", 1);
  sub(1, "b", 1);
  pub(0, "b", "b0");
  recv(0, {"b0"});
  recv(1, {"b0"});

  // Unsubscribe b then advance the log with topic c.
  unsub(1, "b");
  pub(0, "c", "c0");
  SequenceNumber c1 = pub(0, "c", "c1");
  pub(0, "c", "c2");

  // Subsribe client1 to c1.
  // This must cause log tailer to rewind.
  sub(1, "c", c1);
  recv(1, {"c1", "c2"});
  unsub(1, "c");

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
  sub(0, "f", 1);
  SequenceNumber f0 = pub(0, "f", "f0");
  pub(0, "!f", "!f0");
  recv(0, {"f0"});
  sub(1, "f", f0 - 1);
  recv(1, {"f0"});
  pub(0, "f", "f1");
  recv(0, {"f1"});
  recv(1, {"f1"});

  // Topic rewind on empty topic.
  sub(0, "g", 1); // will go to tail
  sub(1, "g", f0);
  pub(0, "g", "g0");
  recv(0, {"g0"});
  recv(1, {"g0"});
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
