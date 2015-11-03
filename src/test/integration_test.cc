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

class IntegrationTest {
 public:
  std::chrono::seconds timeout;

  IntegrationTest()
      : timeout(5)
      , env_(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env_,
                                 "IntegrationTest",
                                 &info_log));
  }

  int64_t GetNumOpenLogs(ControlTower* ct) const {
    return
      ct->GetStatisticsSync().GetCounterValue("tower.log_tailer.open_logs");
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

  auto subscription_callback = [&](const SubscriptionStatus& ss) {
    ASSERT_TRUE(ss.GetTopicName() == topic);
    ASSERT_TRUE(ss.GetNamespace() == namespace_id);
  };

  auto receive_callback = [&] (std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

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
    auto record_cb = [&] (LogRecord& r) {
      // We should not be reading any records as they have been trimmed.
      ASSERT_TRUE(false);
      return true;
    };

    auto gap_cb = [&] (const GapRecord &r) {
      // We expect a Gap, and we expect the low seqno should be our
      // previously published publish lsn.
      ASSERT_TRUE(r.from == seqno);
      num_gaps++;
      read_sem.Post();
      return true;
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

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

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
 * Publishes N messages and trims the first K from a sender client.
 * After this a receiver client validates DataLoss callback is correctly called.
 */
TEST(IntegrationTest, TestDataLossCallback) {
  const size_t kTotalMessagesToSend = 10;
  const size_t kTotalMessagesToTrim = 3;

  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message setup.
  Topic topic = "TestDataLossCallback_Topic";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "TestDataLossCallback_Message";
  GUIDGenerator msgid_generator;

  // PART 1 - Create a sender client, send N messages and trim the first K.

  // Create RocketSpeed sender client.
  std::unique_ptr<ClientImpl> sender;
  cluster.CreateClient(&sender, true);

  port::Semaphore publish_sem;
  SequenceNumber first_seqno = -1;
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {

    // Do the trimming magic only when all records have been published.
    static size_t call_count = 0;
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
    ASSERT_OK(ps.status);
    ASSERT_TRUE(ps.msgid == message_id);
  }

  // Wait on the last message to be published.
  ASSERT_TRUE(publish_sem.TimedWait(timeout));

  // PART 2 - Create a receiver client, sync to the first seqno and
  // confirm received and lost messages.

  // Create RocketSpeed receiver client.
  std::unique_ptr<ClientImpl> receiver;
  cluster.CreateClient(&receiver, true);

  // Receiver flow control semaphores.
  port::Semaphore receive_sem, data_loss_sem;

  auto receive_callback = [&] (std::unique_ptr<MessageReceived>& mr) {
    static size_t received_messages = 0;
    if (++received_messages == kTotalMessagesToSend - kTotalMessagesToTrim) {
      receive_sem.Post();
    }
  };

  auto data_loss_callback = [&] (std::unique_ptr<DataLossInfo>& msg) {
    ASSERT_EQ(DataLossType::kRetention, msg->GetLossType());
    ASSERT_EQ(first_seqno, msg->GetFirstSequenceNumber());
    ASSERT_EQ(first_seqno + kTotalMessagesToTrim - 1,
              msg->GetLastSequenceNumber());
    data_loss_sem.Post();
  };

  SubscriptionParameters sub_params = SubscriptionParameters(GuestTenant,
                                                             namespace_id,
                                                             topic,
                                                             first_seqno);
  auto rec_handle = receiver->Subscribe(sub_params,
                                        receive_callback,
                                        nullptr,
                                        data_loss_callback);
  ASSERT_TRUE(rec_handle);
  ASSERT_TRUE(receive_sem.TimedWait(timeout));
  ASSERT_TRUE(data_loss_sem.TimedWait(timeout));
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
  // n-1 messages.
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
    auto record_cb = [&] (LogRecord& r) {
      // We should not be reading any records as they have been trimmed.
      num_logs++;
      read_sem.Post();
      return true;
    };

    auto gap_cb = [&] (const GapRecord &r) {
      // We expect a Gap, and we expect the low seqno should be our
      // previously published publish lsn.
      num_gaps++;
      read_sem.Post();
      return true;
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

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

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

  port::Semaphore recv_sem[2];

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

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

  auto test_receipt = [&](int topic, SequenceNumber seqno, int expected) {
    auto receive_callback = [&recv_sem, topic](
        std::unique_ptr<MessageReceived>&) { recv_sem[topic].Post(); };
    // Tests that subscribing to topic@seqno results in 'expected' messages.
    auto handle = client->Subscribe(GuestTenant,
                                    ns,
                                    topics[topic],
                                    seqno,
                                    receive_callback);
    ASSERT_TRUE(handle);
    while (expected--) {
      ASSERT_TRUE(recv_sem[topic].TimedWait(std::chrono::seconds(1)));
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

  std::vector<std::string> received;
  ThreadCheck thread_check;
  auto receive_callback = [&] (std::unique_ptr<MessageReceived>& mr) {
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
  client->SetDefaultCallbacks(nullptr, receive_callback);

  // Send some messages and wait for the acks.
  for (int i = 0; i < 3; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(GuestTenant, topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  // Subscribe using seqno 0.
  auto handle = client->Subscribe(GuestTenant, ns, topic, 0);
  ASSERT_TRUE(handle);
  ASSERT_TRUE(!message_sem.TimedWait(std::chrono::milliseconds(100)));

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

  // Verify that we have a client on the Copilot.
  auto num_clients = cluster.GetCopilot()->GetMsgLoop()->GetNumClientsSync();

  // Unsubscribe from previously subscribed topic.
  ASSERT_OK(client->Unsubscribe(std::move(handle)));

  // Send some messages and wait for the acks.
  for (int i = 6; i < 9; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(GuestTenant, topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  // Number of streams on the Copilot's MsgLoop should drop by one.
  ASSERT_EQ(num_clients - 1,
            cluster.GetCopilot()->GetMsgLoop()->GetNumClientsSync());

  // Subscribe using seqno 0.
  ASSERT_TRUE(client->Subscribe(GuestTenant, ns, topic, 0));
  ASSERT_TRUE(!message_sem.TimedWait(std::chrono::milliseconds(100)));

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
    ASSERT_EQ(received, expected);
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
  MsgLoop client(env_, EnvOptions(), 0, 1, info_log, "client");
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mDeliver] = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    received_data.Post();
  };
  callbacks[MessageType::mGap] = [](
      Flow* flow, std::unique_ptr<Message>, StreamID) {};
  callbacks[MessageType::mDataAck] = [](
      Flow* flow, std::unique_ptr<Message>, StreamID) {};
  client.RegisterCallbacks(callbacks);
  StreamSocket socket(client.CreateOutboundStream(
      cluster.GetCopilot()->GetHostId(), 0));
  ASSERT_OK(client.Initialize());
  auto tid = env_->StartThread([&] () { client.Run(); }, "client");
  client.WaitUntilRunning();

  // Subscribe.
  NamespaceID ns = GuestNamespace;
  Topic topic = "UnsubscribeOnGoodbye";
  MessageSubscribe sub(Tenant::GuestTenant, ns, topic, 1, 1);
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

  client.Stop();
  env_->WaitForJoin(tid);
}

TEST(IntegrationTest, LostConnection) {
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
  options.config = cluster->GetConfiguration();
  options.info_log = info_log;
  // We do this to save some time waiting for client to reconnect.
  options.timer_period = std::chrono::milliseconds(1);
  options.backoff_distribution = [](ClientRNG*) { return 0.0; };
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
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

  // Message setup.
  Topic topic = "OneMessageWithoutRollCall";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

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
  auto handle = client->Subscribe(GuestTenant, namespace_id, topic, 1,
    [&] (std::unique_ptr<MessageReceived>& mr) {
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
  handle = client->Subscribe(GuestTenant, namespace_id, topic, 0,
    [&] (std::unique_ptr<MessageReceived>& mr) {
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
  port::Semaphore msg_received;
  auto receive_callback = [&] (std::unique_ptr<MessageReceived>& mr) {
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
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
  LocalTestCluster new_cluster(new_opts);
  ASSERT_OK(new_cluster.GetStatus());

  // Inform copilot of new control tower.
  std::unordered_map<uint64_t, HostId> new_towers = {
    { 0, new_cluster.GetControlTower()->GetHostId() }
  };
  auto new_router =
      std::make_shared<RendezvousHashTowerRouter>(new_towers, 1);
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
  MessageReceivedMock(SubscriptionID sub_id,
                      SequenceNumber seqno,
                      Slice payload)
      : sub_id_(sub_id)
      , seqno_(seqno)
      , payload_(std::move(payload)) {}

  SubscriptionHandle GetSubscriptionHandle() const override { return sub_id_; }

  SequenceNumber GetSequenceNumber() const override { return seqno_; }

  Slice GetContents() const override { return payload_; }

 private:
  SubscriptionID sub_id_;
  SequenceNumber seqno_;
  Slice payload_;
};

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

  // Create some subscriptions.
  std::vector<SubscriptionParameters> expected = {
      SubscriptionParameters(Tenant::GuestTenant, GuestNamespace,
                             "SubscriptionStorage_0", 123),
      SubscriptionParameters(Tenant::GuestTenant, GuestNamespace,
                             "SubscriptionStorage_1", 0), };

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
  opts.tower.max_subscription_lag = 3;
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
    client[i]->SetDefaultCallbacks(nullptr,
      [&, i] (std::unique_ptr<MessageReceived>& mr) {
        {
          std::lock_guard<std::mutex> lock(inbox_lock[i]);
          inbox[i].push_back(mr->GetContents().ToString());
        }
        checkpoint[i].Post();
      });
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
  auto sub = [&](int c, Topic topic, SequenceNumber seqno)->SubscriptionHandle {
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
  auto recv = [&] (int c, std::vector<std::string> expected) {
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
  sub(0, "g", a0); // will go to tail
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

TEST(IntegrationTest, LogAvailability) {
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
    ct_cluster[i].reset(new LocalTestCluster(ct_opts[i]));
    ASSERT_OK(ct_cluster[i]->GetStatus());
  }

  // Inform copilot of origin control tower, and second control tower
  // (but not third -- we'll use that later).
  std::unordered_map<uint64_t, HostId> new_towers = {
      { 0, cluster.GetControlTower()->GetHostId() },
      { 1, ct_cluster[0]->GetControlTower()->GetHostId() }
  };
  auto new_router =
      std::make_shared<RendezvousHashTowerRouter>(new_towers, 2);
  ASSERT_OK(cluster.GetCopilot()->UpdateTowerRouter(std::move(new_router)));

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));


  // Listen on many topics (to ensure at least one goes to each tower).
  port::Semaphore msg_received;
  std::vector<SubscriptionHandle> subscriptions;
  enum { kNumTopics = 10 };
  for (int i = 0; i < kNumTopics; ++i) {
    auto handle =
      client->Subscribe(GuestTenant,
                        GuestNamespace,
                        "LogAvailability" + std::to_string(i),
                        0,
                        [&] (std::unique_ptr<MessageReceived>& mr) {
                          msg_received.Post();
                        });
    subscriptions.push_back(handle);
  }

  // The copilot should be subscribed to all topics on BOTH control towers.
  env_->SleepForMicroseconds(200000);

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
    { 2, ct_cluster[1]->GetControlTower()->GetHostId() },
  };
  new_router = std::make_shared<RendezvousHashTowerRouter>(new_towers, 1);
  ASSERT_OK(cluster.GetCopilot()->UpdateTowerRouter(std::move(new_router)));
  env_->SleepForMicroseconds(200000);

  // Resend subscriptions.
  port::Semaphore msg_received2;
  for (int i = 0; i < kNumTopics; ++i) {
    auto handle =
      client->Subscribe(GuestTenant,
                        GuestNamespace,
                        "LogAvailability" + std::to_string(i),
                        0,
                        [&] (std::unique_ptr<MessageReceived>& mr) {
                          msg_received2.Post();
                        });
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

TEST(IntegrationTest, TowerDeathReconnect) {
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
  auto receive_callback = [&] (std::unique_ptr<MessageReceived>& mr) {
    msg_received.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  client->SetDefaultCallbacks(nullptr, receive_callback);

  // Listen for messages.
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_TRUE(
      client->Subscribe(GuestTenant,
                        GuestNamespace,
                        "TowerDeathReconnect" + std::to_string(t),
                        0));
  }

  env_->SleepForMicroseconds(1000000);

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

  // Let the copilot fail to reconnect for a few ticks (to check that code path)
  env_->SleepForMicroseconds(3 * int(opts.copilot.timer_interval_micros));

  auto stats1 = cluster.GetCopilot()->GetStatisticsSync();
  ASSERT_EQ(kNumTopics, stats1.GetCounterValue("copilot.orphaned_topics"));

  // Start new control tower (only) with same host:port.
  LocalTestCluster::Options new_opts;
  new_opts.info_log = info_log;
  new_opts.start_controltower = true;
  new_opts.start_copilot = false;
  new_opts.start_pilot = false;
  new_opts.controltower_port = original_port;

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

TEST(IntegrationTest, CopilotDeath) {
  const size_t kNumTopics = 10;

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.start_controltower = true;
  opts.start_pilot = false;
  opts.start_copilot = true;
  opts.copilot.timer_interval_micros = 100000;
  opts.copilot.resubscriptions_per_second = kNumTopics;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Separate cluster for pilot (since we need to stop the copilot
  // independently from the pilot).
  LocalTestCluster pilot_cluster(info_log, false, false, true);
  ASSERT_OK(pilot_cluster.GetStatus());

  // Create RocketSpeed clients.
  ClientOptions sub_options;
  sub_options.config = cluster.GetConfiguration();
  sub_options.info_log = info_log;
  std::unique_ptr<Client> sub_client;
  ASSERT_OK(Client::Create(std::move(sub_options), &sub_client));

  ClientOptions pub_options;
  pub_options.config = pilot_cluster.GetConfiguration();
  pub_options.info_log = info_log;
  std::unique_ptr<Client> pub_client;
  ASSERT_OK(Client::Create(std::move(pub_options), &pub_client));

  // Listen for messages.
  port::Semaphore msg_received;
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_TRUE(
      sub_client->Subscribe(GuestTenant,
                            GuestNamespace,
                            "CopilotDeath" + std::to_string(t),
                            0,
                            [&] (std::unique_ptr<MessageReceived>& mr) {
                              msg_received.Post();
                            }));
  }

  env_->SleepForMicroseconds(100000);

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

TEST(IntegrationTest, ControlTowerCache) {
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
  port::Semaphore msg_received, msg_received2,
                  msg_received3, msg_received4, msg_received5;

  // Message setup.
  Topic topic = "ControlTowerCache";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // RocketSpeed callbacks;
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {
    msg_seqno = rs->GetSequenceNumber();
    msg_written.Post();
  };

  auto subscription_callback = [&](const SubscriptionStatus& ss) {
    ASSERT_TRUE(ss.GetTopicName() == topic);
    ASSERT_TRUE(ss.GetNamespace() == namespace_id);
  };

  auto receive_callback = [&] (std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received.Post();
  };

  auto receive_callback2 = [&] (std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received2.Post();
  };

  auto receive_callback3 = [&] (std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received3.Post();
  };

  auto receive_callback4 = [&] (std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received4.Post();
  };

  auto receive_callback5 = [&] (std::unique_ptr<MessageReceived>& mr) {
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    msg_received5.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

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
  auto cache_misses =
    stats1.GetCounterValue("tower.data_cache.cache_misses");
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
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_GT(usage, 0);

  // clear cache
  cluster.GetControlTower()->SetInfoSync({"cache", "clear"});

  // check that we do not have any cache usage
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
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
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_GT(usage, 0);

  // Set the capacity to zero. This should disable the cache and
  // purge existing entries.
  new_capacity_str =
    cluster.GetControlTower()->SetInfoSync({"cache", "capacity", "0"});
  ASSERT_EQ(new_capacity_str, "");

  // verify that cache is purged
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_EQ(usage, 0);

  // verify that cache is disabled
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "capacity"});
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
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_EQ(usage, 0);

  // remember cache statistics
  auto stats3 = cluster.GetControlTower()->GetStatisticsSync();
  auto cached_hits3 =
    stats3.GetCounterValue("tower.topic_tailer.records_served_from_cache");

  // set a  1 MB cache capacity
  size_t set_capacity = 1024000;
  new_capacity_str =
    cluster.GetControlTower()->SetInfoSync({"cache", "capacity",
                                            std::to_string(set_capacity)});
  ASSERT_EQ(new_capacity_str, "");
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "capacity"});
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
  usage_str =
    cluster.GetControlTower()->GetInfoSync({"cache", "usage"});
  usage = std::stol(usage_str);
  ASSERT_GT(usage, 0);

  // Verify that there were no additional cache hits
  auto stats4 = cluster.GetControlTower()->GetStatisticsSync();
  auto cached_hits4 =
    stats4.GetCounterValue("tower.topic_tailer.records_served_from_cache");
  ASSERT_EQ(cached_hits4, cached_hits3);
}

TEST(IntegrationTest, ReadingFromCache) {
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
      opts.tower.topic_tailer.cache_size = 0;    // cache disabled
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
    ClientOptions options;
    options.config = cluster.GetConfiguration();
    options.info_log = info_log;
    std::unique_ptr<Client> client;
    ASSERT_OK(Client::Create(std::move(options), &client));

    // Send a message.
    port::Semaphore msg_written;
    SequenceNumber msg_seqno = 0;
    auto ps = client->Publish(GuestTenant,
                              topic,
                              namespace_id,
                              topic_options,
                              Slice(data1),
                              [&] (std::unique_ptr<ResultStatus> rs) {
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
    auto handle = client->Subscribe(GuestTenant, namespace_id, topic, msg_seqno,
    [&] (std::unique_ptr<MessageReceived>& mr) {
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
    handle = client->Subscribe(GuestTenant, namespace_id, topic, 0,
      [&] (std::unique_ptr<MessageReceived>& mr) {
        ASSERT_TRUE((mr->GetContents().ToString() == data2));
        ASSERT_EQ(lastReceived + 1 , mr->GetSequenceNumber());
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
    handle = client->Subscribe(GuestTenant, namespace_id, topic, msg_seqno,
      [&] (std::unique_ptr<MessageReceived>& mr) {
        msg_received3.Post();
      });
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
TEST(IntegrationTest, TowerRebalance) {
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
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

  // Listen on many topics (to ensure at least one goes to each tower).
  enum { kNumTopics = 40 };
  for (int i = 0; i < kNumTopics; ++i) {
    client->Subscribe(GuestTenant,
                      GuestNamespace,
                      "TowerRebalance" + std::to_string(i),
                      0,
                      [] (std::unique_ptr<MessageReceived>&) {});
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
    { 0, cluster.GetControlTower()->GetHostId() },
    { 1, ct_cluster[0]->GetControlTower()->GetHostId() },
    { 2, ct_cluster[1]->GetControlTower()->GetHostId() },
  };
  auto new_router =
      std::make_shared<RendezvousHashTowerRouter>(new_towers, 1);
  ASSERT_OK(cluster.GetCopilot()->UpdateTowerRouter(std::move(new_router)));
  env_->SleepForMicroseconds(2000000);

  // Now all should have logs open, and first should have fewer logs open.
  ASSERT_NE(GetNumOpenLogs(cluster.GetControlTower()), 0);
  ASSERT_NE(GetNumOpenLogs(ct_cluster[0]->GetControlTower()), 0);
  ASSERT_NE(GetNumOpenLogs(ct_cluster[1]->GetControlTower()), 0);
  ASSERT_LT(GetNumOpenLogs(cluster.GetControlTower()), initial_logs_open);
}
#endif

TEST(IntegrationTest, InvalidSubscription) {
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
    [&] (std::unique_ptr<MessageReceived>& mr) {
      ASSERT_TRUE(false);
    },
    [&](const SubscriptionStatus& ss) {
      ASSERT_EQ(ss.GetNamespace(), InvalidNamespace);
      ASSERT_EQ(ss.GetTopicName(), "InvalidSubscription");
      ASSERT_EQ(ss.GetSequenceNumber(), 1);
      ASSERT_TRUE(!ss.IsSubscribed());
      ASSERT_TRUE(!ss.GetStatus().ok());
      sem.Post();
    }));
  ASSERT_TRUE(sem.TimedWait(timeout));
}

TEST(IntegrationTest, ReaderRestarts) {
  // Test that messages are still received with frequent reader restarts.
  const size_t kNumTopics = 10;
  const size_t kNumMessages = 1000;
  const std::chrono::milliseconds kMessageDelay(1);

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.tower.timer_interval = std::chrono::microseconds(10000); // 10ms
  opts.tower.topic_tailer.min_reader_restart_duration =
    std::chrono::milliseconds(20);
  opts.tower.topic_tailer.max_reader_restart_duration =
    std::chrono::milliseconds(40);
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

  // Subscribe to all topics.
  port::Semaphore msg_received;
  for (size_t t = 0; t < kNumTopics; ++t) {
    ASSERT_TRUE(
      client->Subscribe(GuestTenant,
                        GuestNamespace,
                        "ReaderRestarts" + std::to_string(t),
                        0,
                        [&] (std::unique_ptr<MessageReceived>& mr) {
                          msg_received.Post();
                        }));
  }
  env_->SleepForMicroseconds(100000);

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
  auto restarts = stats.GetCounterValue("tower.topic_tailer.reader_restarts");
  size_t min_expected = kNumTopics * kNumMessages * kMessageDelay /
    opts.tower.topic_tailer.max_reader_restart_duration;
  size_t max_expected = kNumTopics * kNumMessages * kMessageDelay /
    opts.tower.topic_tailer.min_reader_restart_duration;
  ASSERT_GE(restarts, min_expected - 1);  // - 1 for timer tolerance
  ASSERT_LE(restarts, max_expected + 1);  // + 1 for timer tolerance
}

TEST(IntegrationTest, VirtualReaderMerge) {
  // Tests the virtual reader merge path.

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.single_log = true;
  opts.tower.readers_per_room = 2;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

  // Publish a message to find the current seqno for the topic.
  SequenceNumber seqno;
  port::Semaphore pub_sem;
  ASSERT_OK(client->Publish(GuestTenant,
                            "VirtualReaderMerge",
                            GuestNamespace,
                            TopicOptions(),
                            "test",
                            [&] (std::unique_ptr<ResultStatus> rs) {
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
    sub[i] = client->Subscribe(GuestTenant,
                               GuestNamespace,
                               "VirtualReaderMerge" + std::to_string(i),
                               seqno + (2 - i) * 100,
                               [&, i] (std::unique_ptr<MessageReceived>& mr) {
                                 recv[i].Post();
                               });
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

TEST(IntegrationTest, SmallRoomQueues) {
  // Tests control tower with room queue size 1.
  const size_t kNumMessages = 100;

  // Setup local RocketSpeed cluster.
  LocalTestCluster::Options opts;
  opts.info_log = info_log;
  opts.tower.room_to_client_queue_size = 1;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed sender client.
  std::unique_ptr<Client> client;
  cluster.CreateClient(&client);

  // Publish a message to find the current seqno for the topic.
  for (size_t i = 0; i < kNumMessages; ++i) {
    ASSERT_OK(client->Publish(GuestTenant,
                              "SmallRoomQueues",
                              GuestNamespace,
                              TopicOptions(),
                              "#" + std::to_string(i),
                              [] (std::unique_ptr<ResultStatus> rs) {
                                ASSERT_OK(rs->GetStatus());
                              }).status);
  }
  port::Semaphore sem;
  client->Subscribe(GuestTenant,
                    GuestNamespace,
                    "SmallRoomQueues",
                    1,
                    [&] (std::unique_ptr<MessageReceived>& mr) {
                      sem.Post();
                    });
  for (size_t i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem.TimedWait(timeout));
  }

  auto stats = cluster.GetControlTower()->GetStatisticsSync();
  auto applied = stats.GetCounterValue(
    "tower.topic_tailer.flow_control.backpressure_applied");
  auto lifted = stats.GetCounterValue(
    "tower.topic_tailer.flow_control.backpressure_lifted");
  ASSERT_GT(applied, 0);  // ensure that backpressure was applied
  ASSERT_GE(applied, lifted);  // ensure that it was lifted as often as applied
  ASSERT_LE(applied, lifted + 1);  // could be 1 less if not lifted yet.
}

TEST(IntegrationTest, CacheReentrance) {
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

  auto get_topic_tailer_stats = [&] () {
    std::string tt_names[] = {
      "log_records_with_subscriptions",
      "records_served_from_cache",
      "reader_restarts",
      "reader_merges",
      "cache_reentries"
    };
    std::string lt_names[] = {
      "readers_started",
      "readers_restarted",
      "readers_stopped",
    };
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
                              [&, i] (std::unique_ptr<ResultStatus> rs) {
                                msg_seqnos[i] = rs->GetSequenceNumber();
                                pub_sem.Post();
                              });
    ASSERT_TRUE(pub_sem.TimedWait(timeout));
  }

  // Read half the messages (into the cache).
  const size_t kNumInCache = 4;
  port::Semaphore sub_sem;
  auto handle1 = client->Subscribe(GuestTenant,
                                   GuestNamespace,
                                   "CacheReentrance1",
                                   msg_seqnos[kNumMessages - kNumInCache],
                                   [&] (std::unique_ptr<MessageReceived>& mr) {
                                     sub_sem.Post();
                                   });
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
  ASSERT_EQ(stats1["reader_restarts"], 0);
  ASSERT_EQ(stats1["reader_merges"], 0);
  ASSERT_EQ(stats1["cache_reentries"], 0);
  ASSERT_EQ(stats1["readers_started"], 1);
  ASSERT_EQ(stats1["readers_restarted"], 0);
  ASSERT_EQ(stats1["readers_stopped"], 1);

  // Now subscribe to the second message.
  // Some records should come from backlog, but some should come from cache.
  // The new reader should have restarted at the tail.
  client->Subscribe(GuestTenant,
                    GuestNamespace,
                    "CacheReentrance1",
                    msg_seqnos[1],
                    [&] (std::unique_ptr<MessageReceived>& mr) {
                      sub_sem.Post();
                    });
  for (size_t i = 0; i < kNumMessages - 1; ++i) {
    ASSERT_TRUE(sub_sem.TimedWait(timeout));
  }

  auto stats2 = get_topic_tailer_stats();
  ASSERT_EQ(stats2["log_records_with_subscriptions"], kNumMessages - 1);
  ASSERT_EQ(stats2["records_served_from_cache"], kNumInCache);
  ASSERT_EQ(stats2["reader_restarts"], 1);
  ASSERT_EQ(stats2["reader_merges"], 0);
  ASSERT_EQ(stats2["cache_reentries"], 1);
  ASSERT_EQ(stats2["readers_started"], 2);
  ASSERT_EQ(stats2["readers_restarted"], 1);
  ASSERT_EQ(stats2["readers_stopped"], 1);

  // Finally, without closing existing subscription, start a new subscription
  // on a different topic (but same log, due to single_log flag) at the first
  // message. This should open a new reader, which will read one message
  // from backlog, re-enter the cache, then merge with existing reader without
  // restarting.
  client->Subscribe(GuestTenant,
                    GuestNamespace,
                    "CacheReentrance2",
                    msg_seqnos[0],
                    [&] (std::unique_ptr<MessageReceived>& mr) {
                      sub_sem.Post();
                    });
  env_->SleepForMicroseconds(100000);

  auto stats3 = get_topic_tailer_stats();
  ASSERT_EQ(stats3["log_records_with_subscriptions"], kNumMessages - 1);
  ASSERT_EQ(stats3["records_served_from_cache"], kNumInCache);
  ASSERT_EQ(stats3["reader_restarts"], 1);
  ASSERT_EQ(stats3["reader_merges"], 1);
  ASSERT_EQ(stats3["cache_reentries"], 2);
  ASSERT_EQ(stats3["readers_started"], 3);
  ASSERT_EQ(stats3["readers_restarted"], 1);
  ASSERT_EQ(stats3["readers_stopped"], 2);
}

TEST(IntegrationTest, CopilotTailSubscribeFast) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed sender client.
  std::unique_ptr<Client> client;
  cluster.CreateClient(&client);

  // First subscribe should not be a fast subscription.
  client->Subscribe(GuestTenant, GuestNamespace,
    "CopilotTailSubscribeFast1", 0, nullptr);
  env_->SleepForMicroseconds(100000);
  auto stats = cluster.GetCopilot()->GetStatisticsSync();
  auto fast_subs = stats.GetCounterValue("copilot.tail_subscribe_fast_path");
  ASSERT_EQ(fast_subs, 0);

  // Second subscribe should be a fast subscription.
  client->Subscribe(GuestTenant, GuestNamespace,
    "CopilotTailSubscribeFast1", 0, nullptr);
  env_->SleepForMicroseconds(100000);
  stats = cluster.GetCopilot()->GetStatisticsSync();
  fast_subs = stats.GetCounterValue("copilot.tail_subscribe_fast_path");
  ASSERT_EQ(fast_subs, 1);


  // Different topic now.
  // First subscribe should not be a fast subscription.
  client->Subscribe(GuestTenant, GuestNamespace,
    "CopilotTailSubscribeFast2", 1, nullptr);
  env_->SleepForMicroseconds(100000);
  stats = cluster.GetCopilot()->GetStatisticsSync();
  fast_subs = stats.GetCounterValue("copilot.tail_subscribe_fast_path");
  ASSERT_EQ(fast_subs, 1);

  // Second subscribe should not be a fast subscription since we don't have
  // tail subscription.
  // Note: this could change with better tail subscription logic.
  client->Subscribe(GuestTenant, GuestNamespace,
    "CopilotTailSubscribeFast2", 0, nullptr);
  env_->SleepForMicroseconds(100000);
  stats = cluster.GetCopilot()->GetStatisticsSync();
  fast_subs = stats.GetCounterValue("copilot.tail_subscribe_fast_path");
  ASSERT_EQ(fast_subs, 1);

  // Third subscribe should be a fast subscription.
  // Note: this could change with better tail subscription logic.
  client->Subscribe(GuestTenant, GuestNamespace,
    "CopilotTailSubscribeFast2", 0, nullptr);
  env_->SleepForMicroseconds(100000);
  stats = cluster.GetCopilot()->GetStatisticsSync();
  fast_subs = stats.GetCounterValue("copilot.tail_subscribe_fast_path");
  ASSERT_EQ(fast_subs, 2);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
