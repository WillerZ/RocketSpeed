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
  Topic topic = "test_topic";
  NamespaceID namespace_id = GuestNamespace;
  TopicOptions topic_options;
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // RocketSpeed callbacks;
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {
    printf("publish -- %s\n", rs->GetStatus().ToString().c_str());
  };

  auto subscription_callback = [&] (SubscriptionStatus ss) {
    ASSERT_TRUE(ss.topic_name == topic);
    ASSERT_TRUE(ss.namespace_id == namespace_id);
    printf("subscribe -- %s\n", ss.status.ToString().c_str());
  };

  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    ASSERT_TRUE(mr->GetTopicName().ToString() == topic);
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    printf("received (topic='%s', contents='%s', seqno=%" PRIu64 ")\n",
      mr->GetTopicName().ToString().c_str(),
      mr->GetContents().ToString().c_str(),
      mr->GetSequenceNumber());
    msg_received.Post();
  };

  // Create RocketSpeed client.
  std::unique_ptr<Configuration> config(
      Configuration::Create(cluster.GetPilotHostIds(),
                            cluster.GetCopilotHostIds(),
                            Tenant(102),
                            1));
  //TODO(ranji42) Try to use the same integration_test for mqttclient.
  ClientOptions options(*config, GUIDGenerator().GenerateString());
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

  // Send a message.
  auto ps = client->Publish(topic,
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
  client->ListenTopics(subscriptions);

  // Wait for the message.
  bool result = msg_received.TimedWait(timeout);
  ASSERT_TRUE(result);
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
    subscribe_sem.Post();
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
  std::unique_ptr<Configuration> config(
      Configuration::Create(cluster.GetPilotHostIds(),
                            cluster.GetCopilotHostIds(),
                            Tenant(102),
                            4));
  ClientOptions options(*config, GUIDGenerator().GenerateString());
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, receive_callback));

  // Send some messages and wait for the acks.
  for (int i = 0; i < 3; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  { // Subscribe using seqno 0.
    std::vector<SubscriptionRequest> subscriptions = {
        SubscriptionRequest(ns, topic, true, 0)
    };
    client->ListenTopics(subscriptions);
    ASSERT_TRUE(subscribe_sem.TimedWait(timeout));
  }

  // Should not receive any of the last three messages.
  // Send 3 more different messages.
  for (int i = 3; i < 6; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(topic, ns, opts, Slice(data),
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
    client->ListenTopics(subscriptions);
    ASSERT_TRUE(subscribe_sem.TimedWait(timeout));
  }

  // Send some messages and wait for the acks.
  for (int i = 6; i < 9; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(topic, ns, opts, Slice(data),
                                publish_callback).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  { // Subscribe using seqno 0.
    std::vector<SubscriptionRequest> subscriptions = {
        SubscriptionRequest(ns, topic, true, 0)
    };
    client->ListenTopics(subscriptions);
    ASSERT_TRUE(subscribe_sem.TimedWait(timeout));
  }

  // Send 3 more messages again.
  for (int i = 9; i < 12; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client->Publish(topic, ns, opts, Slice(data),
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

  port::Semaphore subscribed;
  port::Semaphore received_data;

  // Start client loop.
  MsgLoop client(env_, EnvOptions(), 58499, 1, info_log, "client");
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mMetadata] =
    [&] (std::unique_ptr<Message> msg) {
      subscribed.Post();
    };
  callbacks[MessageType::mDeliver] =
    [&] (std::unique_ptr<Message> msg) {
      received_data.Post();
    };
  callbacks[MessageType::mDataAck] = [] (std::unique_ptr<Message>) {};
  client.RegisterCallbacks(callbacks);
  env_->StartThread([&] () { client.Run(); }, "client");
  client.WaitUntilRunning();

  // Subscribe.
  NamespaceID ns = GuestNamespace;
  MessageMetadata sub(Tenant::GuestTenant,
                      MessageMetadata::MetaType::Request,
                      "client",
                      { TopicPair(1, "topic", MetadataType::mSubscribe, ns) });
  ClientID host = cluster.GetCopilotHostIds().front().ToClientId();
  ASSERT_OK(client.SendRequest(sub, host));
  ASSERT_TRUE(subscribed.TimedWait(std::chrono::milliseconds(100)));

  // Now say goodbye.
  MessageGoodbye goodbye(Tenant::GuestTenant,
                         "client",
                         MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);
  ASSERT_OK(client.SendRequest(goodbye, host));
  env_->SleepForMicroseconds(100 * 1000);  // allow goodbye to process

  // Now publish to pilot.
  // We shouldn't get the message.
  MessageData publish(MessageType::mPublish,
                      Tenant::GuestTenant,
                      "client",
                      "topic",
                      ns,
                      Slice("data"));
  ASSERT_OK(client.SendRequest(publish, host));
  ASSERT_TRUE(!received_data.TimedWait(std::chrono::milliseconds(100)));
}

/**
 * Unsubscribe callback.
 */
TEST(IntegrationTest, UnsubscribeCallback) {
    // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message setup.
  Topic topic = "test_topic";
  NamespaceID namespace_id = GuestNamespace;

  port::Semaphore sub_checkpoint;
  std::atomic<bool> expected_subscribed{true};  // first message is a subscribe.
  auto subscription_callback = [&] (SubscriptionStatus ss) {
    ASSERT_OK(ss.status);
    ASSERT_TRUE(ss.topic_name == topic);
    ASSERT_TRUE(ss.namespace_id == namespace_id);
    ASSERT_EQ(ss.subscribed, expected_subscribed.load());
    expected_subscribed.store(false);  // next message should be unsub.
    sub_checkpoint.Post();
  };

  // Create RocketSpeed client.
  std::unique_ptr<Configuration> config(
      Configuration::Create(cluster.GetPilotHostIds(),
                            cluster.GetCopilotHostIds(),
                            Tenant(102),
                            1));
  ClientOptions options(*config, GUIDGenerator().GenerateString());
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));
  ASSERT_OK(client->Start(subscription_callback, nullptr));

  // Subscribe.
  std::vector<SubscriptionRequest> subscriptions1 = {
    SubscriptionRequest(namespace_id, topic, true, 1)
  };
  client->ListenTopics(subscriptions1);
  sub_checkpoint.TimedWait(std::chrono::seconds(1));

  // Unsubscribe.
  std::vector<SubscriptionRequest> subscriptions2 = {
    SubscriptionRequest(namespace_id, topic, false, 1)
  };
  client->ListenTopics(subscriptions2);
  sub_checkpoint.TimedWait(std::chrono::seconds(1));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
