// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

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
      , env_(Env::Default())
      , file_path(test::TmpDir() + "/IntegrationTest/FileStorage.bin") {
    ASSERT_OK(test::CreateLogger(env_, "IntegrationTest", &info_log));
  }

 protected:
  Env* env_;
  std::shared_ptr<rocketspeed::Logger> info_log;
  std::string file_path;
};

TEST(IntegrationTest, OneMessage) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  // Message read semaphore.
  port::Semaphore msg_received;

  // Message setup.
  Topic topic = "test_topic";
  NamespaceID namespace_id = 102;
  TopicOptions topic_options(Retention::OneDay);
  std::string data = "test_message";
  GUIDGenerator msgid_generator;
  MsgId message_id = msgid_generator.Generate();

  // RocketSpeed callbacks;
  auto publish_callback = [&] (std::unique_ptr<ResultStatus> rs) {
    printf("publish -- %s\n", rs->GetStatus().ToString().c_str());
  };

  auto subscription_callback = [&] (SubscriptionStatus ss) {
    printf("subscribe -- %s\n", ss.status.ToString().c_str());
  };

  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    ASSERT_TRUE(mr->GetTopicName().ToString() == topic);
    ASSERT_TRUE(mr->GetContents().ToString() == data);
    printf("received (topic='%s', contents='%s', seqno=%ld)\n",
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
  options.subscription_callback = subscription_callback;
  options.receive_callback = receive_callback;
  options.info_log = info_log;
  ASSERT_OK(SubscriptionStorage::File(env_,
                                      file_path,
                                      info_log,
                                      &options.storage));
  std::unique_ptr<Client> client;
  ASSERT_TRUE((Client::Open(std::move(options), &client).ok()));

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
  NamespaceID ns = 102;
  TopicOptions opts(Retention::OneDay);

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
  options.subscription_callback = subscription_callback;
  options.receive_callback = receive_callback;
  options.info_log = info_log;
  ASSERT_OK(SubscriptionStorage::File(env_,
                                      file_path,
                                      info_log,
                                      &options.storage));
  std::unique_ptr<Client> client;
  ASSERT_TRUE((Client::Open(std::move(options), &client).ok()));

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

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
