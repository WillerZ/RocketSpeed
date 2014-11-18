// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include <chrono>
#include <memory>
#include <vector>

#include "include/RocketSpeed.h"
#include "src/util/testharness.h"
#include "src/test/test_cluster.h"
#include "src/port/port.h"
#include "src/client/client.h"
#include "src/util/client/guid_generator.h"

namespace rocketspeed {

class IntegrationTest {
 public:
  IntegrationTest() {
    if (!rocketspeed::CreateLoggerFromOptions(rocketspeed::Env::Default(),
                                              "",
                                              "LOG.integrationtest",
                                              0,
                                              0,
                                              rocketspeed::INFO_LEVEL,
                                              &info_log).ok()) {
      fprintf(stderr, "Error creating logger, aborting.\n");
    } else {
      info_log = std::make_shared<rocketspeed::NullLogger>();
    }
  }
  std::shared_ptr<rocketspeed::Logger> info_log;
};

TEST(IntegrationTest, OneMessage) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);

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
  auto publish_callback = [&] (ResultStatus rs) {
    printf("publish -- %s\n", rs.status.ToString().c_str());
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
  ClientImpl client(GUIDGenerator().GenerateString(),
                    cluster.GetPilotHostIds().front(),
                    cluster.GetCopilotHostIds().front(),
                    Tenant(102),
                    publish_callback,
                    subscription_callback,
                    receive_callback,
                    info_log);

  // Send a message.
  auto ps = client.Publish(topic,
                           namespace_id,
                           topic_options,
                           Slice(data),
                           message_id);
  ASSERT_TRUE(ps.status.ok());
  ASSERT_TRUE(ps.msgid == message_id);

  // Listen for the message.
  std::vector<SubscriptionPair> subscriptions = {
    SubscriptionPair(1, topic, namespace_id)
  };
  client.ListenTopics(subscriptions, topic_options);

  // Wait for the message.
  bool result = msg_received.TimedWait(std::chrono::seconds(10));
  ASSERT_TRUE(result);
}

TEST(IntegrationTest, SequenceNumberZero) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster(info_log);

  // Message read semaphore.
  port::Semaphore message_sem;
  port::Semaphore publish_sem;
  port::Semaphore subscribe_sem;

  // Message setup.
  Topic topic = "SequenceNumberZero";
  NamespaceID ns = 102;
  TopicOptions opts(Retention::OneDay);
  std::chrono::seconds timeout(5);

  // RocketSpeed callbacks;
  auto publish_callback = [&] (ResultStatus rs) {
    publish_sem.Post();
  };

  auto subscription_callback = [&] (SubscriptionStatus ss) {
    subscribe_sem.Post();
  };

  std::vector<std::string> received;
  auto receive_callback = [&] (std::unique_ptr<MessageReceived> mr) {
    received.push_back(mr->GetContents().ToString());
    message_sem.Post();
  };

    // Create RocketSpeed client.
  ClientImpl client(GUIDGenerator().GenerateString(),
                    cluster.GetPilotHostIds().front(),
                    cluster.GetCopilotHostIds().front(),
                    Tenant(102),
                    publish_callback,
                    subscription_callback,
                    receive_callback,
                    info_log);

  // Send some messages and wait for the acks.
  for (int i = 0; i < 3; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client.Publish(topic, ns, opts, Slice(data)).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
  }

  // Subscribe using seqno 0.
  std::vector<SubscriptionPair> subscriptions = {
    SubscriptionPair(0, topic, ns)
  };
  client.ListenTopics(subscriptions, opts);
  ASSERT_TRUE(subscribe_sem.TimedWait(timeout));

  // Should not receive any of the last three messages.
  // Send 3 more different messages.
  for (int i = 3; i < 6; ++i) {
    std::string data = std::to_string(i);
    ASSERT_TRUE(client.Publish(topic, ns, opts, Slice(data)).status.ok());
    ASSERT_TRUE(publish_sem.TimedWait(timeout));
    ASSERT_TRUE(message_sem.TimedWait(timeout));
  }

  std::vector<std::string> expected = {"3", "4", "5"};
  ASSERT_TRUE(received == expected);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
