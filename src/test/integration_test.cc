// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <memory>
#include "include/RocketSpeed.h"
#include "src/util/testharness.h"
#include "src/test/test_cluster.h"
#include "src/port/port.h"

namespace rocketspeed {

class IntegrationTest {};

TEST(IntegrationTest, OneMessage) {
  // Setup local RocketSpeed cluster.
  LocalTestCluster cluster;

  // Message read semaphore.
  port::Semaphore msg_received;

  // Message setup.
  Topic topic = "test_topic";
  NamespaceID namespace_id = 102;
  TopicOptions topic_options(Retention::OneDay);
  std::string data = "test_message";

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

  // Create configuration for this cluster.
  std::unique_ptr<Configuration> config(
    Configuration::Create(cluster.GetPilotHostIds(),
                          cluster.GetCopilotHostIds(),
                          Tenant(102),
                          58499));

  // Create RocketSpeed client.
  Client* client = nullptr;
  Status st = Client::Open(config.get(),
                           publish_callback,
                           subscription_callback,
                           receive_callback,
                           &client);
  ASSERT_TRUE(st.ok());

  // Send a message.
  auto ps = client->Publish(topic, namespace_id, topic_options, Slice(data));
  ASSERT_TRUE(ps.status.ok());

  // Listen for the message.
  std::vector<SubscriptionPair> subscriptions = {
    SubscriptionPair(1, topic, namespace_id)
  };
  client->ListenTopics(subscriptions, topic_options);

  // Wait for the message.
  bool result = msg_received.TimedWait(std::chrono::system_clock::now() +
                                       std::chrono::seconds(10));
  ASSERT_TRUE(result);
  delete client;
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
