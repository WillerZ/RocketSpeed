//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <string>
#include <vector>

#include "src/messages/messages.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class Messaging { };

TEST(Messaging, Data) {
  Slice name1("Topic1");
  Slice payload1("Payload1");

  // create a message
  MessageData data1(Tenant::Guest, name1, payload1);

  // serialize the message
  Slice original = data1.Serialize();

  // un-serialize to a new message
  MessageData data2;
  data2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_EQ(data2.GetTopicName().ToString(), name1.ToString());
  ASSERT_EQ(data2.GetPayload().ToString(), payload1.ToString());
  ASSERT_EQ(data2.GetTenantID(), Tenant::Guest);
}
TEST(Messaging, Metadata) {
  SequenceNumber seqno = 100;
  int port = 200;
  std::string mymachine = "machine.com";
  HostId hostid(mymachine, port);
  std::vector<TopicPair> topics;
  int num_topics = 5;

  // create a few topics
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    topics.push_back(TopicPair(std::to_string(i), type));
  }

  // create a message
  MessageMetadata meta1(Tenant::Guest, seqno, hostid, topics);

  // serialize the message
  Slice original = meta1.Serialize();

  // un-serialize to a new message
  MessageMetadata data2;
  data2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_EQ(seqno, data2.GetSequenceNumber());
  ASSERT_EQ(mymachine, data2.GetHostId().hostname);
  ASSERT_EQ(port, data2.GetHostId().port);
  ASSERT_EQ(Tenant::Guest, data2.GetTenantID());

  // verify that the new message is the same as original
  std::vector<TopicPair> nt = data2.GetTopicInfo();
  ASSERT_EQ(nt.size(), topics.size());
  for (unsigned int i = 0; i < topics.size(); i++) {
    ASSERT_EQ(nt[i].topic_name, topics[i].topic_name);
    ASSERT_EQ(nt[i].topic_type, topics[i].topic_type);
  }
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
