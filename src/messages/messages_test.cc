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
  HostId host1("host.id", 1234);
  NamespaceID nsid1 = 200;

  // create a message
  MessageData data1(MessageType::mPublish,
                    Tenant::GuestTenant, host1, name1, nsid1, payload1,
                    Retention::OneDay);

  // serialize the message
  Slice original = data1.Serialize();

  // un-serialize to a new message
  MessageData data2;
  data2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_TRUE(data2.GetMessageId() == data1.GetMessageId());
  ASSERT_TRUE(data2.GetOrigin() == host1);
  ASSERT_EQ(data2.GetTopicName().ToString(), name1.ToString());
  ASSERT_EQ(data2.GetPayload().ToString(), payload1.ToString());
  ASSERT_EQ(data2.GetRetention(), Retention::OneDay);
  ASSERT_EQ(data2.GetTenantID(), Tenant::GuestTenant);
  ASSERT_EQ(data2.GetNamespaceId(), nsid1);
}

TEST(Messaging, Metadata) {
  int port = 200;
  std::string mymachine = "machine.com";
  HostId hostid(mymachine, port);
  std::vector<TopicPair> topics;
  int num_topics = 5;

  // create a few topics
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    topics.push_back(TopicPair(3 + i, std::to_string(i), type, 200 + i));
  }

  // create a message
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        hostid, topics);

  // serialize the message
  Slice original = meta1.Serialize();

  // un-serialize to a new message
  MessageMetadata data2;
  data2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_EQ(mymachine, data2.GetOrigin().hostname);
  ASSERT_EQ(port, data2.GetOrigin().port);
  ASSERT_EQ(Tenant::GuestTenant, data2.GetTenantID());

  // verify that the new message is the same as original
  std::vector<TopicPair> nt = data2.GetTopicInfo();
  ASSERT_EQ(nt.size(), topics.size());
  for (unsigned int i = 0; i < topics.size(); i++) {
    ASSERT_EQ(nt[i].seqno, topics[i].seqno);
    ASSERT_EQ(nt[i].topic_name, topics[i].topic_name);
    ASSERT_EQ(nt[i].topic_type, topics[i].topic_type);
    ASSERT_EQ(nt[i].namespace_id, topics[i].namespace_id);
  }
}

TEST(Messaging, DataAck) {
  int port = 200;
  std::string mymachine = "machine.com";
  HostId hostid(mymachine, port);

  // create a message
  std::vector<MessageDataAck::Ack> acks(10);
  char value = 0;
  for (auto& ack : acks) {
    for (size_t i = 0; i < sizeof(acks[0].msgid.id); ++i) {
      ack.msgid.id[i] = value++;
      ack.seqno = i;
    }
  }
  MessageDataAck ack1(101, hostid, acks);

  // serialize the message
  Slice original = ack1.Serialize();

  // un-serialize to a new message
  MessageDataAck ack2;
  ack2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_TRUE(ack1.GetAcks() == ack2.GetAcks());
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
