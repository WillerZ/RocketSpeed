//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <string>
#include <vector>

#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class Messaging {
 public:
  Messaging() {
    env_ = Env::Default();
    ASSERT_OK(test::CreateLogger(env_, "MessagesTest", &info_log_));
  }

  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;
};

TEST(Messaging, Data) {
  Slice name1("Topic1");
  Slice payload1("Payload1");
  HostId host1("host.id", 1234);
  ClientID clid1("client1");
  NamespaceID nsid1 = GuestNamespace;

  // create a message
  MessageData data1(MessageType::mPublish,
                    Tenant::GuestTenant, clid1, name1, nsid1, payload1);

  // serialize the message
  Slice original = data1.Serialize();

  // un-serialize to a new message
  MessageData data2;
  data2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_TRUE(data2.GetMessageId() == data1.GetMessageId());
  ASSERT_TRUE(data2.GetOrigin() == clid1);
  ASSERT_EQ(data2.GetTopicName().ToString(), name1.ToString());
  ASSERT_EQ(data2.GetPayload().ToString(), payload1.ToString());
  ASSERT_EQ(data2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(data2.GetNamespaceId().ToString(), nsid1);
}

TEST(Messaging, Metadata) {
  std::string mymachine = "machine.com";
  ClientID clid1("client1");
  std::vector<TopicPair> topics;
  int num_topics = 5;

  // create a few topics
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    NamespaceID ns = "test" + std::to_string(i);
    topics.push_back(TopicPair(3 + i, std::to_string(i), type, ns));
  }

  // create a message
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        clid1, topics);

  // serialize the message
  Slice original = meta1.Serialize();

  // un-serialize to a new message
  MessageMetadata data2;
  data2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_EQ(clid1, data2.GetOrigin());
  ASSERT_EQ((TenantID)Tenant::GuestTenant, data2.GetTenantID());

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
  ClientID clid = hostid.ToClientId();

  // create a message
  MessageDataAck::AckVector acks(10);
  char value = 0;
  for (auto& ack : acks) {
    for (size_t i = 0; i < sizeof(acks[0].msgid.id); ++i) {
      ack.msgid.id[i] = value++;
      ack.seqno = i;
    }
  }
  MessageDataAck ack1(101, clid, acks);

  // serialize the message
  Slice original = ack1.Serialize();

  // un-serialize to a new message
  MessageDataAck ack2;
  ack2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_TRUE(ack1.GetAcks() == ack2.GetAcks());
}

static void TestMessage(const Serializer& msg) {
  Slice slice = msg.Serialize();

  // Should successfully parse.
  ASSERT_TRUE(Message::CreateNewInstance(slice.ToUniqueChars(),
                                         slice.size()) != nullptr);

  // All sub-sizes should fail to parse:
  for (size_t n = 0; n < slice.size(); ++n) {
    // Only first n bytes
    ASSERT_TRUE(Message::CreateNewInstance(slice.ToUniqueChars(),
                                           n) == nullptr);
  }
}

TEST(Messaging, Goodbye) {
  ClientID origin("client1");

  // create a message
  MessageGoodbye goodbye1(Tenant::GuestTenant,
                          origin,
                          MessageGoodbye::Code::Graceful,
                          MessageGoodbye::Server);

  // serialize the message
  Slice original = goodbye1.Serialize();

  // un-serialize to a new message
  MessageGoodbye goodbye2;
  goodbye2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_TRUE(goodbye2.GetOrigin() == origin);
  ASSERT_EQ(goodbye2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(goodbye2.GetCode(), goodbye1.GetCode());
  ASSERT_EQ(goodbye2.GetOriginType(), goodbye1.GetOriginType());
}

TEST(Messaging, ErrorHandling) {
  // Test that Message::CreateNewInstance handles bad messages.
  TenantID tenant = Tenant::GuestTenant;
  ClientID client = "client1";
  NamespaceID nsid = GuestNamespace;

  MessagePing msg0(tenant, MessagePing::Request, client);
  TestMessage(msg0);

  MessageData msg1(MessageType::mPublish,
                   tenant, client, "topic", nsid, "payload");
  TestMessage(msg1);

  std::vector<TopicPair> topics = {{ 100, "topic", mSubscribe, nsid }};
  MessageMetadata msg2(tenant,
                       MessageMetadata::MetaType::Request,
                       client,
                       topics);
  TestMessage(msg2);

  MessageDataAck::AckVector acks(1);
  MessageDataAck msg3(100, client, acks);
  TestMessage(msg3);

  MessageGap msg4(tenant, client, kBenign, 100, 200);
  TestMessage(msg4);
}

TEST(Messaging, ClientOnNewSocket) {
  // Tests that a client can reconnect seamlessly on a new socket.
  // Receiver loop.
  MsgLoop loop1(env_, env_options_, 58499, 1, info_log_, "loop1");
  env_->StartThread([&] () { loop1.Run(); }, "loop1");

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mPing] =
    [&](std::unique_ptr<Message> msg) {
      checkpoint.Post();
    };

  // Sender loop.
  MsgLoop loop2(env_, env_options_, 0, 1, info_log_, "loop2");
  loop2.RegisterCallbacks(callbacks);
  env_->StartThread([&] () { loop2.Run(); }, "loop2");
  ASSERT_OK(loop1.WaitUntilRunning());
  ASSERT_OK(loop2.WaitUntilRunning());

  // Send a ping from loop2 to loop1.
  MessagePing msg(Tenant::GuestTenant,
                  MessagePing::PingType::Request,
                  "sender");
  ASSERT_OK(loop2.SendRequest(msg, loop1.GetClientId(0)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Start a new loop and communicate with the same client ID.
  MsgLoop loop3(env_, env_options_, 0, 1, info_log_, "loop3");
  loop3.RegisterCallbacks(callbacks);
  env_->StartThread([&] () { loop3.Run(); }, "loop3");
  ASSERT_OK(loop3.WaitUntilRunning());
  ASSERT_OK(loop3.SendRequest(msg, loop1.GetClientId(0)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
}

TEST(Messaging, MultipleClientsOneSocket) {
  // Tests that multiple clients can communicate over the same socket.
  // Server loop.
  MsgLoop server(env_, env_options_, 58499, 1, info_log_, "server");
  env_->StartThread([&] () { server.Run(); }, "server");

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  std::map<MessageType, MsgCallbackType> callbacks;

  std::atomic<int> message_num{0};
  callbacks[MessageType::mPing] =
    [&](std::unique_ptr<Message> msg) {
      // Origin should be set to the client ID that initiated.
      ClientID expected = "client" + std::to_string(message_num++ % 10);
      ASSERT_EQ(msg->GetOrigin(), expected);
      checkpoint.Post();
    };

  // Clients loop.
  MsgLoop client(env_, env_options_, 0, 1, info_log_, "client");
  client.RegisterCallbacks(callbacks);
  env_->StartThread([&] () { client.Run(); }, "client");
  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client.WaitUntilRunning());

  // Send a pings from client to server.
  for (int i = 0; i < 100; ++i) {
    ClientID client_id = "client" + std::to_string(i % 10);
    MessagePing msg(Tenant::GuestTenant,
                    MessagePing::PingType::Request,
                    client_id);
    ASSERT_OK(client.SendRequest(msg, server.GetClientId(0)));
    ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  }
}

TEST(Messaging, GracefulGoodbye) {
  // Tests that a client can disengage from communication.
  // Create two clients on one socket, talking to a server.
  // One client will send a goodbye, followed by a ping.
  // It should not receive a response after the goodbye.
  // Second client will send a ping, and should receive response.

  port::Semaphore goodbye_checkpoint;
  std::map<MessageType, MsgCallbackType> server_cb;
  server_cb[MessageType::mGoodbye] =
    [&](std::unique_ptr<Message> msg) {
      goodbye_checkpoint.Post();
    };

  // Server loop.
  MsgLoop server(env_, env_options_, 58499, 1, info_log_, "server");
  server.RegisterCallbacks(server_cb);
  env_->StartThread([&] () { server.Run(); }, "server");

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  std::map<MessageType, MsgCallbackType> callbacks;
  std::atomic<int> message_num{0};
  ClientID expected_clients[] = { "c1", "c2", "c2" };
  callbacks[MessageType::mPing] =
    [&](std::unique_ptr<Message> msg) {
      int n = message_num++;
      ASSERT_LT(n, 3);
      ASSERT_EQ(msg->GetOrigin(), expected_clients[n]);
      checkpoint.Post();
    };

  // Clients loop.
  MsgLoop client(env_, env_options_, 0, 1, info_log_, "client");
  client.RegisterCallbacks(callbacks);
  env_->StartThread([&] () { client.Run(); }, "client");
  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client.WaitUntilRunning());

  // Create messages
  MessagePing ping1_req(Tenant::GuestTenant, MessagePing::Request, "c1");
  MessagePing ping2_req(Tenant::GuestTenant, MessagePing::Request, "c2");
  MessageGoodbye goodbye1(Tenant::GuestTenant,
                          "c1",
                          MessageGoodbye::Code::Graceful,
                          MessageGoodbye::OriginType::Client);
  MessagePing ping1_resp(Tenant::GuestTenant, MessagePing::Response, "c1");
  MessagePing ping2_resp(Tenant::GuestTenant, MessagePing::Response, "c2");

  // Ping request c1 -> server
  ASSERT_OK(client.SendRequest(ping1_req, server.GetClientId(0)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);

  // Ping request c2 -> server
  ASSERT_OK(client.SendRequest(ping2_req, server.GetClientId(0)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 2);

  // Goodbye c1 -> server
  ASSERT_OK(client.SendRequest(goodbye1, server.GetClientId(0)));
  ASSERT_TRUE(goodbye_checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);

  // Ping response server -> c1
  ASSERT_OK(server.SendResponse(ping1_resp, "c1", 0));
  // Should NOT get response -- c1 has said goodbye
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);

  // Ping response server -> c2
  ASSERT_OK(server.SendResponse(ping2_resp, "c2", 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);
}

TEST(Messaging, WaitUntilRunningFailure) {
  // Check that WaitUntilRunning returns failure.
  MsgLoop loop1(env_, env_options_, 58499, 1, info_log_, "loop1");
  env_->StartThread([&] () { loop1.Run(); }, "loop1");
  ASSERT_OK(loop1.WaitUntilRunning());

  // now start another on same port, should fail.
  MsgLoop loop2(env_, env_options_, 58499, 1, info_log_, "loop2");
  env_->StartThread([&] () { loop2.Run(); }, "loop2");
  ASSERT_TRUE(!loop2.WaitUntilRunning().ok());
}

TEST(Messaging, GatherTest) {
  MsgLoop loop(env_, env_options_, -1, 10, info_log_, "loop");
  env_->StartThread([&] () { loop.Run(); }, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  port::Semaphore done;
  int n = 0;

  // Simple gather test that simple sums up the worker indices.
  ASSERT_OK(loop.Gather([] (int i) { return i; },
                        [&] (std::vector<int> v) {
                          n = std::accumulate(v.begin(), v.end(), 0);
                          done.Post();
                        }));
  ASSERT_TRUE(done.TimedWait(std::chrono::seconds(1)));
  ASSERT_EQ(n, 45); // 45 = 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
