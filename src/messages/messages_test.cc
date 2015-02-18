//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <string>
#include <vector>

#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/testharness.h"
#include "src/port/port.h"

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
  NamespaceID nsid1 = 200;

  // create a message
  MessageData data1(MessageType::mPublish,
                    Tenant::GuestTenant, clid1, name1, nsid1, payload1,
                    Retention::OneDay);

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
  ASSERT_EQ(data2.GetRetention(), Retention::OneDay);
  ASSERT_EQ(data2.GetTenantID(), Tenant::GuestTenant);
  ASSERT_EQ(data2.GetNamespaceId(), nsid1);
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
    topics.push_back(TopicPair(3 + i, std::to_string(i), type, 200 + i));
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
                          MessageGoodbye::Code::Graceful);

  // serialize the message
  Slice original = goodbye1.Serialize();

  // un-serialize to a new message
  MessageGoodbye goodbye2;
  goodbye2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_TRUE(goodbye2.GetOrigin() == origin);
  ASSERT_EQ(goodbye2.GetTenantID(), Tenant::GuestTenant);
  ASSERT_EQ(goodbye2.GetCode(), goodbye1.GetCode());
}

TEST(Messaging, ErrorHandling) {
  // Test that Message::CreateNewInstance handles bad messages.
  TenantID tenant = Tenant::GuestTenant;
  ClientID client = "client1";
  NamespaceID nsid = 200;

  MessagePing msg0(tenant, MessagePing::Request, client);
  TestMessage(msg0);

  MessageData msg1(MessageType::mPublish,
                   tenant, client, "topic", nsid, "payload",
                   Retention::OneDay);
  TestMessage(msg1);

  std::vector<TopicPair> topics = {{ 100, "topic", mSubscribe, nsid }};
  MessageMetadata msg2(tenant,
                       MessageMetadata::MetaType::Request,
                       client,
                       topics);
  TestMessage(msg2);

  MessageDataAck::AckVector acks(1);
  MessageDataAck msg3(nsid, client, acks);
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
  while (!loop1.IsRunning() || !loop2.IsRunning()) {
    std::this_thread::yield();
  }

  // Send a ping from loop2 to loop1.
  MessagePing msg(Tenant::GuestTenant,
                  MessagePing::PingType::Request,
                  "sender");
  std::string serial;
  msg.SerializeToString(&serial);
  std::unique_ptr<Command> cmd1(
      new SerializedSendCommand(serial,
                                loop1.GetClientId(0),
                                env_->NowMicros(),
                                true));
  ASSERT_OK(loop2.SendCommand(std::move(cmd1)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Start a new loop and communicate with the same client ID.
  MsgLoop loop3(env_, env_options_, 0, 1, info_log_, "loop3");
  loop3.RegisterCallbacks(callbacks);
  env_->StartThread([&] () { loop3.Run(); }, "loop3");
  while (!loop3.IsRunning()) {
    std::this_thread::yield();
  }
  std::unique_ptr<Command> cmd2(
      new SerializedSendCommand(serial,  // same msg, same client ID
                                loop1.GetClientId(0),
                                env_->NowMicros(),
                                true));
  ASSERT_OK(loop3.SendCommand(std::move(cmd2)));
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
  while (!server.IsRunning() || !client.IsRunning()) {
    std::this_thread::yield();
  }

  // Send a pings from client to server.
  for (int i = 0; i < 100; ++i) {
    ClientID client_id = "client" + std::to_string(i % 10);
    MessagePing msg(Tenant::GuestTenant,
                    MessagePing::PingType::Request,
                    client_id);
    std::string serial;
    msg.SerializeToString(&serial);
    std::unique_ptr<Command> cmd(
        new SerializedSendCommand(serial,
                                  server.GetClientId(0),
                                  env_->NowMicros(),
                                  true));
    ASSERT_OK(client.SendCommand(std::move(cmd)));
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
  while (!server.IsRunning() || !client.IsRunning()) {
    std::this_thread::yield();
  }

  // Create messages
  MessagePing ping1_req(Tenant::GuestTenant, MessagePing::Request, "c1");
  std::string ping1_req_serial;
  ping1_req.SerializeToString(&ping1_req_serial);

  MessagePing ping2_req(Tenant::GuestTenant, MessagePing::Request, "c2");
  std::string ping2_req_serial;
  ping2_req.SerializeToString(&ping2_req_serial);

  MessageGoodbye goodbye1(Tenant::GuestTenant,
                          "c1",
                          MessageGoodbye::Code::Graceful);
  std::string goodbye1_serial;
  goodbye1.SerializeToString(&goodbye1_serial);

  MessagePing ping1_resp(Tenant::GuestTenant, MessagePing::Response, "c1");
  std::string ping1_resp_serial;
  ping1_resp.SerializeToString(&ping1_resp_serial);

  MessagePing ping2_resp(Tenant::GuestTenant, MessagePing::Response, "c2");
  std::string ping2_resp_serial;
  ping2_resp.SerializeToString(&ping2_resp_serial);

  // Ping request c1 -> server
  std::unique_ptr<Command> cmd1(
      new SerializedSendCommand(ping1_req_serial,
                                server.GetClientId(0),
                                env_->NowMicros(),
                                true));
  ASSERT_OK(client.SendCommand(std::move(cmd1)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // Ping request c2 -> server
  std::unique_ptr<Command> cmd2(
      new SerializedSendCommand(ping2_req_serial,
                                server.GetClientId(0),
                                env_->NowMicros(),
                                true));
  ASSERT_OK(client.SendCommand(std::move(cmd2)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // Goodbye c1 -> server
  std::unique_ptr<Command> cmd3(
      new SerializedSendCommand(goodbye1_serial,
                                server.GetClientId(0),
                                env_->NowMicros(),
                                true));
  ASSERT_OK(client.SendCommand(std::move(cmd3)));
  ASSERT_TRUE(goodbye_checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // Ping response server -> c1
  std::unique_ptr<Command> cmd4(
      new SerializedSendCommand(ping1_resp_serial,
                                "c1",
                                env_->NowMicros(),
                                false));
  ASSERT_OK(server.SendCommand(std::move(cmd4)));
  // Should NOT get response -- c1 has said goodbye
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // Ping response server -> c2
  std::unique_ptr<Command> cmd5(
      new SerializedSendCommand(ping2_resp_serial,
                                "c2",
                                env_->NowMicros(),
                                false));
  ASSERT_OK(server.SendCommand(std::move(cmd5)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
