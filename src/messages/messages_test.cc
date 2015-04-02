//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <string>
#include <unordered_set>
#include <vector>

#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "src/util/testharness.h"
#include "src/util/common/multi_producer_queue.h"

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
  data1.SetSequenceNumbers(1000100010001000ULL, 2000200020002000ULL);

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
  ASSERT_EQ(data2.GetPrevSequenceNumber(), 1000100010001000ULL);
  ASSERT_EQ(data2.GetSequenceNumber(), 2000200020002000ULL);
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

TEST(Messaging, DataGap) {
  // create a message
  MessageGap gap1(Tenant::GuestTenant,
                  "client",
                  "guest",
                  "topic",
                  GapType::kDataLoss,
                  100,
                  200);

  // serialize the message
  Slice original = gap1.Serialize();

  // un-serialize to a new message
  MessageGap gap2;
  gap2.DeSerialize(&original);

  // verify that the new message is the same as original
  ASSERT_TRUE(gap2.GetOrigin() == "client");
  ASSERT_EQ(gap2.GetTopicName().ToString(), "topic");
  ASSERT_EQ(gap2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(gap2.GetNamespaceId().ToString(), "guest");
  ASSERT_EQ(gap2.GetStartSequenceNumber(), 100);
  ASSERT_EQ(gap2.GetEndSequenceNumber(), 200);
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

  MessageGap msg4(tenant, client, "guest", "topic", kBenign, 100, 200);
  TestMessage(msg4);
}

TEST(Messaging, SameStreamsOnDifferentSockets) {
  // Posted on any ping message received by the server.
  port::Semaphore server_ping;
  MultiProducerQueue<std::unique_ptr<MessagePing>> server_pings(3);

  MsgLoop server(env_, env_options_, 58499, 1, info_log_, "server");
  server.RegisterCallbacks({
      {MessageType::mPing, [&](std::unique_ptr<Message> msg) {
        ASSERT_TRUE(
            server_pings.write(static_cast<MessagePing*>(msg.release())));
        server_ping.Post();
      }},
  });
  env_->StartThread([&]() { server.Run(); }, "server");

  // Posted on any ping message received by any client.
  port::Semaphore client_ping;

  // First client loop.
  MsgLoop client1(env_, env_options_, 0, 1, info_log_, "client1");
  client1.RegisterCallbacks({
      {MessageType::mPing, [&](std::unique_ptr<Message> msg) {
        auto ping = static_cast<MessagePing*>(msg.get());
        ASSERT_EQ("stream1", ping->GetCookie());
        client_ping.Post();
      }},
  });
  env_->StartThread([&]() { client1.Run(); }, "client1");

  // Second client loop.
  MsgLoop client2(env_, env_options_, 0, 1, info_log_, "client2");
  client2.RegisterCallbacks({
      {MessageType::mPing, [&](std::unique_ptr<Message> msg) {
        auto ping = static_cast<MessagePing*>(msg.get());
        ASSERT_EQ("stream2", ping->GetCookie());
        client_ping.Post();
      }},
  });
  env_->StartThread([&]() { client2.Run(); }, "client2");

  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client1.WaitUntilRunning());
  ASSERT_OK(client2.WaitUntilRunning());

  // Create two streams on different loops...
  StreamSocket socket1(server.GetClientId(0), client1.GetOutboundAllocator());
  StreamSocket socket2(server.GetClientId(0), client2.GetOutboundAllocator());
  // ...that have the same stream ID.
  ASSERT_EQ(socket1.GetStreamID(), socket2.GetStreamID());

  {  // Send a ping from client1 to server.
    MessagePing ping(
        Tenant::GuestTenant, MessagePing::PingType::Request, "stream1");
    ASSERT_OK(client1.SendRequest(ping, &socket1, 0));
    ASSERT_TRUE(server_ping.TimedWait(std::chrono::seconds(1)));
  }
  {  // Send a ping from client2 to server.
    MessagePing ping(
        Tenant::GuestTenant, MessagePing::PingType::Request, "stream2");
    ASSERT_OK(client2.SendRequest(ping, &socket2, 0));
    ASSERT_TRUE(server_ping.TimedWait(std::chrono::seconds(1)));
  }

  // Used to assert how many clients the server has seen.
  std::unordered_set<StreamID> seen_by_server;

  {  // Send back pong to client1.
    std::unique_ptr<MessagePing> pong;
    ASSERT_TRUE(server_pings.read(pong));
    ASSERT_EQ("stream1", pong->GetCookie());
    seen_by_server.insert(pong->GetOrigin());
    // Send back reponse.
    pong->SetPingType(MessagePing::PingType::Response);
    server.SendResponse(*pong, pong->GetOrigin(), 0);
    ASSERT_TRUE(client_ping.TimedWait(std::chrono::seconds(1)));
  }
  {  // Send back pong to client2.
    std::unique_ptr<MessagePing> pong;
    ASSERT_TRUE(server_pings.read(pong));
    ASSERT_EQ("stream2", pong->GetCookie());
    seen_by_server.insert(pong->GetOrigin());
    // Send back reponse.
    pong->SetPingType(MessagePing::PingType::Response);
    server.SendResponse(*pong, pong->GetOrigin(), 0);
    ASSERT_TRUE(client_ping.TimedWait(std::chrono::seconds(1)));
  }

  ASSERT_EQ(2, seen_by_server.size());
}

TEST(Messaging, MultipleStreamsOneSocket) {
  static const int kNumStreams = 10;

  // Server loop.
  MsgLoop server(env_, env_options_, 58499, 1, info_log_, "server");
  env_->StartThread([&]() { server.Run(); }, "server");

  // Clients loop.
  MsgLoop client(env_, env_options_, 0, 1, info_log_, "client");

  // Create a bunch of sockets for different streams.
  std::vector<StreamSocket> sockets;
  for (int i = 0; i < kNumStreams; ++i) {
    sockets.emplace_back(server.GetClientId(0), client.GetOutboundAllocator());
  }

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  {
    // Callback uses (a copy of) this to count messages.
    int message_num = 0;
    client.RegisterCallbacks({
        {MessageType::mPing,
         [&checkpoint, &sockets, message_num](
             std::unique_ptr<Message> msg) mutable {
          int i = message_num++ % kNumStreams;
          ASSERT_LT(i, kNumStreams);
          auto ping = static_cast<MessagePing*>(msg.get());
          ASSERT_EQ(std::to_string(i), ping->GetCookie());
          ASSERT_EQ(sockets[i].GetStreamID(), msg->GetOrigin());
          checkpoint.Post();
        }},
    });
  }
  env_->StartThread([&]() { client.Run(); }, "loop-client");

  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client.WaitUntilRunning());

  // Send a pings from client to server.
  for (int i = 0; i < 10 * kNumStreams; ++i) {
    MessagePing msg(Tenant::GuestTenant,
                    MessagePing::PingType::Request,
                    std::to_string(i % kNumStreams));
    ASSERT_OK(client.SendRequest(msg, &sockets[i % sockets.size()], 0));
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
  // Records streams of ping messages received by the control tower.
  std::vector<StreamID> inbound_stream;
  std::mutex inbound_stream_mutex;

  // Server loop.
  MsgLoop server(env_, env_options_, 58499, 1, info_log_, "server");
  server.RegisterCallbacks({
      {MessageType::mGoodbye,
       [&](std::unique_ptr<Message> msg) { goodbye_checkpoint.Post(); }},
      {MessageType::mPing, [&](std::unique_ptr<Message> msg) {
        {
          std::lock_guard<std::mutex> lock(inbound_stream_mutex);
          inbound_stream.push_back(msg->GetOrigin());
        }
        auto ping = static_cast<MessagePing*>(msg.get());
        ping->SetPingType(MessagePing::PingType::Response);
        server.SendResponse(
            *ping, msg->GetOrigin(), server.GetThreadWorkerIndex());
      }},
  });
  env_->StartThread([&]() { server.Run(); }, "loop-server");

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  std::atomic<int> message_num{0};
  std::string expected_cookies[] = {"c1", "c2", "c2"};
  std::map<MessageType, MsgCallbackType> callbacks = {
      {MessageType::mPing, [&](std::unique_ptr<Message> msg) {
        int n = message_num++;
        ASSERT_LT(n, 3);
        auto ping = static_cast<MessagePing*>(msg.get());
        ASSERT_EQ(expected_cookies[n], ping->GetCookie());
        checkpoint.Post();
      }},
  };

  // Clients loop.
  MsgLoop client(env_, env_options_, 0, 1, info_log_, "client");
  client.RegisterCallbacks(callbacks);
  env_->StartThread([&]() { client.Run(); }, "client");

  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client.WaitUntilRunning());

  // Two streams.
  StreamSocket c1(server.GetClientId(0), client.GetOutboundAllocator());
  StreamSocket c2(server.GetClientId(0), client.GetOutboundAllocator());

  // Ping request c1 -> server
  MessagePing ping1_req(Tenant::GuestTenant, MessagePing::Request, "c1");
  ASSERT_OK(client.SendRequest(ping1_req, &c1, 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);

  // Ping request c2 -> server
  MessagePing ping2_req(Tenant::GuestTenant, MessagePing::Request, "c2");
  ASSERT_OK(client.SendRequest(ping2_req, &c2, 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 2);

  // Goodbye c1 -> server
  MessageGoodbye goodbye(Tenant::GuestTenant,
                         "",
                         MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);
  ASSERT_OK(client.SendRequest(goodbye, &c1, 0));
  ASSERT_TRUE(goodbye_checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);

  StreamID c1_remote, c2_remote;
  {  // When sending back responses, we have to use stream IDs seen by the
     // server loop.
    std::lock_guard<std::mutex> lock(inbound_stream_mutex);
    ASSERT_EQ(2, inbound_stream.size());
    c1_remote = inbound_stream[0];
    c2_remote = inbound_stream[1];
  }

  // Ping response server -> c1
  MessagePing ping1_res(Tenant::GuestTenant, MessagePing::Response, "c1");
  ASSERT_OK(server.SendResponse(ping1_res, c1_remote, 0));
  // Should NOT get response -- c1 has said goodbye
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);

  // Ping response server -> c2
  MessagePing ping2_res(Tenant::GuestTenant, MessagePing::Response, "c2");
  ASSERT_OK(server.SendResponse(ping2_res, c2_remote, 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_EQ(server.GetNumClientsSync(), 1);
}

TEST(Messaging, WaitUntilRunningFailure) {
  // Check that WaitUntilRunning returns failure.
  MsgLoop loop1(env_, env_options_, 58499, 1, info_log_, "loop1");
  env_->StartThread([&]() { loop1.Run(); }, "loop1");
  ASSERT_OK(loop1.WaitUntilRunning());

  // now start another on same port, should fail.
  MsgLoop loop2(env_, env_options_, 58499, 1, info_log_, "loop2");
  env_->StartThread([&]() { loop2.Run(); }, "loop2");
  ASSERT_TRUE(!loop2.WaitUntilRunning().ok());
}

TEST(Messaging, SocketDeath) {
  // Tests that a client cannot unwillingly send messages on the same logical
  // stream, if the stream has broken during transmission.
  MsgLoop receiver_loop(
      env_, env_options_, 58499, 1, info_log_, "receiver_loop0");
  env_->StartThread([&]() { receiver_loop.Run(); }, "receiver_loop0");

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mPing] = [&](std::unique_ptr<Message> msg) {
    auto ping = static_cast<MessagePing*>(msg.get());
    ASSERT_EQ("expected", ping->GetCookie());
    checkpoint.Post();
  };

  // Sender loop.
  MsgLoop sender_loop(env_, env_options_, 0, 1, info_log_, "sender_loop");
  sender_loop.RegisterCallbacks(callbacks);
  env_->StartThread([&]() { sender_loop.Run(); }, "sender_loop");
  ASSERT_OK(sender_loop.WaitUntilRunning());
  ASSERT_OK(receiver_loop.WaitUntilRunning());

  // Receiver client ID.
  auto receiver_client_id = receiver_loop.GetClientId(0);

  // Create logical stream and corresponding socket1.
  StreamSocket socket1(receiver_client_id, "socket1");

  // Send a ping from sender_loop on socket1.
  MessagePing ping0(
      Tenant::GuestTenant, MessagePing::PingType::Request, "expected");
  ASSERT_OK(sender_loop.SendRequest(ping0, &socket1, 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Kill the receiver loop
  receiver_loop.Stop();
  // Restart receiver_loop with the same parameters.
  MsgLoop receiver_loop1(
      env_, env_options_, 58499, 1, info_log_, "receiver_loop1");
  env_->StartThread([&]() { receiver_loop1.Run(); }, "receiver_loop1");
  receiver_loop1.RegisterCallbacks(callbacks);
  ASSERT_OK(receiver_loop1.WaitUntilRunning());

  // Send a ping from sender_loop on socket1.
  MessagePing ping1(Tenant::GuestTenant, MessagePing::PingType::Request, "bad");
  ASSERT_OK(sender_loop.SendRequest(ping1, &socket1, 0));

  // Create logical stream and corresponding socket2 with.
  StreamSocket socket2(receiver_client_id, "socket2");

  // Send a ping from sender_loop on socket2.
  MessagePing ping2(
      Tenant::GuestTenant, MessagePing::PingType::Request, "expected");
  ASSERT_OK(sender_loop.SendRequest(ping2, &socket2, 0));

  // Only the last ping shall get through.
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
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
