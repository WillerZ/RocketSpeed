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
#include "src/util/common/flow_control.h"
#include "src/util/common/guid_generator.h"
#include "src/util/common/multi_producer_queue.h"

namespace rocketspeed {

class Messaging {
 public:
  Messaging() : timeout_(1) {
    env_ = Env::Default();
    ASSERT_OK(test::CreateLogger(env_, "MessagesTest", &info_log_));
  }

  const std::chrono::seconds timeout_;
  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;
};

TEST(Messaging, Data) {
  Slice name1("Topic1");
  Slice payload1("Payload1");
  HostId host1(HostId::CreateLocal(1234));
  NamespaceID nsid1 = GuestNamespace;

  // create a message
  MessageData data1(MessageType::mPublish,
                    Tenant::GuestTenant, name1, nsid1, payload1);
  data1.SetSequenceNumbers(1000100010001000ULL, 2000200020002000ULL);

  // serialize the message
  std::string str;
  Status stx = data1.Serialize(&str);

  // un-serialize to a new message
  MessageData data2;
  Slice slx(str);
  data2.DeSerialize(&slx);

  // verify that the new message is the same as original
  ASSERT_TRUE(data2.GetMessageId() == data1.GetMessageId());
  ASSERT_EQ(data2.GetTopicName().ToString(), name1.ToString());
  ASSERT_EQ(data2.GetPayload().ToString(), payload1.ToString());
  ASSERT_EQ(data2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(data2.GetNamespaceId().ToString(), nsid1);
  ASSERT_EQ(data2.GetPrevSequenceNumber(), 1000100010001000ULL);
  ASSERT_EQ(data2.GetSequenceNumber(), 2000200020002000ULL);
}

TEST(Messaging, DataAck) {
  HostId hostid(HostId::CreateLocal(200));

  // create a message
  MessageDataAck::AckVector acks(10);
  char value = 0;
  for (auto& ack : acks) {
    for (size_t i = 0; i < sizeof(acks[0].msgid.id); ++i) {
      ack.msgid.id[i] = value++;
      ack.seqno = i;
    }
  }
  MessageDataAck ack1(101, acks);

  // serialize the message
  std::string str;
  ack1.Serialize(&str);

  // un-serialize to a new message
  MessageDataAck ack2;
  Slice slx(str);
  ack2.DeSerialize(&slx);

  // verify that the new message is the same as original
  ASSERT_TRUE(ack1.GetAcks() == ack2.GetAcks());
}

TEST(Messaging, DataGap) {
  // create a message
  MessageGap gap1(Tenant::GuestTenant,
                  "guest",
                  "topic",
                  GapType::kDataLoss,
                  100,
                  200);

  // serialize the message
  std::string stx;
  gap1.Serialize(&stx);

  // un-serialize to a new message
  MessageGap gap2;
  Slice slx(stx);
  gap2.DeSerialize(&slx);

  // verify that the new message is the same as original
  ASSERT_EQ(gap2.GetTopicName(), "topic");
  ASSERT_EQ(gap2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(gap2.GetNamespaceId(), "guest");
  ASSERT_EQ(gap2.GetStartSequenceNumber(), 100);
  ASSERT_EQ(gap2.GetEndSequenceNumber(), 200);
}

static void TestMessage(const Message& msg) {
  std::string str;
  msg.SerializeToString(&str);
  Slice slice(str);

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
  // create a message
  MessageGoodbye goodbye1(Tenant::GuestTenant,
                          MessageGoodbye::Code::Graceful,
                          MessageGoodbye::Server);

  // serialize the message
  std::string str;
  goodbye1.Serialize(&str);

  // un-serialize to a new message
  MessageGoodbye goodbye2;
  Slice slx(str);
  goodbye2.DeSerialize(&slx);

  // verify that the new message is the same as original
  ASSERT_EQ(goodbye2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(goodbye2.GetCode(), goodbye1.GetCode());
  ASSERT_EQ(goodbye2.GetOriginType(), goodbye1.GetOriginType());
}

TEST(Messaging, FindTailSeqno) {
  MessageFindTailSeqno msg1(Tenant::GuestTenant,
                            "TestNamespace",
                            "TestTopic");
  std::string str;
  msg1.Serialize(&str);
  Slice original(str);
  MessageFindTailSeqno msg2;
  msg2.DeSerialize(&original);
  ASSERT_EQ(msg2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(msg2.GetNamespace(), msg1.GetNamespace());
  ASSERT_EQ(msg2.GetTopicName(), msg1.GetTopicName());
}

TEST(Messaging, TailSeqno) {
  MessageTailSeqno msg1(Tenant::GuestTenant,
                        "TestNamespace",
                        "TestTopic",
                        123123);
  std::string str;
  msg1.Serialize(&str);
  Slice original(str);
  MessageTailSeqno msg2;
  msg2.DeSerialize(&original);
  ASSERT_EQ(msg2.GetTenantID(), (TenantID)Tenant::GuestTenant);
  ASSERT_EQ(msg2.GetNamespace(), msg1.GetNamespace());
  ASSERT_EQ(msg2.GetTopicName(), msg1.GetTopicName());
  ASSERT_EQ(msg2.GetSequenceNumber(), msg1.GetSequenceNumber());
}

TEST(Messaging, MessageSubscribe) {
  MessageSubscribe msg1(Tenant::GuestTenant,
                              GuestNamespace,
                              "MessageSubscribe",
                              123123,
                              42);

  std::string str;
  msg1.Serialize(&str);
  Slice slx(str);
  MessageSubscribe msg2;
  ASSERT_OK(msg2.DeSerialize(&slx));

  ASSERT_EQ(msg1.GetMessageType(), msg2.GetMessageType());
  ASSERT_EQ(msg1.GetTenantID(), msg2.GetTenantID());
  ASSERT_EQ(msg1.GetNamespace(), msg2.GetNamespace());
  ASSERT_EQ(msg1.GetTopicName(), msg2.GetTopicName());
  ASSERT_EQ(msg1.GetStartSequenceNumber(), msg2.GetStartSequenceNumber());
  ASSERT_EQ(msg1.GetSubID(), msg2.GetSubID());
}

TEST(Messaging, MessageUnsubscribe) {
  MessageUnsubscribe msg1(Tenant::GuestTenant,
                          42,
                          MessageUnsubscribe::Reason::kBackOff);

  std::string str;
  msg1.Serialize(&str);
  Slice slx(str);
  MessageUnsubscribe msg2;
  ASSERT_OK(msg2.DeSerialize(&slx));

  ASSERT_EQ(msg1.GetMessageType(), msg2.GetMessageType());
  ASSERT_EQ(msg1.GetTenantID(), msg2.GetTenantID());
  ASSERT_EQ(msg1.GetSubID(), msg2.GetSubID());
}

TEST(Messaging, MessageDeliverGap) {
  MessageDeliverGap msg1(Tenant::GuestTenant, 42, GapType::kRetention);
  msg1.SetSequenceNumbers(1000100010001000ULL, 2000200020002000ULL);

  std::string str;
  msg1.Serialize(&str);
  Slice original(str);
  MessageDeliverGap msg2;
  ASSERT_OK(msg2.DeSerialize(&original));

  ASSERT_EQ(msg1.GetMessageType(), msg2.GetMessageType());
  ASSERT_EQ(msg1.GetTenantID(), msg2.GetTenantID());
  ASSERT_EQ(msg1.GetSubID(), msg2.GetSubID());
  ASSERT_EQ(msg1.GetPrevSequenceNumber(), msg2.GetPrevSequenceNumber());
  ASSERT_EQ(msg1.GetSequenceNumber(), msg2.GetSequenceNumber());
  ASSERT_EQ(msg1.GetGapType(), msg2.GetGapType());
}

TEST(Messaging, MessageDeliverData) {
  MessageDeliverData msg1(Tenant::GuestTenant,
                          42,
                          GUIDGenerator().Generate(),
                          Slice("payload"));
  msg1.SetSequenceNumbers(1000100010001000ULL, 2000200020002000ULL);

  std::string str;
  msg1.Serialize(&str);
  Slice original(str);
  MessageDeliverData msg2;
  ASSERT_OK(msg2.DeSerialize(&original));

  ASSERT_EQ(msg1.GetMessageType(), msg2.GetMessageType());
  ASSERT_EQ(msg1.GetTenantID(), msg2.GetTenantID());
  ASSERT_EQ(msg1.GetSubID(), msg2.GetSubID());
  ASSERT_EQ(msg1.GetPrevSequenceNumber(), msg2.GetPrevSequenceNumber());
  ASSERT_EQ(msg1.GetSequenceNumber(), msg2.GetSequenceNumber());
  ASSERT_TRUE(msg1.GetMessageID() == msg2.GetMessageID());
  ASSERT_EQ(msg1.GetPayload().ToString(), msg2.GetPayload().ToString());
}

TEST(Messaging, InvalidEnum) {
  // create a message
  MessageGoodbye goodbye1(
    Tenant::GuestTenant,
    MessageGoodbye::Code(MessageGoodbye::Code::Graceful + 10) /* invalid */,
    MessageGoodbye::Server);

  // serialize the message
  std::string str;
  goodbye1.Serialize(&str);
  Slice original(str);

  // Should fail to deserialize.
  MessageGoodbye goodbye2;
  ASSERT_TRUE(!goodbye2.DeSerialize(&original).ok());
}

TEST(Messaging, ErrorHandling) {
  // Test that Message::CreateNewInstance handles bad messages.
  TenantID tenant = Tenant::GuestTenant;
  NamespaceID nsid = GuestNamespace;

  MessagePing msg0(tenant, MessagePing::Request, "cookie");
  TestMessage(msg0);

  MessageData msg1(MessageType::mPublish,
                   tenant, "topic", nsid, "payload");
  TestMessage(msg1);

  MessageDataAck::AckVector acks(1);
  MessageDataAck msg3(100, acks);
  TestMessage(msg3);

  MessageGap msg4(tenant, "guest", "topic", kBenign, 100, 200);
  TestMessage(msg4);
}

TEST(Messaging, PingPong) {
  // Create server loop.
  MsgLoop server(env_, env_options_, 0, 1, info_log_, "server");
  ASSERT_OK(server.Initialize());
  MsgLoopThread t1(env_, &server, "server");

  // Posted on every ping message received.
  port::Semaphore ping_sem;

  // Create client to communicate with the server.
  MsgLoop loop(env_, env_options_, 0, 1, info_log_, "client");
  StreamSocket socket(loop.CreateOutboundStream(server.GetHostId(), 0));
  loop.RegisterCallbacks({
      {MessageType::mPing,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         ASSERT_EQ(msg->GetMessageType(), MessageType::mPing);
         ASSERT_EQ(socket.GetStreamID(), origin);
         MessagePing* ping = static_cast<MessagePing*>(msg.get());
         ASSERT_EQ(ping->GetPingType(), MessagePing::PingType::Response);
         ASSERT_EQ(ping->GetCookie(), "cookie");
         ping_sem.Post();
       }},
  });
  ASSERT_OK(loop.Initialize());
  MsgLoopThread t2(env_, &loop, "client");

  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(loop.WaitUntilRunning());

  // Create a message, we'll be sending.
  MessagePing msg(
      Tenant::GuestTenant, MessagePing::PingType::Request, "cookie");

  auto pings_recv = [](MsgLoop& msg_loop) {
    return msg_loop.GetStatisticsSync().GetCounterValue(
        msg_loop.GetName() + ".messages_received.ping");
  };

  // Send a single ping first.
  ASSERT_EQ(pings_recv(loop), 0);
  ASSERT_EQ(pings_recv(server), 0);

  ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  ASSERT_TRUE(ping_sem.TimedWait(timeout_));

  ASSERT_EQ(pings_recv(loop), 1);
  ASSERT_EQ(pings_recv(server), 1);

  // Now send multiple ping messages to server back-to-back.
  const int num_msgs = 100;
  for (int i = 0; i < num_msgs; i++) {
    ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  }
  // Check that all responses were received.
  for (int i = 0; i < num_msgs; ++i) {
    ASSERT_TRUE(ping_sem.TimedWait(timeout_));
  }

  ASSERT_EQ(pings_recv(loop), 1 + num_msgs);
  ASSERT_EQ(pings_recv(server), 1 + num_msgs);
}

TEST(Messaging, SameStreamsOnDifferentSockets) {
  // Posted on any ping message received by the server.
  port::Semaphore server_ping;

  // Queue of messages + origins
  MultiProducerQueue<std::pair<std::unique_ptr<MessagePing>, StreamID>>
    server_pings(3);

  MsgLoop server(env_, env_options_, 0, 1, info_log_, "server");
  server.RegisterCallbacks({
      {MessageType::mPing,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         std::unique_ptr<MessagePing> ping(
             static_cast<MessagePing*>(msg.release()));
         ASSERT_TRUE(server_pings.write(std::move(ping), origin));
         server_ping.Post();
       }},
  });
  ASSERT_OK(server.Initialize());
  MsgLoopThread t1(env_, &server, "server");

  // Posted on any ping message received by any client.
  port::Semaphore client_ping;

  // First client loop.
  MsgLoop client1(env_, env_options_, 0, 1, info_log_, "client1");
  client1.RegisterCallbacks({
      {MessageType::mPing,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         auto ping = static_cast<MessagePing*>(msg.get());
         ASSERT_EQ("stream1", ping->GetCookie());
         client_ping.Post();
       }},
  });
  ASSERT_OK(client1.Initialize());
  MsgLoopThread t2(env_, &client1, "client1");

  // Second client loop.
  MsgLoop client2(env_, env_options_, 0, 1, info_log_, "client2");
  client2.RegisterCallbacks({
      {MessageType::mPing,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         auto ping = static_cast<MessagePing*>(msg.get());
         ASSERT_EQ("stream2", ping->GetCookie());
         client_ping.Post();
       }},
  });
  ASSERT_OK(client2.Initialize());
  MsgLoopThread t3(env_, &client2, "client2");

  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client1.WaitUntilRunning());
  ASSERT_OK(client2.WaitUntilRunning());

  // Create two streams on different loops...
  StreamSocket socket1(client1.CreateOutboundStream(server.GetHostId(), 0));
  StreamSocket socket2(client2.CreateOutboundStream(server.GetHostId(), 0));
  // ...that have the same stream ID.
  ASSERT_EQ(socket1.GetStreamID(), socket2.GetStreamID());

  {  // Send a ping from client1 to server.
    MessagePing ping(
        Tenant::GuestTenant, MessagePing::PingType::Request, "stream1");
    ASSERT_OK(client1.SendRequest(ping, &socket1, 0));
    ASSERT_TRUE(server_ping.TimedWait(timeout_));
  }
  {  // Send a ping from client2 to server.
    MessagePing ping(
        Tenant::GuestTenant, MessagePing::PingType::Request, "stream2");
    ASSERT_OK(client2.SendRequest(ping, &socket2, 0));
    ASSERT_TRUE(server_ping.TimedWait(timeout_));
  }

  // Used to assert how many clients the server has seen.
  std::unordered_set<StreamID> seen_by_server;

  {  // Send back pong to client1.
    std::pair<std::unique_ptr<MessagePing>, StreamID> pong;
    ASSERT_TRUE(server_pings.read(pong));
    ASSERT_EQ("stream1", pong.first->GetCookie());
    seen_by_server.insert(pong.second);
    // Send back reponse.
    pong.first->SetPingType(MessagePing::PingType::Response);
    ASSERT_OK(server.SendResponse(*pong.first, pong.second, 0));
    ASSERT_TRUE(client_ping.TimedWait(timeout_));
  }
  {  // Send back pong to client2.
    std::pair<std::unique_ptr<MessagePing>, StreamID> pong;
    ASSERT_TRUE(server_pings.read(pong));
    ASSERT_EQ("stream2", pong.first->GetCookie());
    seen_by_server.insert(pong.second);
    // Send back reponse.
    pong.first->SetPingType(MessagePing::PingType::Response);
    ASSERT_OK(server.SendResponse(*pong.first, pong.second, 0));
    ASSERT_TRUE(client_ping.TimedWait(timeout_));
  }

  ASSERT_EQ(2, seen_by_server.size());
}

TEST(Messaging, MultipleStreamsOneSocket) {
  static const int kNumStreams = 10;

  // Server loop.
  MsgLoop server(env_, env_options_, 0, 1, info_log_, "server");
  ASSERT_OK(server.Initialize());
  MsgLoopThread t1(env_, &server, "server");

  // Clients loop.
  MsgLoop client(env_, env_options_, 0, 1, info_log_, "client");

  // Create a bunch of sockets for different streams.
  std::vector<StreamSocket> sockets;
  for (int i = 0; i < kNumStreams; ++i) {
    sockets.emplace_back(client.CreateOutboundStream(server.GetHostId(), 0));
  }

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  {
    // Callback uses (a copy of) this to count messages.
    int message_num = 0;
    client.RegisterCallbacks({
        {MessageType::mPing,
         [&checkpoint, &sockets, message_num](Flow* flow,
                                              std::unique_ptr<Message> msg,
                                              StreamID origin) mutable {
           int i = message_num++ % kNumStreams;
           ASSERT_LT(i, kNumStreams);
           auto ping = static_cast<MessagePing*>(msg.get());
           ASSERT_EQ(std::to_string(i), ping->GetCookie());
           ASSERT_EQ(sockets[i].GetStreamID(), origin);
           checkpoint.Post();
         }},
    });
  }
  ASSERT_OK(client.Initialize());
  MsgLoopThread t2(env_, &client, "loop-client");

  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client.WaitUntilRunning());

  // Send a pings from client to server.
  for (int i = 0; i < 10 * kNumStreams; ++i) {
    MessagePing msg(Tenant::GuestTenant,
                    MessagePing::PingType::Request,
                    std::to_string(i % kNumStreams));
    ASSERT_OK(client.SendRequest(msg, &sockets[i % sockets.size()], 0));
    ASSERT_TRUE(checkpoint.TimedWait(timeout_));
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
  MsgLoop server(env_, env_options_, 0, 1, info_log_, "server");
  server.RegisterCallbacks({
      {MessageType::mGoodbye,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         goodbye_checkpoint.Post();
       }},
      {MessageType::mPing,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         {
           std::lock_guard<std::mutex> lock(inbound_stream_mutex);
           inbound_stream.push_back(origin);
         }
         auto ping = static_cast<MessagePing*>(msg.get());
         ping->SetPingType(MessagePing::PingType::Response);
         ASSERT_OK(
             server.SendResponse(*ping, origin, server.GetThreadWorkerIndex()));
       }},
  });
  ASSERT_OK(server.Initialize());
  MsgLoopThread t1(env_, &server, "loop-server");

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  std::atomic<int> message_num{0};
  std::string expected_cookies[] = {"c1", "c2", "c2"};
  std::map<MessageType, MsgCallbackType> callbacks = {
      {MessageType::mPing,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
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
  ASSERT_OK(client.Initialize());
  MsgLoopThread t2(env_, &client, "client");

  ASSERT_OK(server.WaitUntilRunning());
  ASSERT_OK(client.WaitUntilRunning());

  // Two streams.
  StreamSocket c1(client.CreateOutboundStream(server.GetHostId(), 0));
  StreamSocket c2(client.CreateOutboundStream(server.GetHostId(), 0));

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


TEST(Messaging, Timer) {
  MsgLoop loop(env_, env_options_, 0, 1, info_log_, "loop");
  std::atomic_int counter1(0);
  std::atomic_int counter2(0);
  ASSERT_OK(loop.Initialize());
  ASSERT_OK(loop.RegisterTimerCallback([&]() { counter1++; },
                           std::chrono::microseconds(30000)));
  ASSERT_OK(loop.RegisterTimerCallback([&]() { counter2++; },
                           std::chrono::microseconds(50000)));

  MsgLoopThread t1(env_, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  // verify the expected number of timer tics after the sleep period
  env_->SleepForMicroseconds(110000);
  ASSERT_EQ(counter1.load(), 3);
  ASSERT_EQ(counter2.load(), 2);
}

TEST(Messaging, InitializeFailure) {
  // Check that Initialize returns failure.
  MsgLoop loop1(env_, env_options_, 0, 1, info_log_, "loop1");
  ASSERT_OK(loop1.Initialize());
  MsgLoopThread t1(env_, &loop1, "loop1");
  ASSERT_OK(loop1.WaitUntilRunning());

  auto port1 = loop1.GetHostId().GetPort();
  ASSERT_NE(0, port1);
  // now start another on same port, should fail.
  MsgLoop loop2(env_, env_options_, port1, 1, info_log_, "loop2");
  ASSERT_TRUE(!loop2.Initialize().ok());
}

TEST(Messaging, SocketDeath) {
  // Tests that a client cannot unwillingly send messages on the same logical
  // stream, if the stream has broken during transmission.
  MsgLoop receiver_loop(env_, env_options_, 0, 1, info_log_, "receiver_loop");
  ASSERT_OK(receiver_loop.Initialize());
  std::unique_ptr<MsgLoopThread> t1(
    new MsgLoopThread(env_, &receiver_loop, "receiver_loop"));

  // Post to the checkpoint when receiving a ping.
  port::Semaphore checkpoint;
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mPing] = [&](
      Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
    auto ping = static_cast<MessagePing*>(msg.get());
    ASSERT_EQ("expected", ping->GetCookie());
    checkpoint.Post();
  };

  // Sender loop.
  MsgLoop sender_loop(env_, env_options_, 0, 1, info_log_, "sender_loop");
  sender_loop.RegisterCallbacks(callbacks);
  ASSERT_OK(sender_loop.Initialize());
  MsgLoopThread t2(env_, &sender_loop, "sender_loop");
  ASSERT_OK(sender_loop.WaitUntilRunning());
  ASSERT_OK(receiver_loop.WaitUntilRunning());

  // Receiver client ID.
  auto receiver_host_id = receiver_loop.GetHostId();

  // Create logical stream and corresponding socket1.
  StreamSocket socket1(sender_loop.CreateOutboundStream(receiver_host_id, 0));

  // Send a ping from sender_loop on socket1.
  MessagePing ping0(
      Tenant::GuestTenant, MessagePing::PingType::Request, "expected");
  ASSERT_OK(sender_loop.SendRequest(ping0, &socket1, 0));
  ASSERT_TRUE(checkpoint.TimedWait(timeout_));

  // Kill the receiver loop
  t1.reset();

  // Restart receiver_loop with the same parameters.
  MsgLoop receiver_loop1(env_,
                         env_options_,
                         receiver_host_id.GetPort(),
                         1,
                         info_log_,
                         "receiver_loop1");
  ASSERT_OK(receiver_loop1.Initialize());
  MsgLoopThread t3(env_, &receiver_loop1, "receiver_loop1");
  receiver_loop1.RegisterCallbacks(callbacks);
  ASSERT_OK(receiver_loop1.WaitUntilRunning());

  // It has to listen on the same address.
  ASSERT_TRUE(receiver_loop1.GetHostId() == receiver_host_id);

  // Send a ping from sender_loop on socket1.
  MessagePing ping1(Tenant::GuestTenant, MessagePing::PingType::Request, "bad");
  ASSERT_OK(sender_loop.SendRequest(ping1, &socket1, 0));

  // Create logical stream and corresponding socket2 with.
  StreamSocket socket2(sender_loop.CreateOutboundStream(receiver_host_id, 0));

  // Send a ping from sender_loop on socket2.
  MessagePing ping2(
      Tenant::GuestTenant, MessagePing::PingType::Request, "expected");
  ASSERT_OK(sender_loop.SendRequest(ping2, &socket2, 0));

  // Only the last ping shall get through.
  ASSERT_TRUE(checkpoint.TimedWait(timeout_));
}

TEST(Messaging, GatherTest) {
  MsgLoop loop(env_, env_options_, -1, 10, info_log_, "loop");
  ASSERT_OK(loop.Initialize());
  MsgLoopThread t1(env_, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  port::Semaphore done;
  int n = 0;

  // Simple gather test that simple sums up the worker indices.
  ASSERT_OK(loop.Gather([] (int i) { return i; },
                        [&] (std::vector<int> v) {
                          n = std::accumulate(v.begin(), v.end(), 0);
                          done.Post();
                        }));
  ASSERT_TRUE(done.TimedWait(timeout_));
  ASSERT_EQ(n, 45); // 45 = 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9
}

TEST(Messaging, TimeoutTest) {
  // Initialize, but don't start loop.
  MsgLoop loop(env_, env_options_, -1, 4, info_log_, "loop");
  ASSERT_OK(loop.Initialize());

  int result = 0;
  Status st = loop.WorkerRequestSync([](){ return 1; },
                                     2,
                                     &result,
                                     std::chrono::seconds(1));
  ASSERT_TRUE(st.IsTimedOut());
  ASSERT_EQ(st.ToString(), "Timed out: Queue size = 1");

  st = loop.MapReduceSync([](int) { return 1; },
                          [](std::vector<int>) { return 1; },
                          &result,
                          std::chrono::seconds(1));
  ASSERT_TRUE(st.IsTimedOut());
  ASSERT_EQ(st.ToString(), "Timed out: Queue sizes = 1, 1, 2, 1");
}

TEST(Messaging, ConnectTimeout) {
  MsgLoop::Options opts;
  opts.event_loop.connect_timeout = std::chrono::milliseconds(200);
  MsgLoop loop(env_, env_options_, -1, 1, info_log_, "loop", opts);
  ASSERT_OK(loop.Initialize());

  // Register callback for a goodbye message.
  port::Semaphore goodbye;
  std::map<MessageType, MsgCallbackType> callbacks = {
      {MessageType::mGoodbye,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         goodbye.Post();
       }},
  };
  loop.RegisterCallbacks(callbacks);

  // Start loop.
  MsgLoopThread t1(env_, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  // Send a message to a bad host (10.0.0.0 is non-routable).
  HostId bad_host;
  ASSERT_OK(HostId::CreateFromIP("10.0.0.0", 666, &bad_host));
  StreamSocket socket(loop.CreateOutboundStream(bad_host, 0));
  MessagePing msg(Tenant::GuestTenant, MessagePing::PingType::Request, "ping");
  ASSERT_OK(loop.SendRequest(msg, &socket, 0));

  // Check that we receive a goodbye, but not immediately.
  ASSERT_TRUE(!goodbye.TimedWait(opts.event_loop.connect_timeout / 2));
  ASSERT_TRUE(goodbye.TimedWait(opts.event_loop.connect_timeout * 2));
}

TEST(Messaging, FlowControlOnDelivery) {
  MsgLoop::Options opts;
  MsgLoop loop(env_, env_options_, 0, 1, info_log_, "loop", opts);
  ASSERT_OK(loop.Initialize());

  // Create a test sink of capacity 1.
  auto queue = std::make_shared<Queue<int>>(
      info_log_, loop.GetEventLoop(0)->GetQueueStats(), 1);

  // Register callback for a goodbye message.
  port::Semaphore delivered;
  std::map<MessageType, MsgCallbackType> callbacks = {
      {MessageType::mPing,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID origin) {
         int value = 0;
         flow->Write(queue.get(), value);
         delivered.Post();
       }},
  };
  loop.RegisterCallbacks(callbacks);

  // Start loop.
  MsgLoopThread t1(env_, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  // We'll be sending messages to self.
  StreamSocket socket(loop.CreateOutboundStream(loop.GetHostId(), 0));
  MessagePing msg(Tenant::GuestTenant, MessagePing::PingType::Request, "ping");

  // Send a message twice, flow control will kick after the second one.
  ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  ASSERT_TRUE(delivered.TimedWait(timeout_));
  ASSERT_TRUE(delivered.TimedWait(timeout_));

  // Send a message again, since the backpressure was applied, it won't be
  // delivered.
  ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  ASSERT_TRUE(!delivered.TimedWait(std::chrono::milliseconds(100)));

  // Finally register the queue, so that messages will be read from it.
  auto queue_cb = [](Flow* flow, int value) {
    // Ignore, we only want to free up the queue.
  };
  FlowControl flow_control("queue", loop.GetEventLoop(0));
  std::unique_ptr<Command> cmd(MakeExecuteCommand(std::bind(
      &FlowControl::Register<int>, &flow_control, queue.get(), queue_cb)));
  loop.SendCommand(std::move(cmd), 0);
  // The backpressure will eventually be removed and all messages should be
  // delivered.
  ASSERT_TRUE(delivered.TimedWait(timeout_));

  // Send a message again it will be delivered, as there is no backpressure.
  ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  ASSERT_TRUE(delivered.TimedWait(timeout_));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
