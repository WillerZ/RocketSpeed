//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <unistd.h>
#include <chrono>
#include <string>
#include <unordered_set>
#include <vector>

#include "src/test/test_cluster.h"
#include "src/proxy/proxy.h"
#include "src/util/testharness.h"
#include "src/port/port.h"

namespace rocketspeed {

class WrappedMessage {};

TEST(WrappedMessage, Serialization) {
  MessagePing ping(Tenant::GuestTenant, MessagePing::PingType::Request);
  const StreamID stream = 42;
  const MessageSequenceNumber seqno = 1337;
  std::string message;
  ping.SerializeToString(&message);

  // Wrap the message.
  std::string wrapped = WrapMessage(message, stream, seqno);

  // Unwarp it back.
  std::unique_ptr<Message> ping1;
  StreamID stream1;
  MessageSequenceNumber seqno1;
  ASSERT_OK(UnwrapMessage(wrapped, &ping1, &stream1, &seqno1));

  // Message and metadata must match.
  std::string message1;
  ping1->SerializeToString(&message1);
  ASSERT_EQ(message, message1);
  ASSERT_EQ(stream, stream1);
  ASSERT_EQ(seqno, seqno1);
}

class ProxyTest {
 public:
  static const int kOrderingBufferSize = 10;

  ProxyTest() {
    env = Env::Default();
    ASSERT_OK(test::CreateLogger(env, "ProxyTest", &info_log));
    LocalTestCluster::Options cluster_opts;
    cluster_opts.info_log = info_log;
    cluster_opts.copilot.rollcall_enabled = false;
    cluster.reset(new LocalTestCluster(cluster_opts));
    ASSERT_OK(cluster->GetStatus());

    // Create proxy.
    ProxyOptions opts;
    opts.info_log = info_log;
    opts.conf = cluster->GetConfiguration();
    opts.ordering_buffer_size = kOrderingBufferSize;
    ASSERT_OK(Proxy::CreateNewInstance(std::move(opts), &proxy));
  }

  Env* env;
  std::shared_ptr<Logger> info_log;
  std::unique_ptr<LocalTestCluster> cluster;
  std::unique_ptr<Proxy> proxy;
};

TEST(ProxyTest, Publish) {
  // Start the proxy.
  // We're going to publish a message and expect an ack in return.
  port::Semaphore checkpoint;
  const StreamID expected_stream = 1337;
  std::atomic<int64_t> expected_session;
  // This is accessed from on_message callback only.
  std::unordered_map<int64_t, MessageSequenceNumber> expected_seqno;
  auto on_message = [&](int64_t session, std::string data) {
    ASSERT_EQ(session, expected_session.load());

    StreamID stream;
    MessageSequenceNumber seqno;
    std::unique_ptr<Message> msg;
    ASSERT_OK(UnwrapMessage(std::move(data), &msg, &stream, &seqno));

    ASSERT_EQ(expected_stream, stream);
    ASSERT_EQ(expected_seqno[session]++, seqno);

    ASSERT_TRUE(msg != nullptr);
    ASSERT_EQ(MessageType::mDataAck, msg->GetMessageType());

    checkpoint.Post();
  };
  std::atomic<size_t> forcibly_disconnected(0);
  auto on_disconnect = [&](const std::vector<int64_t>& sessions) {
    forcibly_disconnected += sessions.size();
  };
  proxy->Start(on_message, on_disconnect);

  // Send a publish message.
  std::string serial;
  MessageData publish(MessageType::mPublish,
                      Tenant::GuestTenant,
                      Slice("topic"),
                      GuestNamespace,
                      Slice("payload"));
  publish.SerializeToString(&serial);

  expected_session = 123;

  // Send through proxy to pilot. Pilot should respond and proxy will send
  // serialized response to on_message defined above.
  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, -1),
                           expected_session));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  // Another message send out of band should pass as well.
  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, -1),
                           expected_session));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Now try some out of order messages.

  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 1),
                           expected_session));
  // should not arrive
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));

  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 2),
                           expected_session));
  // should not arrive
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));

  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 0),
                           expected_session));
  // all three should arrive
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));

  expected_session = expected_session + 1;
  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 0),
                           expected_session));
  // different session, should arrive
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // It doesn't mean that on_disconnect would not eventually be called, but if
  // called, it is always a thread that handles one of the messages that we were
  // waiting for.
  ASSERT_EQ(0, forcibly_disconnected.load());
}

TEST(ProxyTest, SeqnoError) {
  const StreamID expected_stream = 1337;

  // Start the proxy.
  // We're going to ping an expect an error.
  port::Semaphore checkpoint;
  auto on_disconnect = [&](std::vector<int64_t> session) { checkpoint.Post(); };
  proxy->Start(nullptr, on_disconnect);

  MessagePing ping(Tenant::GuestTenant, MessagePing::PingType::Request);
  std::string serial;
  ping.SerializeToString(&serial);

  const int64_t session = 123;

  // Send to proxy on out of range seqnos. Will run out of space and fail.
  // Should get the on_disconnect_ error.
  for (int i = 0; i < kOrderingBufferSize + 1; ++i) {
    ASSERT_OK(
      proxy->Forward(WrapMessage(serial, expected_stream, 99999 + i), session));
  }
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
}

TEST(ProxyTest, DestroySession) {
  const int64_t expected_session = 123;
  const StreamID expected_stream = 1337;

  // Start the proxy.
  // We're going to ping an expect an error.
  port::Semaphore checkpoint;
  // This is accessed from on_message callback only.
  std::unordered_map<int64_t, MessageSequenceNumber> expected_seqno;
  auto on_message = [&](int64_t session, std::string data) {
    ASSERT_EQ(session, expected_session);

    StreamID stream;
    MessageSequenceNumber seqno;
    std::unique_ptr<Message> msg;
    ASSERT_OK(UnwrapMessage(std::move(data), &msg, &stream, &seqno));

    ASSERT_EQ(expected_stream, stream);
    ASSERT_EQ(expected_seqno[session]++, seqno);

    checkpoint.Post();
  };
  proxy->Start(on_message, nullptr);

  MessagePing ping(Tenant::GuestTenant, MessagePing::PingType::Request);
  std::string serial;
  ping.SerializeToString(&serial);

  // Send to proxy then await response.
  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 0),
                           expected_session));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Check that pilot and copilot have at least one client.
  ASSERT_NE(cluster->GetPilot()->GetMsgLoop()->GetNumClientsSync(), 0);
  ASSERT_NE(cluster->GetCopilot()->GetMsgLoop()->GetNumClientsSync(), 0);

  // Now destroy, and send at seqno 1. Should not get response.
  proxy->DestroySession(expected_session);
  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 1),
                           expected_session));
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // Check that pilot and copilot have no clients.
  ASSERT_EQ(cluster->GetPilot()->GetMsgLoop()->GetNumClientsSync(), 0);
  ASSERT_EQ(cluster->GetCopilot()->GetMsgLoop()->GetNumClientsSync(), 0);
}

TEST(ProxyTest, ServerDown) {
  const StreamID expected_stream = 1337;

  // Start the proxy.
  // We're going to ping and expect an error.
  port::Semaphore checkpoint;
  // This is accessed from on_message callback only.
  std::unordered_map<int64_t, MessageSequenceNumber> expected_seqno;
  auto on_message = [&](int64_t session, std::string data) {
    StreamID stream;
    MessageSequenceNumber seqno;
    std::unique_ptr<Message> msg;
    ASSERT_OK(UnwrapMessage(std::move(data), &msg, &stream, &seqno));

    ASSERT_EQ(expected_stream, stream);
    ASSERT_EQ(expected_seqno[session]++, seqno);

    checkpoint.Post();
  };
  port::Semaphore disconnected;
  std::unordered_set<int64_t> disconnected_sessions;
  auto on_disconnect = [&](std::vector<int64_t> sessions) {
    disconnected_sessions.insert(sessions.begin(), sessions.end());
    disconnected.Post();
  };
  proxy->Start(on_message, on_disconnect);

  MessagePing ping(Tenant::GuestTenant, MessagePing::PingType::Request);
  std::string serial;
  ping.SerializeToString(&serial);

  // Send to proxy then await response.
  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 0), 123));
  ASSERT_OK(proxy->Forward(WrapMessage(serial, expected_stream, 0), 456));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Now destroy cluster.
  cluster.reset(nullptr);

  // Disconnect callback should be called twice.
  ASSERT_TRUE(disconnected.TimedWait(std::chrono::seconds(1)));
  ASSERT_TRUE(disconnected.TimedWait(std::chrono::seconds(1)));

  // Verify that all sessions where disconnected.
  ASSERT_EQ(disconnected_sessions.size(), 2);
  ASSERT_EQ(1, disconnected_sessions.count(123));
  ASSERT_EQ(1, disconnected_sessions.count(456));
}

TEST(ProxyTest, ForwardGoodbye) {
  const int64_t expected_session = 123;
  const StreamID pilot_stream = 123;
  const StreamID copilot_stream = 321;
  ASSERT_TRUE(pilot_stream != copilot_stream);

  // Start the proxy.
  // We're going to talk to pilot and copilot, then say goodbye.
  port::Semaphore checkpoint;
  // This is accessed from on_message callback only.
  std::unordered_map<int64_t, MessageSequenceNumber> expected_seqno;
  auto on_message = [&](int64_t session, std::string data) {
    ASSERT_EQ(session, expected_session);

    StreamID stream;
    MessageSequenceNumber seqno;
    std::unique_ptr<Message> message;
    ASSERT_OK(UnwrapMessage(std::move(data), &message, &stream, &seqno));

    ASSERT_EQ(expected_seqno[session]++, seqno);

    // Find the message type.
    if (!message) {
      ASSERT_TRUE(false);
    }
    switch (message->GetMessageType()) {
      case MessageType::mDataAck:
        // Publish ACKs shall arrive on pilot stream.
        ASSERT_EQ(stream, pilot_stream);
        break;
      case MessageType::mUnsubscribe:
      case MessageType::mDeliverGap:
      case MessageType::mDeliverData:
        // Unsubscriptions, data and gaps shall arrive on copilot stream.
        ASSERT_EQ(stream, copilot_stream);
        break;
      default:
        ASSERT_TRUE(false);
    }

    checkpoint.Post();
  };
  proxy->Start(on_message, nullptr);

  // Send a publish message.
  std::string publish_serial;
  MessageData publish(MessageType::mPublish,
                      Tenant::GuestTenant,
                      Slice("ForwardGoodbye"),
                      GuestNamespace,
                      Slice("payload"));
  publish.SerializeToString(&publish_serial);

  ASSERT_OK(proxy->Forward(WrapMessage(publish_serial, pilot_stream, 0),
                           expected_session));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Send subscribe message.
  std::string sub_serial;
  MessageSubscribe subscribe(Tenant::GuestTenant,
                             GuestNamespace,
                             "ForwardGoodbye",
                             1,
                             123);
  subscribe.SerializeToString(&sub_serial);

  ASSERT_OK(proxy->Forward(WrapMessage(sub_serial, copilot_stream, 1),
                           expected_session));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Pilot and copilot share message loop.
  ASSERT_EQ(cluster->GetCopilot()->GetMsgLoop(),
            cluster->GetPilot()->GetMsgLoop());
  const auto cockpit_loop = cluster->GetCopilot()->GetMsgLoop();
  // Check that pilot and copilot knwo about all three streams (one extra for CT
  // connection).
  int clients_num = cockpit_loop->GetNumClientsSync();
  ASSERT_EQ(clients_num, 3);

  // Prepare generic goodbye message.
  std::string goodbye_serial;
  MessageGoodbye goodbye(Tenant::GuestTenant,
                         MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);
  goodbye.SerializeToString(&goodbye_serial);

  // Send goodbye message on behalf on the publisher.
  ASSERT_OK(proxy->Forward(WrapMessage(goodbye_serial, pilot_stream, 2),
                           expected_session));
  env->SleepForMicroseconds(50000);  // time to propagate

  // Check that one stream is gone.
  ASSERT_EQ(cockpit_loop->GetNumClientsSync(), clients_num - 1);

  // Send goodbye message on behalf on the subscriber.
  ASSERT_OK(proxy->Forward(WrapMessage(goodbye_serial, copilot_stream, 3),
                           expected_session));
  env->SleepForMicroseconds(50000);  // time to propagate

  // Check that another stream is gone.
  ASSERT_EQ(cockpit_loop->GetNumClientsSync(), clients_num - 2);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
