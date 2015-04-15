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

class ProxyTest {
 public:
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
    opts.conf.reset(Configuration::Create(cluster->GetPilotHostIds(),
                                          cluster->GetCopilotHostIds(),
                                          Tenant::GuestTenant,
                                          4));
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
  const StreamID expected_stream = 0;
  std::atomic<int64_t> expected_session;
  auto on_message = [&](int64_t session, std::string data) {
    ASSERT_EQ(session, expected_session.load());

    StreamID stream;
    Slice in(data.data(), data.size());
    DecodeOrigin(&in, &stream);
    ASSERT_EQ(expected_stream, stream);

    std::unique_ptr<Message> msg =
        Message::CreateNewInstance(in.ToUniqueChars(), in.size());
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
  serial = EncodeOrigin(expected_stream).append(serial);

  const int64_t session = 123;
  expected_session = session;

  // Send through proxy to pilot. Pilot should respond and proxy will send
  // serialized response to on_message defined above.
  ASSERT_OK(proxy->Forward(serial, session, -1));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Now try some out of order messages.

  ASSERT_OK(proxy->Forward(serial, session, 1));
  // should not arrive
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));

  ASSERT_OK(proxy->Forward(serial, session, 2));
  // should not arrive
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));

  ASSERT_OK(proxy->Forward(serial, session, 0));
  // all three should arrive
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));

  expected_session = session + 1;
  ASSERT_OK(proxy->Forward(serial, session + 1, 0));
  // different session, should arrive
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // It doesn't mean that on_disconnect would not eventually be called, but if
  // called, it is always a thread that handles one of the messages that we were
  // waiting for.
  ASSERT_EQ(0, forcibly_disconnected.load());
}

TEST(ProxyTest, SeqnoError) {
  const StreamID expected_stream = 0;

  // Start the proxy.
  // We're going to ping an expect an error.
  port::Semaphore checkpoint;
  auto on_disconnect = [&](std::vector<int64_t> session) { checkpoint.Post(); };
  proxy->Start(nullptr, on_disconnect);

  std::string serial;
  MessagePing ping(
      Tenant::GuestTenant, MessagePing::PingType::Request, "cookie");
  ping.SerializeToString(&serial);
  serial = EncodeOrigin(expected_stream).append(serial);

  const int64_t session = 123;

  // Send to proxy on seqno 999999999. Will be out of buffer space and fail.
  // Should get the on_disconnect_ error.
  ASSERT_OK(proxy->Forward(serial, session, 999999999));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
}

TEST(ProxyTest, DestroySession) {
  const StreamID expected_stream = 0;

  // Start the proxy.
  // We're going to ping an expect an error.
  port::Semaphore checkpoint;
  auto on_message = [&](int64_t session, std::string data) {
    StreamID stream;
    Slice in(data.data(), data.size());
    DecodeOrigin(&in, &stream);
    ASSERT_EQ(stream, expected_stream);

    checkpoint.Post();
  };
  proxy->Start(on_message, nullptr);

  std::string serial;
  MessagePing ping(
      Tenant::GuestTenant, MessagePing::PingType::Request, "cookie");
  ping.SerializeToString(&serial);
  serial = EncodeOrigin(expected_stream).append(serial);

  const int64_t session = 123;

  // Send to proxy then await response.
  ASSERT_OK(proxy->Forward(serial, session, 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Check that pilot and copilot have at least one client.
  ASSERT_NE(cluster->GetPilot()->GetMsgLoop()->GetNumClientsSync(), 0);
  ASSERT_NE(cluster->GetCopilot()->GetMsgLoop()->GetNumClientsSync(), 0);

  // Now destroy, and send at seqno 1. Should not get response.
  proxy->DestroySession(session);
  ASSERT_OK(proxy->Forward(serial, session, 1));
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));

  // Check that pilot and copilot have no clients.
  ASSERT_EQ(cluster->GetPilot()->GetMsgLoop()->GetNumClientsSync(), 0);
  ASSERT_EQ(cluster->GetCopilot()->GetMsgLoop()->GetNumClientsSync(), 0);
}

TEST(ProxyTest, ServerDown) {
  const StreamID expected_stream = 0;

  // Start the proxy.
  // We're going to ping and expect an error.
  port::Semaphore checkpoint;
  auto on_message = [&](int64_t session, std::string data) {
    StreamID stream;
    Slice in(data.data(), data.size());
    DecodeOrigin(&in, &stream);
    ASSERT_EQ(stream, expected_stream);

    checkpoint.Post();
  };
  port::Semaphore disconnected;
  std::unordered_set<int64_t> disconnected_sessions;
  auto on_disconnect = [&](std::vector<int64_t> sessions) {
    disconnected_sessions.insert(sessions.begin(), sessions.end());
    disconnected.Post();
  };
  proxy->Start(on_message, on_disconnect);

  std::string serial;
  MessagePing ping(
      Tenant::GuestTenant, MessagePing::PingType::Request, "cookie");
  ping.SerializeToString(&serial);
  serial = EncodeOrigin(expected_stream).append(serial);

  // Send to proxy then await response.
  ASSERT_OK(proxy->Forward(serial, 123, 0));
  ASSERT_OK(proxy->Forward(serial, 456, 0));
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
  const StreamID pilot_stream = 0;
  const StreamID copilot_stream = 1;
  ASSERT_TRUE(pilot_stream != copilot_stream);

  // Start the proxy.
  // We're going to talk to pilot and copilot, then say goodbye.
  port::Semaphore checkpoint;
  auto on_message = [&](int64_t session, std::string data) {
    ASSERT_EQ(session, expected_session);

    StreamID stream;
    Slice in(data.data(), data.size());
    DecodeOrigin(&in, &stream);

    // Find the message type.
    std::unique_ptr<Message> message =
        Message::CreateNewInstance(in.ToUniqueChars(), in.size());
    if (!message) {
      ASSERT_TRUE(false);
    }
    switch (message->GetMessageType()) {
      case MessageType::mDataAck:
        // Publish ACKs shall arrive on pilot stream.
        ASSERT_EQ(stream, pilot_stream);
        break;
      case MessageType::mMetadata:
        // Subscribe ACKs shall arrive on copilot stream.
        ASSERT_EQ(stream, copilot_stream);
        break;
      case MessageType::mDeliver:
        // Ignore.
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
                      Slice("topic"),
                      GuestNamespace,
                      Slice("payload"));
  publish.SerializeToString(&publish_serial);

  ASSERT_OK(proxy->Forward(
      EncodeOrigin(pilot_stream).append(publish_serial), expected_session, 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Send subscribe message.
  std::string sub_serial;
  MessageMetadata subscribe(
      Tenant::GuestTenant,
      MessageMetadata::MetaType::Request,
      {TopicPair(1, "topic", MetadataType::mSubscribe, GuestNamespace)});
  subscribe.SerializeToString(&sub_serial);

  ASSERT_OK(proxy->Forward(
      EncodeOrigin(copilot_stream).append(sub_serial), expected_session, 1));
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
  ASSERT_OK(proxy->Forward(
      EncodeOrigin(pilot_stream).append(goodbye_serial), expected_session, 2));
  env->SleepForMicroseconds(50000);  // time to propagate

  // Check that one stream is gone.
  ASSERT_EQ(cockpit_loop->GetNumClientsSync(), clients_num - 1);

  // Send goodbye message on behalf on the subscriber.
  ASSERT_OK(proxy->Forward(EncodeOrigin(copilot_stream).append(goodbye_serial),
                           expected_session,
                           2));
  env->SleepForMicroseconds(50000);  // time to propagate

  // Check that another stream is gone.
  ASSERT_EQ(cockpit_loop->GetNumClientsSync(), clients_num - 2);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
