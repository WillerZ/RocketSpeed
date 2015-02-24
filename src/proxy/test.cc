//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <unistd.h>
#include <chrono>
#include <string>
#include <vector>

#include "src/test/test_cluster.h"
#include "src/proxy/proxy.h"
#include "src/util/testharness.h"
#include "src/port/port.h"

namespace rocketspeed {

class ProxyTest {
 public:
  ProxyTest() {
    ASSERT_OK(test::CreateLogger(Env::Default(), "ProxyTest", &info_log));
    cluster.reset(new LocalTestCluster(info_log, true, true, true));
    ASSERT_OK(cluster->GetStatus());

    // Create proxy.
    ProxyOptions opts;
    opts.info_log = info_log;
    opts.conf.reset(Configuration::Create(cluster->GetPilotHostIds(),
                                          cluster->GetCopilotHostIds(),
                                          Tenant::GuestTenant,
                                          1));
    ASSERT_OK(Proxy::CreateNewInstance(std::move(opts), &proxy));
  }

  std::shared_ptr<Logger> info_log;
  std::unique_ptr<LocalTestCluster> cluster;
  std::unique_ptr<Proxy> proxy;
};

TEST(ProxyTest, Publish) {
  // Start the proxy.
  // We're going to publish a message and expect an ack in return.
  const ClientID our_client = "proxy_client";
  port::Semaphore checkpoint;

  std::atomic<int64_t> expected_session;
  auto on_message = [&] (int64_t session, std::string data) {
    ASSERT_EQ(session, expected_session.load());
    std::unique_ptr<Message> msg =
      Message::CreateNewInstance(Slice(data).ToUniqueChars(), data.size());
    ASSERT_TRUE(msg != nullptr);
    ASSERT_TRUE(msg->GetMessageType() == MessageType::mDataAck);
    checkpoint.Post();
  };
  proxy->Start(on_message, nullptr);

  // Send a publish message.
  std::string serial;
  MessageData publish(MessageType::mPublish,
                      Tenant::GuestTenant,
                      our_client,
                      Slice("topic"),
                      101,
                      Slice("payload"));
  publish.SerializeToString(&serial);

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
}

TEST(ProxyTest, SeqnoError) {
  // Start the proxy.
  // We're going to ping an expect an error.
  port::Semaphore checkpoint;
  auto on_disconnect = [&] (std::vector<int64_t> session) {
    checkpoint.Post();
  };
  proxy->Start(nullptr, on_disconnect);

  std::string serial;
  MessagePing ping(Tenant::GuestTenant,
                   MessagePing::PingType::Request,
                   "client");
  ping.SerializeToString(&serial);

  const int64_t session = 123;

  // Send to proxy on seqno 999999999. Will be out of buffer space and fail.
  // Should get the on_disconnect_ error.
  ASSERT_OK(proxy->Forward(serial, session, 999999999));
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::seconds(1)));
}

TEST(ProxyTest, DestroySession) {
  // Start the proxy.
  // We're going to ping an expect an error.
  port::Semaphore checkpoint;
  auto on_message = [&] (int64_t session, std::string data) {
    checkpoint.Post();
  };
  proxy->Start(on_message, nullptr);

  std::string serial;
  MessagePing ping(Tenant::GuestTenant,
                   MessagePing::PingType::Request,
                   "client");
  ping.SerializeToString(&serial);

  const int64_t session = 123;

  // Send to proxy then await response.
  ASSERT_OK(proxy->Forward(serial, session, 0));
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));

  // Now destroy, and send at seqno 1. Should not get response.
  proxy->DestroySession(session);
  ASSERT_OK(proxy->Forward(serial, session, 1));
  ASSERT_TRUE(!checkpoint.TimedWait(std::chrono::milliseconds(100)));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
