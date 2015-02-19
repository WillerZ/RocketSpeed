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

class ProxyTest {};

TEST(ProxyTest, Publish) {
  std::shared_ptr<Logger> info_log;
  ASSERT_OK(test::CreateLogger(Env::Default(), "ProxyTest", &info_log));

  // Cluster with pilot only.
  LocalTestCluster cluster(info_log, false, false, true);
  ASSERT_OK(cluster.GetStatus());

  // Create proxy.
  ProxyOptions opts;
  opts.info_log = info_log;
  opts.conf.reset(Configuration::Create(cluster.GetPilotHostIds(),
                                        {},
                                        Tenant::GuestTenant,
                                        1));
  std::unique_ptr<Proxy> proxy;
  ASSERT_OK(Proxy::CreateNewInstance(std::move(opts), &proxy));

  // Start the proxy.
  // We're going to publish a message and expect an ack in return.
  const ClientID our_client = "proxy_client";
  port::Semaphore checkpoint;

  auto on_message = [&] (const ClientID& client_id, std::string data) {
    ASSERT_EQ(our_client, client_id);
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

  // Send through proxy to pilot. Pilot should respond and proxy will send
  // serialized response to on_message defined above.
  const int64_t session = 123;
  ASSERT_OK(proxy->Forward(serial, session, -1));
  proxy->DestroySession(session);
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
