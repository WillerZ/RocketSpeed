// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "src/port/Env.h"
#include "src/supervisor/supervisor_loop.h"
#include "src/test/test_cluster.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

namespace rocketspeed {

class SupervisorTest {
 public:
  SupervisorTest()
    : env_(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env_, "SupervisorTest", &info_log_));
  }

  std::unique_ptr<SupervisorLoop> MakeSupervisor(LocalTestCluster* cluster);
  std::string DoRequest(std::string request);

 protected:
  Env* env_;
  std::shared_ptr<rocketspeed::Logger> info_log_;
};

std::unique_ptr<SupervisorLoop>
SupervisorTest::MakeSupervisor(LocalTestCluster* cluster) {
  SupervisorOptions options {
    SupervisorLoop::DEFAULT_PORT,
    info_log_,
    cluster->GetControlTower(),
    cluster->GetPilot(),
    cluster->GetCopilot()
  };

  std::unique_ptr<SupervisorLoop> supervisor;
  auto st = SupervisorLoop::CreateNewInstance(options, &supervisor);
  ASSERT_OK(st);

  ASSERT_OK(supervisor->Initialize());
  return supervisor;
}

std::string SupervisorTest::DoRequest(std::string request) {
  std::unique_ptr<Connection> connection;
  env_->NewConnection("localhost", 58800, true, &connection, EnvOptions());
  ASSERT_TRUE(connection);
  ASSERT_OK(connection->Send(request.c_str()));
  char buffer[4096] = { 0 };
  ssize_t size = sizeof(buffer);
  ASSERT_OK(connection->Receive(buffer, &size));
  connection.reset();
  return std::string(buffer);
}

TEST(SupervisorTest, Basic) {
  LocalTestCluster cluster(info_log_, true, true, true);
  ASSERT_OK(cluster.GetStatus());

  auto supervisor = MakeSupervisor(&cluster);
  rocketspeed::Env::ThreadId supervisor_thread_id;
  supervisor_thread_id = env_->StartThread([&]() {
    supervisor->Run();
  });
  ASSERT_OK(supervisor->WaitUntilRunning());

  std::string str = DoRequest("stats pilot\n");
  ASSERT_TRUE(str.find("cockpit.accepts") != std::string::npos);

  // close the supervisor & the connection to it
  supervisor->Stop();
  env_->WaitForJoin(supervisor_thread_id);
}

TEST(SupervisorTest, TowerLog) {
  LocalTestCluster::Options opts;
  opts.copilot.rollcall_enabled = false;
  opts.info_log = info_log_;
  opts.single_log = true;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log_;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

  // Publish and subscribe
  SequenceNumber seqno;
  port::Semaphore published;
  ASSERT_OK(client->Publish(GuestTenant,
                            "TowerLog",
                            GuestNamespace,
                            TopicOptions(),
                            "test",
                            [&] (std::unique_ptr<ResultStatus> rs) {
                              ASSERT_OK(rs->GetStatus());
                              seqno = rs->GetSequenceNumber();
                              published.Post();
                            }).status);
  ASSERT_TRUE(published.TimedWait(std::chrono::seconds(1)));

  // Listen for the message.
  port::Semaphore received;
  ASSERT_TRUE(client->Subscribe(GuestTenant,
                                GuestNamespace,
                                "TowerLog",
                                seqno,
                                [&] (std::unique_ptr<MessageReceived>& msg) {
                                  received.Post();
                                }));
  ASSERT_TRUE(received.TimedWait(std::chrono::seconds(1)));

  auto supervisor = MakeSupervisor(&cluster);
  rocketspeed::Env::ThreadId supervisor_thread_id;
  supervisor_thread_id = env_->StartThread([&]() {
    supervisor->Run();
  });
  ASSERT_OK(supervisor->WaitUntilRunning());

  char expected[1024];
  snprintf(expected, sizeof(expected),
    "Log(1).start_seqno: %" PRIu64 "\n"
    "Log(1).last_read: %" PRIu64 "\n"
    "Log(1).tail_seqno: 0\n"
    "Log(1).num_subscribers: 1\n"
    "Log(1).num_topics_subscribed: 1\n\n",
    seqno,
    seqno);
  ASSERT_EQ(std::string(expected), DoRequest("info tower log 1\n"));
  ASSERT_EQ(std::string(expected), DoRequest("info tower logs\n"));

  supervisor->Stop();
  env_->WaitForJoin(supervisor_thread_id);
}

} // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
