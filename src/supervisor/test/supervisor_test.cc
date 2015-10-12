// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS

#include <memory>

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

  std::shared_ptr<SupervisorLoop> MakeSupervisor(LocalTestCluster* cluster);
  std::string DoRequest(std::string request);

 protected:
  Env* env_;
  std::shared_ptr<Logger> info_log_;
  std::weak_ptr<SupervisorLoop> last_supervisor_;
};

std::shared_ptr<SupervisorLoop> SupervisorTest::MakeSupervisor(
    LocalTestCluster* cluster) {
  // We allow to create a single supervisor per test, but do not own it.
  ASSERT_TRUE(last_supervisor_.expired());

  SupervisorOptions options{0,  // Automatically allocate the port.
                            info_log_,
                            cluster->GetControlTower(),
                            cluster->GetPilot(),
                            cluster->GetCopilot()};

  std::unique_ptr<SupervisorLoop> supervisor;
  ASSERT_OK(SupervisorLoop::CreateNewInstance(options, &supervisor));
  ASSERT_OK(supervisor->Initialize());

  std::shared_ptr<SupervisorLoop> supervisor_shared = std::move(supervisor);
  last_supervisor_ = supervisor_shared;
  return supervisor_shared;
}

std::string SupervisorTest::DoRequest(std::string request) {
  auto supervisor = last_supervisor_.lock();
  ASSERT_TRUE(supervisor);

  std::unique_ptr<Connection> connection;
  env_->NewConnection("localhost",
                      supervisor->GetHostId().GetPort(),
                      true,
                      &connection,
                      EnvOptions());
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
  opts.tower.readers_per_room = 1;
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
    "Log(1).tail_seqno_cached: 0\n"
    "Log(1).reader[1].start_seqno: %" PRIu64 "\n"
    "Log(1).reader[1].last_read: %" PRIu64 "\n"
    "Log(1).reader[1].num_topics_subscribed: 1\n\n",
    seqno,
    seqno);
  ASSERT_EQ(std::string(expected), DoRequest("info tower log 1\n"));

  snprintf(expected, sizeof(expected),
    "Log(1).reader[1].start_seqno: %" PRIu64 "\n"
    "Log(1).reader[1].last_read: %" PRIu64 "\n"
    "Log(1).reader[1].num_topics_subscribed: 1\n\n",
    seqno,
    seqno);
  ASSERT_EQ(std::string(expected), DoRequest("info tower logs\n"));

  ASSERT_EQ(cluster.GetControlTower()->GetHostId().ToString() + "\n\n",
    DoRequest("info copilot towers_for_log 1\n"));
  ASSERT_EQ("1\n", DoRequest("info copilot log_for_topic guest TowerLog\n"));

  snprintf(expected, sizeof(expected),
    "Topic(guest,TowerLog).log_id: 1\n"
    "Topic(guest,TowerLog).subscription_count: 1\n"
    "Topic(guest,TowerLog).records_sent: 1\n"
    "Topic(guest,TowerLog).gaps_sent: 0\n"
    "Topic(guest,TowerLog).tower[0].next_seqno: %" PRIu64 "\n\n",
    seqno + 1);
  ASSERT_EQ(std::string(expected),
    DoRequest("info copilot subscriptions Tower\n"));
  ASSERT_EQ(std::string(expected),
    DoRequest("info copilot subscriptions Tower 1\n"));
  ASSERT_EQ(std::string(expected),
    DoRequest("info copilot subscriptions Tower 100\n"));
  ASSERT_EQ("\n", DoRequest("info copilot subscriptions 0\n"));
  ASSERT_EQ("\n", DoRequest("info copilot subscriptions foo\n"));

  ASSERT_EQ(std::to_string(seqno + 1) + "\n",
    DoRequest("info tower tail_seqno 1\n"));


  supervisor->Stop();
  env_->WaitForJoin(supervisor_thread_id);
}

} // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
