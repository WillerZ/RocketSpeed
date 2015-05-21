// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
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

 protected:
  Env* env_;
  std::shared_ptr<rocketspeed::Logger> info_log_;
};

TEST(SupervisorTest, Basic) {
  LocalTestCluster cluster(info_log_, true, true, true);
  ASSERT_OK(cluster.GetStatus());

  // create new supervisor instance
  SupervisorOptions options {
    SupervisorLoop::DEFAULT_PORT,
    info_log_,
    cluster.GetControlTower()->GetMsgLoop(),
    cluster.GetPilot()->GetMsgLoop(),
    cluster.GetCopilot()->GetMsgLoop()
  };

  std::unique_ptr<SupervisorLoop> supervisor;

  auto st = SupervisorLoop::CreateNewInstance(options, &supervisor);
  ASSERT_OK(st);

  // start the Supervisor loop
  rocketspeed::Env::ThreadId supervisor_thread_id;

  ASSERT_OK(supervisor->Initialize());

  supervisor_thread_id = env_->StartThread([&]() {
    supervisor->Run();
  });
  ASSERT_OK(supervisor->WaitUntilRunning());

  std::unique_ptr<Connection> connection;
  env_->NewConnection("localhost", 58800, true, &connection, EnvOptions());

  ASSERT_OK(connection->Send("stats pilot\n"));
  char buffer[256];
  bzero(buffer, 256);
  ASSERT_OK(connection->Receive(buffer, (unsigned int) 255));
  std::string str(buffer);
  ASSERT_TRUE(str.find("cockpit.accepts") != std::string::npos);

  // close the supervisor & the connection to it
  connection.reset();
  supervisor->Stop();
  env_->WaitForJoin(supervisor_thread_id);
}

} // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
