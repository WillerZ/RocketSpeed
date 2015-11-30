//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <unistd.h>
#include <chrono>
#include <string>
#include <vector>

#include "src/controltower/log_tailer.h"
#include "src/controltower/room.h"
#include "src/controltower/tower.h"
#include "src/port/port.h"
#include "src/test/test_cluster.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class ControlTowerTest {
 public:
  std::chrono::seconds timeout;

  ControlTowerTest()
      : timeout(5), env_(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env_, "ControlTowerTest", &info_log_));
  }

  virtual ~ControlTowerTest() {
    env_->WaitForJoin();  // This is good hygine
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }

  // gets the number of open logs
  int64_t GetNumOpenLogs(ControlTower* ct) const {
    return
      ct->GetStatisticsSync().GetCounterValue("tower.log_tailer.open_logs");
  }
};

TEST(ControlTowerTest, Subscribe) {
  // Create cluster with copilot and controltower.
  LocalTestCluster cluster(info_log_, true, true, false);
  ASSERT_OK(cluster.GetStatus());

  // create a client to communicate with the ControlTower
  MsgLoop loop(env_, env_options_, 0, 1, info_log_, "client");
  StreamSocket socket(
      loop.CreateOutboundStream(cluster.GetControlTower()->GetHostId(), 0));
  // Define a callback to process the subscribe response at the client
  loop.RegisterCallbacks({
      {MessageType::mGap,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mDeliverGap,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
  });
  ASSERT_OK(loop.Initialize());
  MsgLoopThread t1(env_, &loop, "client");
  ASSERT_OK(loop.WaitUntilRunning());

  // create a few topics
  const int num_topics = 5;
  for (int i = 0; i < num_topics; i++) {
    NamespaceID namespace_id = "test" + std::to_string(i);
    Topic topic_name = std::to_string(i);
    MessageSubscribe subscribe(Tenant::GuestTenant,
                               namespace_id,
                               topic_name,
                               SequenceNumber(4 + i),
                               SubscriptionID(i));
    ASSERT_OK(loop.SendRequest(subscribe, &socket, 0));
  }

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto open_logs = GetNumOpenLogs(cluster.GetControlTower());
  ASSERT_NE(open_logs, 0);
  ASSERT_LE(open_logs, num_topics);
}

TEST(ControlTowerTest, MultipleSubscribers) {
  // Create cluster with copilot and controltower.
  LocalTestCluster::Options opts;
  opts.info_log = info_log_;
  opts.start_copilot = true;
  opts.start_controltower = true;
  opts.tower.readers_per_room = 1;
  LocalTestCluster cluster(opts);
  ASSERT_OK(cluster.GetStatus());
  auto ct = cluster.GetControlTower();

  // create a client to communicate with the ControlTower
  MsgLoop loop1(env_, env_options_, 0, 1, info_log_, "loop1");
  StreamSocket socket1(loop1.CreateOutboundStream(ct->GetHostId(), 0));
  loop1.RegisterCallbacks({
      {MessageType::mDeliver,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mGap,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mDeliverGap,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
  });
  ASSERT_OK(loop1.Initialize());
  MsgLoopThread t1(env_, &loop1, "loop1");
  ASSERT_OK(loop1.WaitUntilRunning());

  // first subscriber *******
  MessageSubscribe meta1(Tenant::GuestTenant, "test", "topic", 1, 1);

  // send message to control tower
  ASSERT_OK(loop1.SendRequest(meta1, &socket1, 0));

  // The number of distinct logs that are opened cannot be more than
  // the number of topics.
  // Wait a little, since log opening is async.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  int64_t numopenlogs1 = GetNumOpenLogs(ct);
  ASSERT_EQ(numopenlogs1, 1);

  // create second client to communicate with the ControlTower
  MsgLoop loop2(env_, env_options_, 0, 1, info_log_, "loop2");
  StreamSocket socket2(loop2.CreateOutboundStream(ct->GetHostId(), 0));
  loop2.RegisterCallbacks({
      {MessageType::mDeliver,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mGap,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mDeliverGap,
       [](Flow* flow, std::unique_ptr<Message>, StreamID) {}},
  });
  ASSERT_OK(loop2.Initialize());
  MsgLoopThread t2(env_, &loop2, "loop2");
  ASSERT_OK(loop2.WaitUntilRunning());

  // The second subscriber subscribes to the same topics.
  MessageSubscribe meta2(Tenant::GuestTenant, "test", "topic", 2, 2);

  // send message to control tower
  ASSERT_OK(loop2.SendRequest(meta2, &socket2, 0));

  // The control tower should not have re-opened any of those logs
  // because they were already subscribed to by the first client.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(numopenlogs1, GetNumOpenLogs(ct));

  // Unsubscribe all the topics from the first client.
  MessageUnsubscribe meta3(
    Tenant::GuestTenant, 1, MessageUnsubscribe::Reason::kRequested);

  // send message to control tower
  ASSERT_OK(loop1.SendRequest(meta3, &socket1, 0));

  // The number of open logs should not change because the second
  // client still has a live subscription for all those topics.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(numopenlogs1, GetNumOpenLogs(ct));

  // Finally, unsubscribe from the second client too.
  MessageUnsubscribe meta4(
    Tenant::GuestTenant, 2, MessageUnsubscribe::Reason::kRequested);

  // send message to control tower
  ASSERT_OK(loop2.SendRequest(meta4, &socket2, 0));

  // The number of open logs should be zero now because no client
  // has any topic subscriptions.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(0, GetNumOpenLogs(ct));
}


TEST(ControlTowerTest, NoLogger) {
  // Create cluster with tower only (only need this for the log storage).
  LocalTestCluster cluster(info_log_, true, false, false);
  ASSERT_OK(cluster.GetStatus());

  MsgLoop loop(
      env_, env_options_, 0, 1, std::make_shared<NullLogger>(), "test");
  ASSERT_OK(loop.Initialize());
  ControlTower* tower = nullptr;
  ControlTowerOptions options;
  options.msg_loop = &loop;
  options.log_dir = "///";  // invalid dir, will fail to create LOG file.
  options.storage = cluster.GetLogStorage();
  options.log_router = cluster.GetLogRouter();
  ASSERT_OK(ControlTower::CreateNewInstance(options, &tower));
  loop.Stop();
  loop.Run();
  tower->Stop();
  delete tower;
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
