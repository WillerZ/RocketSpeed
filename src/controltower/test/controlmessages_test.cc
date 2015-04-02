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
  port::Semaphore subscribe_sem_;

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }

  // A method to process a subscribe response message
  void ProcessMetadata(std::unique_ptr<Message> msg) {
    ASSERT_EQ(msg->GetMessageType(), MessageType::mMetadata);
    MessageMetadata* m = static_cast<MessageMetadata*>(msg.get());
    ASSERT_EQ(m->GetMetaType(), MessageMetadata::MetaType::Response);
    subscribe_sem_.Post();
  }

  // If the number of subscribe responses have reached the expected value,
  // then return true, otherwise return false.
  bool CheckSubscribeResponse(int expected) {
    for (int i = 0; i < expected; ++i) {
      if (!subscribe_sem_.TimedWait(timeout)) {
        return false;
      }
    }
    return true;
  }

  // gets the number of open logs
  int GetNumOpenLogs(ControlTower* ct) const {
    return ct->GetLogTailer()->NumberOpenLogs();
  }
};

TEST(ControlTowerTest, Subscribe) {
  // Create cluster with copilot and controltower.
  LocalTestCluster cluster(info_log_, true, true, false);
  ASSERT_OK(cluster.GetStatus());

  std::vector<TopicPair> topics;
  const int num_topics = 5;

  // create a few topics
  for (int i = 0; i < num_topics; i++) {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    NamespaceID ns = "test" + std::to_string(i);
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, ns));
  }

  // create a client to communicate with the ControlTower
  MsgLoop loop(env_, env_options_, 58499, 1, info_log_, "client");
  StreamSocket socket(cluster.GetControlTower()->GetClientId(0),
                      loop.GetOutboundAllocator());
  // Define a callback to process the subscribe response at the client
  loop.RegisterCallbacks({
      {MessageType::mMetadata, [&](std::unique_ptr<Message> msg) {
        ASSERT_EQ(socket.GetStreamID(), msg->GetOrigin());
        ProcessMetadata(std::move(msg));
      }},
  });
  env_->StartThread(ControlTowerTest::MsgLoopStart, &loop, "client");
  ASSERT_OK(loop.WaitUntilRunning());

  // create a message
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        topics);

  // send message to control tower
  ASSERT_OK(loop.SendRequest(meta1, &socket, 0));

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);
}

TEST(ControlTowerTest, MultipleSubscribers) {
  // Create cluster with copilot and controltower.
  LocalTestCluster cluster(info_log_, true, true, false);
  ASSERT_OK(cluster.GetStatus());
  auto ct = cluster.GetControlTower();

  std::vector<TopicPair> topics;
  int num_topics = 5;

  // create a few topics
  for (int i = 0; i < num_topics; i++) {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    NamespaceID ns = "test" + std::to_string(i);
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, ns));
  }

  // create a client to communicate with the ControlTower
  MsgLoop loop1(env_, env_options_, 58499, 1, info_log_, "loop1");
  StreamSocket socket1(ct->GetClientId(0), loop1.GetOutboundAllocator());
  loop1.RegisterCallbacks({
      {MessageType::mMetadata, [&](std::unique_ptr<Message> msg) {
        ASSERT_EQ(socket1.GetStreamID(), msg->GetOrigin());
        ProcessMetadata(std::move(msg));
      }},
      {MessageType::mDeliver, [](std::unique_ptr<Message>) {}},
      {MessageType::mGap, [](std::unique_ptr<Message>){}},
  });
  env_->StartThread(ControlTowerTest::MsgLoopStart, &loop1, "loop1");
  ASSERT_OK(loop1.WaitUntilRunning());

  // first subscriber *******
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        topics);

  // send message to control tower
  ASSERT_OK(loop1.SendRequest(meta1, &socket1, 0));

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);

  // The number of distinct logs that are opened cannot be more than
  // the number of topics.
  // Wait a little, since log opening is async.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  int numopenlogs1 = GetNumOpenLogs(ct);
  ASSERT_LE(numopenlogs1, num_topics);
  ASSERT_NE(numopenlogs1, 0);

  // create second client to communicate with the ControlTower
  MsgLoop loop2(env_, env_options_, 58489, 1, info_log_, "loop2");
  StreamSocket socket2(ct->GetClientId(0), loop2.GetOutboundAllocator());
  loop2.RegisterCallbacks({
      {MessageType::mMetadata, [&](std::unique_ptr<Message> msg) {
        ASSERT_EQ(socket2.GetStreamID(), msg->GetOrigin());
        ProcessMetadata(std::move(msg));
      }},
      {MessageType::mDeliver, [](std::unique_ptr<Message>) {}},
      {MessageType::mGap, [](std::unique_ptr<Message>){}},
  });
  env_->StartThread(ControlTowerTest::MsgLoopStart, &loop2, "loop2");
  ASSERT_OK(loop2.WaitUntilRunning());

  // The second subscriber subscribes to the same topics.
  MessageMetadata meta2(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        topics);

  // send message to control tower
  ASSERT_OK(loop2.SendRequest(meta2, &socket2, 0));

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);

  // The control tower should not have re-opened any of those logs
  // because they were already subscribed to by the first client.
  ASSERT_EQ(numopenlogs1, GetNumOpenLogs(ct));

  // Create unsubscription request for all topics
  topics.clear();
  for (int i = 0; i < num_topics; i++) {
    // alternate between types
    MetadataType type = mUnSubscribe;
    NamespaceID ns = "test" + std::to_string(i);
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, ns));
  }

  // Unsubscribe all the topics from the first client.
  MessageMetadata meta3(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        topics);

  // send message to control tower
  ASSERT_OK(loop1.SendRequest(meta3, &socket1, 0));

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);

  // The number of open logs should not change because the second
  // client still has a live subscription for all those topics.
  ASSERT_EQ(numopenlogs1, GetNumOpenLogs(ct));

  // Finally, unsubscribe from the second client too.
  MessageMetadata meta4(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        topics);

  // send message to control tower
  ASSERT_OK(loop2.SendRequest(meta4, &socket2, 0));

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);

  // The number of open logs should be zero now because no client
  // has any topic subscriptions.
  ASSERT_EQ(0, GetNumOpenLogs(ct));
}


TEST(ControlTowerTest, NoLogger) {
  // Create cluster with tower only (only need this for the log storage).
  LocalTestCluster cluster(info_log_, true, false, false);
  ASSERT_OK(cluster.GetStatus());

  MsgLoop loop(env_,
               env_options_,
               58499,
               1,
               std::make_shared<NullLogger>(),
               "test");
  ControlTower* tower = nullptr;
  ControlTowerOptions options;
  options.msg_loop = &loop;
  options.log_dir = "///";  // invalid dir, will fail to create LOG file.
  options.storage = cluster.GetLogStorage();
  options.log_router = cluster.GetLogRouter();
  ASSERT_OK(ControlTower::CreateNewInstance(options, &tower));
  delete tower;
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
