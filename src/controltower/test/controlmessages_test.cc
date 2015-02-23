//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <unistd.h>
#include <chrono>
#include <string>
#include <vector>

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
  ClientID client_id1_ = "clientid1";
  ClientID client_id2_ = "clientid2";

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
    return ct->GetTailer()->NumberOpenLogs();
  }
};

TEST(ControlTowerTest, Ping) {
  // Create cluster with controltower only.
  LocalTestCluster cluster(info_log_, true, false, false);
  ASSERT_OK(cluster.GetStatus());

  port::Semaphore ping_sem;

  // Define a callback to process the Ping response at the client
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mPing] =
      [this, &ping_sem](std::unique_ptr<Message> msg) {
    ASSERT_EQ(msg->GetMessageType(), MessageType::mPing);
    ASSERT_EQ(msg->GetOrigin(), client_id1_);
    MessagePing* m = static_cast<MessagePing*>(msg.get());
    ASSERT_EQ(m->GetPingType(), MessagePing::PingType::Response);
    ping_sem.Post();
  };

  // create a client to communicate with the ControlTower
  MsgLoop loop(env_, env_options_, 58499, 1, info_log_, "test");
  loop.RegisterCallbacks(client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart,
                    &loop,
                    "testc-" + std::to_string(loop.GetHostId().port));
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // create a message
  auto ct_client_id = cluster.GetControlTower()->GetClientId(0);
  MessagePing msg(Tenant::GuestTenant,
                  MessagePing::PingType::Request,
                  client_id1_);
  ASSERT_OK(loop.SendRequest(msg, ct_client_id));

  info_log_->Flush();
  // Verify that the ping response was received by the client.
  ASSERT_TRUE(ping_sem.TimedWait(timeout));

  // Now send multiple ping messages to server back-to-back.
  const int num_msgs = 100;
  for (int i = 0; i < num_msgs; i++) {
    MessagePing newmsg(Tenant::GuestTenant,
                       MessagePing::PingType::Request,
                       client_id1_);
    ASSERT_OK(loop.SendRequest(msg, ct_client_id));
  }

  // Check that all responses were received.
  for (int i = 0; i < num_msgs; ++i) {
    ASSERT_TRUE(ping_sem.TimedWait(timeout));
  }
}

TEST(ControlTowerTest, Subscribe) {
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
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, 101 + i));
  }

  // Define a callback to process the subscribe response at the client
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mMetadata] =
    [this](std::unique_ptr<Message> msg) {
      ASSERT_EQ(msg->GetOrigin(), client_id1_);
      ProcessMetadata(std::move(msg));
    };

  // create a client to communicate with the ControlTower
  MsgLoop loop(env_, env_options_, 58499, 1, info_log_, "test");
  loop.RegisterCallbacks(client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart,
                    &loop,
                    "testc-" + std::to_string(loop.GetHostId().port));
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // create a message
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        client_id1_,
                        topics);

  // send message to control tower
  ASSERT_OK(loop.SendRequest(meta1, ct->GetClientId(0)));

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
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, 101 + i));
  }

  // Define a callback to process the subscribe response at the client
  std::map<MessageType, MsgCallbackType> client_callback1;
  client_callback1[MessageType::mMetadata] =
    [this](std::unique_ptr<Message> msg) {
      ASSERT_EQ(msg->GetOrigin(), client_id1_);
      ProcessMetadata(std::move(msg));
    };

  std::map<MessageType, MsgCallbackType> client_callback2;
  client_callback2[MessageType::mMetadata] =
    [this](std::unique_ptr<Message> msg) {
      ASSERT_EQ(msg->GetOrigin(), client_id2_);
      ProcessMetadata(std::move(msg));
    };

  // create a client to communicate with the ControlTower
  MsgLoop loop1(env_, env_options_, 58499, 1, info_log_, "test");
  loop1.RegisterCallbacks(client_callback1);
  env_->StartThread(ControlTowerTest::MsgLoopStart,
                    &loop1,
                    "testc-" + std::to_string(loop1.GetHostId().port));
  while (!loop1.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // first subscriber *******
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        client_id1_,
                        topics);

  // send message to control tower
  ASSERT_OK(loop1.SendRequest(meta1, ct->GetClientId(0)));

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);

  // The number of distinct logs that are opened cannot be more than
  // the number of topics.
  int numopenlogs1 = GetNumOpenLogs(ct);
  ASSERT_LE(numopenlogs1, num_topics);
  ASSERT_NE(numopenlogs1, 0);

  // create second client to communicate with the ControlTower
  MsgLoop loop2(env_, env_options_, 58489, 1, info_log_, "test");
  loop2.RegisterCallbacks(client_callback2);
  env_->StartThread(ControlTowerTest::MsgLoopStart,
                    &loop2,
                    "testc-" + std::to_string(loop2.GetHostId().port));
  while (!loop2.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // The second subscriber subscribes to the same topics.
  MessageMetadata meta2(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        client_id2_,
                        topics);

  // send message to control tower
  ASSERT_OK(loop2.SendRequest(meta2, ct->GetClientId(0)));

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
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, 101 + i));
  }

  // Unsubscribe all the topics from the first client.
  MessageMetadata meta3(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        client_id1_,
                        topics);

  // send message to control tower
  ASSERT_OK(loop1.SendRequest(meta3, ct->GetClientId(0)));

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);

  // The number of open logs should not change because the second
  // client still has a live subscription for all those topics.
  ASSERT_EQ(numopenlogs1, GetNumOpenLogs(ct));

  // Finally, unsubscribe from the second client too.
  MessageMetadata meta4(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        client_id2_,
                        topics);

  // send message to control tower
  ASSERT_OK(loop2.SendRequest(meta4, ct->GetClientId(0)));

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
