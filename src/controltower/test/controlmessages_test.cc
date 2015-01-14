//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <unistd.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/thread.h>
#include <event2/util.h>
#include <string>
#include <vector>

#include "src/messages/event2_version.h"
#include "src/util/testharness.h"
#include "src/controltower/tower.h"
#include "src/controltower/room.h"

namespace rocketspeed {

class ControlTowerTest;

namespace {
// A static variable that is used to initialize the
// event loop library settings at the start of the
// first unit test. If you set it to true, then you
// will see the entire event loop debug messages
// along with the test output.
static bool debug_event_loop = false;

static std::shared_ptr<Logger> info_log;

}  //  namespace

class ControlTowerTest {
 public:
  // Create a new instance of the control tower
  ControlTowerTest():
    env_(Env::Default()), ct_(nullptr),
    started_(false),
    num_ping_responses_(0),
    num_subscribe_responses_(0) {
    // set control tower to log information to test dir
    st_ = test::CreateLogger(env_, "ControlTowerTest", &info_log);

    msg_loop_.reset(new MsgLoop(env_,
                                EnvOptions(),
                                ControlTower::DEFAULT_PORT,
                                1,
                                info_log,
                                "tower"));

    ctoptions_.info_log = info_log;
    ctoptions_.msg_loop = msg_loop_.get();
    ctoptions_.storage_url =
      "configerator:logdevice/rocketspeed.logdevice.primary.conf";

    st_ = ControlTower::CreateNewInstance(ctoptions_, &ct_);

    // enable all kinds of event loop debugging
    // not enabling by default since it is not thread safe
    if (debug_event_loop) {
      EventLoop::EnableDebugThreadUnsafe(dump_libevent_cb);
      debug_event_loop = false;
    }
  }

  // deleting the ControlTower shuts down the event disptach loop.
  virtual ~ControlTowerTest() {
    msg_loop_->Stop();
    delete ct_;
    ct_ = nullptr;
    env_->WaitForJoin();  // This is good hygine
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  ControlTower* ct_;
  bool started_;
  ControlTowerOptions ctoptions_;
  Status st_;
  std::string hostname_;
  std::atomic<int> num_ping_responses_;
  std::atomic<int> num_subscribe_responses_;
  std::unique_ptr<MsgLoop> msg_loop_;

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }

  // A method to process a ping message
  void processPing(std::unique_ptr<Message> msg) {
    ASSERT_EQ(msg->GetMessageType(), MessageType::mPing);
    MessagePing* m = static_cast<MessagePing*>(msg.get());
    ASSERT_EQ(m->GetPingType(), MessagePing::PingType::Response);
    num_ping_responses_++;
  }

  // A method to process a subscribe response message
  void processMetadata(std::unique_ptr<Message> msg) {
    ASSERT_EQ(msg->GetMessageType(), MessageType::mMetadata);
    MessageMetadata* m = static_cast<MessageMetadata*>(msg.get());
    ASSERT_EQ(m->GetMetaType(), MessageMetadata::MetaType::Response);
    num_subscribe_responses_++;
  }

  // Dumps libevent info messages to stdout
  static void
  dump_libevent_cb(int severity, const char* msg) {
    const char* s = EventLoop::SeverityToString(severity);
    LOG_INFO(info_log, "[%s] %s\n", s, msg);
    info_log->Flush();
  }

  // Setup dispatch thread and ensure that it is running.
  Status ControlTowerRun() {
    // If there was an error in instantiating the ControlTower earlier.
    // then return error immediately.
    if (!st_.ok()) {
      return st_;
    }

    // If the control tower has not already been started, then start it
    if (!started_) {
      env_->StartThread(ControlTowerTest::MsgLoopStart, msg_loop_.get(),
                        "tower-" +
                        std::to_string(ct_->GetHostId().port));
    }
    started_ = true;

    // Wait till the background thread has setup the dispatch loop
    while (!msg_loop_->IsRunning()) {
      env_->SleepForMicroseconds(1000);
    }
    return Status::OK();
  }

  // If the number of ping responses have reached the expected value,
  // then return true, otherwise return false.
  bool CheckPingResponse(int expected) {
    int retry = 10000;
    while (retry-- > 0 &&
           num_ping_responses_ != expected) {
      env_->SleepForMicroseconds(1000);
    }
    if (num_ping_responses_ == expected) {
      return true;
    }
    return false;
  }

  // If the number of subscribe responses have reached the expected value,
  // then return true, otherwise return false.
  bool CheckSubscribeResponse(int expected) const {
    int retry = 10000;
    while (retry-- > 0 &&
           num_subscribe_responses_ != expected) {
      env_->SleepForMicroseconds(1000);
    }
    if (num_subscribe_responses_ == expected) {
      return true;
    }
    return false;
  }

  void ClearSubscribeResponse() {
    num_subscribe_responses_ = 0;
  }

  // gets the number of open logs
  int GetNumOpenLogs() const {
    return ct_->GetTailer()->NumberOpenLogs();
  }
};

//
// Send a ping message and receive a ping response back from
// the control tower
TEST(ControlTowerTest, Ping) {
  int num_msgs = 100;

  // create a ControlTower (if not already created)
  ASSERT_EQ(ControlTowerRun().ok(), true);

  // Define a callback to process the Ping response at the client
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mPing] =
    [this] (std::unique_ptr<Message> msg) {
      processPing(std::move(msg));
    };

  // create a client to communicate with the ControlTower
  MsgLoop loop(env_, env_options_, 58499, 1, info_log, "test");
  loop.RegisterCallbacks(client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart, &loop,
                    "testc-" + std::to_string(loop.GetHostId().port));
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // create a message
  std::string serial;
  const bool is_new_request = true;
  MessagePing msg(Tenant::GuestTenant,
                  MessagePing::PingType::Request,
                  ClientID("clientid1:100"));
  msg.SerializeToString(&serial);  // serialize msg
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(serial),
                              ct_->GetClientId(0),
                              env_->NowMicros(),
                              is_new_request));
  ASSERT_EQ(loop.SendCommand(std::move(cmd)).ok(), true);

  // verify that the ping response was received by the client
  ASSERT_EQ(CheckPingResponse(1), true);

  // now send multiple ping messages to server back-to-back
  for (int i = 0; i < num_msgs; i++) {
    MessagePing newmsg(Tenant::GuestTenant,
                       MessagePing::PingType::Request,
                       ClientID("clientidx:100"));
    msg.SerializeToString(&serial);  // serialize msg
    std::unique_ptr<Command> cmd(
      new SerializedSendCommand(std::move(serial),
                                ct_->GetClientId(0),
                                env_->NowMicros(),
                                is_new_request));
    ASSERT_EQ(loop.SendCommand(std::move(cmd)).ok(), true);
  }

  // check that all 100 responses were received
  ASSERT_EQ(CheckPingResponse(1+num_msgs), true);
}

TEST(ControlTowerTest, Subscribe) {
  std::vector<TopicPair> topics;
  int num_topics = 5;

  // create a ControlTower (if not already created)
  ASSERT_EQ(ControlTowerRun().ok(), true);

  // create a few topics
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, 101 + i));
  }

  // Define a callback to process the subscribe response at the client
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mMetadata] =
    [this] (std::unique_ptr<Message> msg) {
      processMetadata(std::move(msg));
    };

  // create a client to communicate with the ControlTower
  MsgLoop loop(env_, env_options_, 58499, 1, info_log, "test");
  loop.RegisterCallbacks(client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart, &loop,
                    "testc-" + std::to_string(loop.GetHostId().port));
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // create a message
  std::string serial;
  const bool is_new_request = true;
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        ClientID("clientid100:100"), topics);
  meta1.SerializeToString(&serial);
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(serial),
                              ct_->GetClientId(0),
                              env_->NowMicros(),
                              is_new_request));

  // send message to control tower
  ASSERT_EQ(loop.SendCommand(std::move(cmd)).ok(), true);

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);
}

TEST(ControlTowerTest, MultipleSubscribers) {
  std::vector<TopicPair> topics;
  int num_topics = 5;

  // create a ControlTower (if not already created)
  ASSERT_EQ(ControlTowerRun().ok(), true);

  // create a few topics
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, 101 + i));
  }

  // Define a callback to process the subscribe response at the client
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mMetadata] =
    [this] (std::unique_ptr<Message> msg) {
      processMetadata(std::move(msg));
    };

  // create a client to communicate with the ControlTower
  MsgLoop loop1(env_, env_options_, 58499, 1, info_log, "test");
  loop1.RegisterCallbacks(client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart, &loop1,
                    "testc-" + std::to_string(loop1.GetHostId().port));
  while (!loop1.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // first subscriber *******
  std::string serial;
  const bool is_new_request = true;
  MessageMetadata meta1(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        ClientID("clientid100:100"), topics);
  meta1.SerializeToString(&serial);
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(serial),
                              ct_->GetClientId(0),
                              env_->NowMicros(),
                              is_new_request));

  // send message to control tower
  ASSERT_EQ(loop1.SendCommand(std::move(cmd)).ok(), true);

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);
  ClearSubscribeResponse();

  // The number of distinct logs that are opened cannot be more than
  // the number of topics.
  int numopenlogs1 = GetNumOpenLogs();
  ASSERT_LE(numopenlogs1, num_topics);
  ASSERT_NE(numopenlogs1, 0);

  // create second client to communicate with the ControlTower
  MsgLoop loop2(env_, env_options_, 58489, 1, info_log, "test");
  loop2.RegisterCallbacks(client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart, &loop2,
                    "testc-" + std::to_string(loop2.GetHostId().port));
  while (!loop2.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // The second subscriber subscribes to the same topics.
  std::string serial2;
  MessageMetadata meta2(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        ClientID("clientid200:200"), topics);
  meta2.SerializeToString(&serial2);
  std::unique_ptr<Command> cmd2(
    new SerializedSendCommand(std::move(serial2),
                              ct_->GetClientId(0),
                              env_->NowMicros(),
                              is_new_request));

  // send message to control tower
  ASSERT_EQ(loop2.SendCommand(std::move(cmd2)).ok(), true);

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);
  ClearSubscribeResponse();

  // The control tower should not have re-opened any of those logs
  // because they were already subscribed to by the first client.
  ASSERT_EQ(numopenlogs1, GetNumOpenLogs());

  // Create unsubscription request for all topics
  topics.clear();
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = mUnSubscribe;
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, 101 + i));
  }

  // Unsubscribe all the topics from the first client.
  std::string serial3;
  MessageMetadata meta3(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        ClientID("clientid100:100"), topics);
  meta3.SerializeToString(&serial3);
  std::unique_ptr<Command> cmd3(
    new SerializedSendCommand(std::move(serial3),
                              ct_->GetClientId(0),
                              env_->NowMicros(),
                              is_new_request));

  // send message to control tower
  ASSERT_EQ(loop1.SendCommand(std::move(cmd3)).ok(), true);

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);
  ClearSubscribeResponse();

  // The number of open logs should not change because the second
  // client still has a live subscription for all those topics.
  ASSERT_EQ(numopenlogs1, GetNumOpenLogs());

  // Finally, unsubscribe from the second client too.
  std::string serial4;
  MessageMetadata meta4(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        ClientID("clientid200:200"), topics);
  meta4.SerializeToString(&serial4);
  std::unique_ptr<Command> cmd4(
    new SerializedSendCommand(std::move(serial4),
                              ct_->GetClientId(0),
                              env_->NowMicros(),
                              is_new_request));

  // send message to control tower
  ASSERT_EQ(loop2.SendCommand(std::move(cmd4)).ok(), true);

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics), true);
  ClearSubscribeResponse();

  // The number of open logs should be zero now because no client
  // has any topic subscriptions.
  ASSERT_EQ(0, GetNumOpenLogs());
}
}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
