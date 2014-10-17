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

#include "src/util/testharness.h"
#include "src/controltower/tower.h"
#include "src/controltower/room.h"

namespace rocketspeed {

class ControlTowerTest;

namespace {
// A static variable that is used to initialize the
// libevent library settings at the start of the
// first unit test. If you set it to true, then you
// will see the entire libevent debug messages along
// with the test output.
static bool debug_libevent = false;

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
    ctoptions_.log_dir = test::TmpDir() + "/controltower";
    ctoptions_.storage_url =
      "configerator:logdevice/rocketspeed.logdevice.primary.conf";

    char myname[1024];
    st_ = ControlTower::CreateNewInstance(ctoptions_, &ct_);

    // what is my machine name?
    ASSERT_EQ(gethostname(&myname[0], sizeof(myname)), 0);
    hostname_.assign(myname);

    info_log = GetLogger();

    // enable all kinds of libevent debugging
    if (debug_libevent) {
      ld_event_enable_debug_logging(EVENT_DBG_ALL);
      ld_event_set_log_callback(dump_libevent_cb);
      ld_event_enable_debug_mode();
      debug_libevent = false;
    }
  }

  // deleting the ControlTower shuts down the event disptach loop.
  virtual ~ControlTowerTest() {
    delete ct_;
    ct_ = nullptr;
    env_->WaitForJoin();  // This is good hygine
  }

  std::string GetHostName() {
    return hostname_;
  }

  // Returns the logger that logs into the LOG file
  std::shared_ptr<Logger> GetLogger() {
    return ct_->GetOptions().info_log;
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  ControlTower* ct_;
  std::shared_ptr<Logger> info_log_;
  bool started_;
  ControlTowerOptions ctoptions_;
  Status st_;
  std::string hostname_;
  int num_ping_responses_;
  int num_subscribe_responses_;

  // A static method that is the entry point of a background thread
  static void ControlTowerStart(void* arg) {
    ControlTower* ct = reinterpret_cast<ControlTower*>(arg);
    ct->Run();
  }

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
    const char* s;
    switch (severity) {
      case _EVENT_LOG_DEBUG: s = "libev:dbg"; break;
      case _EVENT_LOG_MSG:   s = "libev:msg"; break;
      case _EVENT_LOG_WARN:  s = "libev:wrn"; break;
      case _EVENT_LOG_ERR:   s = "libev:err"; break;
      default:               s = "libev:???"; break; /* never reached */
    }
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
      env_->StartThread(ControlTowerTest::ControlTowerStart, ct_,
                        "tower-" +
                        std::to_string(ct_->GetOptions().port_number));
    }
    started_ = true;

    // Wait till the background thread has setup the dispatch loop
    while (!ct_->IsRunning()) {
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
  bool CheckSubscribeResponse(int expected) {
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

  Status ControlTowerStop() {
    delete ct_;
    ct_ = nullptr;
    return Status::OK();
  }
};

//
// Send a ping message and receive a ping response back from
// the control tower
TEST(ControlTowerTest, Ping) {
  int num_msgs = 100;
  HostId controltower(hostname_, ctoptions_.port_number);
  HostId clientId(hostname_, ctoptions_.port_number-1);

  // create a ControlTower (if not already created)
  ASSERT_EQ(ControlTowerRun().ok(), true);

  // create a message
  std::unique_ptr<Message> msg(new MessagePing(
                               Tenant::GuestTenant,
                               MessagePing::PingType::Request,
                               clientId));

  // Define a callback to process the Ping response at the client
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mPing] =
    [this] (std::unique_ptr<Message> msg) {
      processPing(std::move(msg));
    };

  // create a client to communicate with the ControlTower
  MsgLoop* loop = new MsgLoop(env_, env_options_, clientId,
                              GetLogger(),
                              client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart, loop,
                    "testc-" + std::to_string(clientId.port));
  while (!loop->IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }
  std::unique_ptr<Command> cmd(new ControlRoom::TowerCommand(
                               std::move(msg), controltower));
  ASSERT_EQ(loop->SendCommand(std::move(cmd)).ok() ||
            (delete loop, ControlTowerStop(), false), true);

  // verify that the ping response was received by the client
  ASSERT_EQ(CheckPingResponse(1) ||
            (delete loop, ControlTowerStop(), false), true);

  // now send multiple ping messages to server back-to-back
  for (int i = 0; i < num_msgs; i++) {
    std::unique_ptr<Message> newmsg(new MessagePing(
                                    Tenant::GuestTenant,
                                    MessagePing::PingType::Request,
                                    clientId));
    std::unique_ptr<Command> cmd(new ControlRoom::TowerCommand(
                                 std::move(newmsg), controltower));
    ASSERT_EQ(loop->SendCommand(std::move(cmd)).ok() ||
              (delete loop, ControlTowerStop(), false), true);
  }

  // check that all 100 responses were received
  ASSERT_EQ(CheckPingResponse(1+num_msgs) ||
            (delete loop, ControlTowerStop(), false), true);

  // free up resources
  delete loop;
}

TEST(ControlTowerTest, Subscribe) {
  std::vector<TopicPair> topics;
  int num_topics = 5;
  HostId controltower(hostname_, ctoptions_.port_number);
  HostId clientId(hostname_, ctoptions_.port_number-1);

  // create a ControlTower (if not already created)
  ASSERT_EQ(ControlTowerRun().ok(), true);

  // create a few topics
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    topics.push_back(TopicPair(4 + i, std::to_string(i), type, 101 + i));
  }
  // create a message
  std::unique_ptr<Message> meta1(new MessageMetadata(
                                     Tenant::GuestTenant,
                                     MessageMetadata::MetaType::Request,
                                     clientId, topics));

  // Define a callback to process the subscribe response at the client
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mMetadata] =
    [this] (std::unique_ptr<Message> msg) {
      processMetadata(std::move(msg));
    };

  // create a client to communicate with the ControlTower
  MsgLoop* loop = new MsgLoop(env_, env_options_, clientId,
                              GetLogger(),
                              client_callback);
  env_->StartThread(ControlTowerTest::MsgLoopStart, loop,
                    "testc-" + std::to_string(clientId.port));
  while (!loop->IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }
  std::unique_ptr<Command> cmd(new ControlRoom::TowerCommand(
                               std::move(meta1), controltower));

  // send message to control tower
  ASSERT_EQ(loop->SendCommand(std::move(cmd)).ok() ||
            (delete loop, ControlTowerStop(), false), true);

  // verify that the subscribe response was received by the client
  ASSERT_EQ(CheckSubscribeResponse(num_topics) ||
           (delete loop, ControlTowerStop(), false), true);

  // free up resources
  delete loop;
}
}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
