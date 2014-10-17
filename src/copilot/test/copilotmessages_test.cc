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
#include <chrono>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "src/controltower/tower.h"
#include "src/controltower/options.h"
#include "src/copilot/copilot.h"
#include "src/copilot/worker.h"
#include "src/copilot/options.h"
#include "src/util/logdevice.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class CopilotTest {
 public:
  // Create a new instance of the copilot
  CopilotTest():
    env_(Env::Default()), copilot_(nullptr), started_(false) {

    Status s = CreateLoggerFromOptions(env_,
                                       test::TmpDir(),
                                       "LOG.copilotmessages",
                                       0,
                                       0,
                                       InfoLogLevel::DEBUG_LEVEL,
                                       &info_log_);
    ASSERT_TRUE(s.ok());

    // Create ControlTower
    ControlTowerOptions ct_options;
    ct_options.log_range = std::pair<LogID, LogID>(1, 1);
    ct_options.storage_url =
      "configerator:logdevice/rocketspeed.logdevice.primary.conf";
    ct_options.log_dir = test::TmpDir();
    ct_options.info_log = info_log_;
    Status st = ControlTower::CreateNewInstance(ct_options, &ct_);
    ASSERT_TRUE(ct_ != nullptr);
    ASSERT_TRUE(st.ok());

    // Create Copilot
    options_.log_range = std::pair<LogID, LogID>(1, 1);
    options_.log_dir = test::TmpDir();
    options_.info_log = info_log_;
    options_.control_towers.push_back(ct_->GetHostId());
    st_ = Copilot::CreateNewInstance(options_, &copilot_);

    // what is my machine name?
    char myname[1024];
    ASSERT_EQ(gethostname(&myname[0], sizeof(myname)), 0);
    hostname_.assign(myname);

    // enable all kinds of libevent debugging
    // not enabling debugging by default since it isn't threadsafe in our
    // version of libevent (we compile it without threading support).
    if (false) {
      ld_event_enable_debug_logging(EVENT_DBG_ALL);
      ld_event_set_log_callback(dump_libevent_cb);
      ld_event_enable_debug_mode();
    }
  }

  virtual ~CopilotTest() {
    // deleting the Copilot shuts down the event disptach loop.
    delete copilot_;
    delete ct_;
    env_->WaitForJoin();  // This is good hygine
  }

  // Setup dispatch thread and ensure that it is running.
  Status CopilotRun() {
    // If there was an error in instantiating the Copilot earlier.
    // then return error immediately.
    if (!st_.ok()) {
      return st_;
    }

    // If the copilot has not already been started, then start it
    if (!started_) {
      env_->StartThread(CopilotTest::ControlTowerStart, ct_);
      env_->StartThread(CopilotTest::CopilotStart, copilot_);
    }
    started_ = true;

    // Wait till the background thread has setup the dispatch loop
    while (!copilot_->IsRunning() || !ct_->IsRunning()) {
      env_->SleepForMicroseconds(1000);
    }
    return Status::OK();
  }

  // Returns the logger that logs into the LOG file
  std::shared_ptr<Logger> GetLogger() {
    return copilot_->GetOptions().info_log;
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  Copilot* copilot_;
  ControlTower* ct_;
  bool started_;
  CopilotOptions options_;
  Status st_;
  std::string hostname_;
  std::set<Topic> sent_msgs_;
  std::set<Topic> acked_msgs_;
  std::shared_ptr<Logger> info_log_;

  // A static method that is the entry point of a background thread
  // for the Copilot.
  static void CopilotStart(void* arg) {
    Copilot* copilot = reinterpret_cast<Copilot*>(arg);
    copilot->Run();
  }

  // A static method that is the entry point of a background thread
  // for the ControlTower.
  static void ControlTowerStart(void* arg) {
    ControlTower* ct = reinterpret_cast<ControlTower*>(arg);
    ct->Run();
  }

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }

  // A static method to process a subscribe response message
  void ProcessMetadata(std::unique_ptr<Message> msg) {
    ASSERT_EQ(msg->GetMessageType(), MessageType::mMetadata);
    MessageMetadata* metadata = static_cast<MessageMetadata*>(msg.get());
    ASSERT_EQ(metadata->GetMetaType(), MessageMetadata::MetaType::Response);
    ASSERT_EQ(metadata->GetTopicInfo().size(), 1);
    acked_msgs_.insert(metadata->GetTopicInfo()[0].topic_name);
  }

  // Dumps libevent info messages to stdout
  static void
  dump_libevent_cb(int severity, const char* msg) {
    const char* s;
    switch (severity) {
      case _EVENT_LOG_DEBUG: s = "dbg"; break;
      case _EVENT_LOG_MSG:   s = "msg"; break;
      case _EVENT_LOG_WARN:  s = "wrn"; break;
      case _EVENT_LOG_ERR:   s = "err"; break;
      default:               s = "?";   break; /* never reached */
    }
    printf("[%s] %s\n", s, msg);
  }
};

TEST(CopilotTest, Publish) {
  // create a Copilot (if not already created)
  ASSERT_EQ(CopilotRun().ok(), true);

  // create a client to communicate with the Copilot
  HostId client_id(hostname_, options_.port_number - 1);
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mMetadata] =
    [this] (std::unique_ptr<Message> msg) {
      ProcessMetadata(std::move(msg));
    };

  MsgLoop loop(env_,
               env_options_,
               client_id,
               GetLogger(),
               client_callback);
  env_->StartThread(CopilotTest::MsgLoopStart, &loop,
                    "testc-" + std::to_string(client_id.port));
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // send messages to copilot
  HostId copilot(hostname_, options_.port_number);
  int num_msg = 100;
  for (int i = 0; i < num_msg; ++i) {
    std::string topic = "copilot_test_" + std::to_string(i % 50);
    auto type = i < num_msg/2 ? MetadataType::mSubscribe :
                                MetadataType::mUnSubscribe;
    std::unique_ptr<Message> msg(new MessageMetadata(
                                 Tenant::GuestTenant,
                                 MessageMetadata::MetaType::Request,
                                 client_id,
                                 { TopicPair(0, topic, type, 101 + i) }));
    std::unique_ptr<Command> cmd(new CopilotCommand(
                                     std::move(msg), copilot));
    ASSERT_EQ(loop.SendCommand(std::move(cmd)).ok(), true);
    sent_msgs_.insert(topic);
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Ensure all messages were ack'd
  ASSERT_TRUE(sent_msgs_ == acked_msgs_);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
