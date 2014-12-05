//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <unistd.h>
#include <chrono>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
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

    Status s = test::CreateLogger(env_, "CopilotTest", &info_log_);
    ASSERT_TRUE(s.ok());

    // Create MsgLoops
    ct_msg_loop_.reset(new MsgLoop(env_,
                                   EnvOptions(),
                                   ControlTower::DEFAULT_PORT,
                                   1,
                                   info_log_,
                                   "tower"));

    cp_msg_loop_.reset(new MsgLoop(env_,
                                   EnvOptions(),
                                   Copilot::DEFAULT_PORT,
                                   1,
                                   info_log_,
                                   "copilot"));

    // Create ControlTower
    ControlTowerOptions ct_options;
    ct_options.log_range = std::pair<LogID, LogID>(1, 1);
    ct_options.storage_url =
      "configerator:logdevice/rocketspeed.logdevice.primary.conf";
    ct_options.log_dir = test::TmpDir();
    ct_options.info_log = info_log_;
    ct_options.msg_loop = ct_msg_loop_.get();

    Status st = ControlTower::CreateNewInstance(ct_options, &ct_);
    ASSERT_TRUE(ct_ != nullptr);
    ASSERT_TRUE(st.ok());

    // Create Copilot
    options_.log_range = std::pair<LogID, LogID>(1, 1);
    options_.log_dir = test::TmpDir();
    options_.info_log = info_log_;
    options_.control_towers.push_back(ct_->GetClientId(0));
    options_.msg_loop = cp_msg_loop_.get();
    st_ = Copilot::CreateNewInstance(options_, &copilot_);

    // what is my machine name?
    char myname[1024];
    ASSERT_EQ(gethostname(&myname[0], sizeof(myname)), 0);
    hostname_.assign(myname);

    // enable all kinds of event loop debugging
    // not enabling by default since it is not thread safe
    if (false) {
      EventLoop::EnableDebugThreadUnsafe(dump_libevent_cb);
    }
  }

  virtual ~CopilotTest() {
    // deleting the Copilot shuts down the event disptach loop.
    ct_msg_loop_->Stop();
    cp_msg_loop_->Stop();
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
      env_->StartThread(CopilotTest::MsgLoopStart, ct_msg_loop_.get());
      env_->StartThread(CopilotTest::MsgLoopStart, cp_msg_loop_.get());
    }
    started_ = true;

    // Wait till the background thread has setup the dispatch loop
    while (!ct_msg_loop_->IsRunning() || !cp_msg_loop_->IsRunning()) {
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
  std::unique_ptr<MsgLoop> ct_msg_loop_;
  std::unique_ptr<MsgLoop> cp_msg_loop_;

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
      case EventLoop::kLogSeverityDebug: s = "dbg"; break;
      case EventLoop::kLogSeverityMsg:   s = "msg"; break;
      case EventLoop::kLogSeverityWarn:  s = "wrn"; break;
      case EventLoop::kLogSeverityErr:   s = "err"; break;
      default:                           s = "???"; break; // never reached
    }
    printf("[%s] %s\n", s, msg);
  }
};

TEST(CopilotTest, Publish) {
  // create a Copilot (if not already created)
  ASSERT_EQ(CopilotRun().ok(), true);
  port::Semaphore checkpoint;

  // create a client to communicate with the Copilot
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mMetadata] =
    [this, &checkpoint] (std::unique_ptr<Message> msg) {
      ProcessMetadata(std::move(msg));
      if (sent_msgs_.size() == acked_msgs_.size()) {
        checkpoint.Post();
      }
    };

  MsgLoop loop(env_, env_options_, 58499, 1, GetLogger(), "test");
  loop.RegisterCallbacks(client_callback);
  env_->StartThread(CopilotTest::MsgLoopStart, &loop,
                    "testc-" + std::to_string(loop.GetHostId().port));
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // send messages to copilot
  int num_msg = 100;
  for (int i = 0; i < num_msg; ++i) {
    NamespaceID ns = 101 + i % 50;
    std::string topic = "copilot_test_" + std::to_string(i % 50);
    auto type = i < num_msg/2 ? MetadataType::mSubscribe :
                                MetadataType::mUnSubscribe;
    std::string serial;
    MessageMetadata msg(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        ClientID("client1"),
                        { TopicPair(0, topic, type, ns) });
    msg.SerializeToString(&serial);
    std::unique_ptr<Command> cmd(
      new CopilotCommand(std::move(serial),
                         copilot_->GetClientId(0),
                         env_->NowMicros()));
    ASSERT_EQ(loop.SendCommand(std::move(cmd)).ok(), true);
    sent_msgs_.insert(topic);
  }

  // Ensure all messages were ack'd
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_TRUE(sent_msgs_ == acked_msgs_);
}

TEST(CopilotTest, WorkerMapping) {
  const int num_towers = 100;
  const int num_workers = 10;
  const int port = ControlTower::DEFAULT_PORT;

  MsgLoop loop(env_, EnvOptions(), port, num_workers, info_log_, "test");

  CopilotOptions options;
  options.log_range = std::pair<LogID, LogID>(1, 10000);
  options.info_log = info_log_;
  for (int i = 0; i < num_towers; ++i) {
    // Generate fake control towers.
    options.control_towers.push_back(HostId("tower", i).ToClientId());
  }
  options.msg_loop = &loop;
  options.control_tower_connections = 4;
  Copilot* copilot;
  Status st = Copilot::CreateNewInstance(options, &copilot);
  ASSERT_TRUE(st.ok());

  // Now check that each control tower is mapped to one worker.
  std::unordered_map<const ClientID*, std::set<int>> tower_to_workers;
  const auto& router = copilot->GetControlTowerRouter();
  for (LogID logid = options.log_range.first;
       logid <= options.log_range.second;
       ++logid) {
    // Find the tower responsible for this log.
    ClientID const* control_tower = nullptr;
    ASSERT_TRUE(router.GetControlTower(logid, &control_tower).ok());

    // Find the worker responsible for this log.
    int worker_id = copilot->GetLogWorker(logid);
    tower_to_workers[control_tower].insert(worker_id);

    // Check that the tower maps to only one worker.
    ASSERT_LE(tower_to_workers[control_tower].size(),
              options.control_tower_connections);
  }
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
