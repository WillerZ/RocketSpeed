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
#include <vector>

#include "src/pilot/pilot.h"
#include "src/logdevice/storage.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class PilotTest {
 public:
  // Create a new instance of the pilot
  PilotTest():
    env_(Env::Default()), pilot_(nullptr), started_(false) {
    // Create Logger
    st_ = test::CreateLogger(env_, "PilotTest", &info_log_);
    if (!st_.ok()) {
      return;
    }

    msg_loop_.reset(new MsgLoop(env_,
                                EnvOptions(),
                                Pilot::DEFAULT_PORT,
                                1,
                                info_log_,
                                "pilot"));

    // Create Pilot
    options_.log_range = std::pair<LogID, LogID>(1, 1);
    options_.msg_loop = msg_loop_.get();
    options_.info_log = info_log_;
    st_ = Pilot::CreateNewInstance(std::move(options_), &pilot_);
    if (!st_.ok()) {
      return;
    }

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

  virtual ~PilotTest() {
    // deleting the Pilot shuts down the event disptach loop.
    msg_loop_->Stop();
    delete pilot_;
    env_->WaitForJoin();  // This is good hygine
  }

  // Setup dispatch thread and ensure that it is running.
  Status PilotRun() {
    // If there was an error in instantiating the Pilot earlier.
    // then return error immediately.
    if (!st_.ok()) {
      return st_;
    }

    // If the pilot has not already been started, then start it
    if (!started_) {
      env_->StartThread(PilotTest::MsgLoopStart, msg_loop_.get());
    }
    started_ = true;

    // Wait till the background thread has setup the dispatch loop
    while (!msg_loop_->IsRunning()) {
      env_->SleepForMicroseconds(1000);
    }
    return Status::OK();
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  Pilot* pilot_;
  bool started_;
  PilotOptions options_;
  Status st_;
  std::string hostname_;
  std::set<MsgId> sent_msgs_;
  std::set<MsgId> acked_msgs_;
  std::unique_ptr<MsgLoop> msg_loop_;
  std::shared_ptr<Logger> info_log_;

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }

  void ProcessDataAck(std::unique_ptr<Message> msg) {
    const MessageDataAck* acks = static_cast<const MessageDataAck*>(msg.get());
    for (const auto& ack : acks->GetAcks()) {
      ASSERT_EQ(ack.status, MessageDataAck::AckStatus::Success);
      acked_msgs_.insert(ack.msgid);  // mark msgid as ack'd
    }
  }

  // Dumps libevent info messages to stdout
  static void
  dump_libevent_cb(int severity, const char* msg) {
    const char* s = EventLoop::SeverityToString(severity);
    printf("[%s] %s\n", s, msg);
  }
};

TEST(PilotTest, Publish) {
  // create a Pilot (if not already created)
  ASSERT_EQ(PilotRun().ok(), true);
  port::Semaphore checkpoint;

  // create a client to communicate with the Pilot
  std::map<MessageType, MsgCallbackType> client_callback;
  client_callback[MessageType::mDataAck] =
    [this, &checkpoint] (std::unique_ptr<Message> msg) {
      ProcessDataAck(std::move(msg));
      if (sent_msgs_.size() == acked_msgs_.size()) {
        checkpoint.Post();
      }
    };

  MsgLoop loop(env_, env_options_, 58499, 1, info_log_, "test");
  loop.RegisterCallbacks(client_callback);
  env_->StartThread(PilotTest::MsgLoopStart, &loop);
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }


  // send messages to pilot
  NamespaceID nsid = 101;
  const bool is_new_request = true;
  for (int i = 0; i < 100; ++i) {
    std::string payload = std::to_string(i);
    std::string topic = "test" + std::to_string(i);
    std::string serial;
    MessageData data(MessageType::mPublish,
                     Tenant::GuestTenant,
                     ClientID("client1"), Slice(topic), nsid,
                     Slice(payload));
    data.SerializeToString(&serial);
    sent_msgs_.insert(data.GetMessageId());
    std::unique_ptr<Command> cmd(
      new SerializedSendCommand(std::move(serial),
                                pilot_->GetClientId(0),
                                env_->NowMicros(),
                                is_new_request));
    ASSERT_EQ(loop.SendCommand(std::move(cmd)).ok(), true);
  }

  // Ensure all messages were ack'd
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(100)));
  ASSERT_TRUE(sent_msgs_ == acked_msgs_);

  const Statistics& stats = pilot_->GetStatistics();
  std::string stats_report = stats.Report();
  ASSERT_NE(stats_report.find("rocketspeed.pilot.append_requests:        100"),
    std::string::npos);
  ASSERT_NE(stats_report.find("rocketspeed.pilot.failed_appends:         0"),
    std::string::npos);
  ASSERT_NE(stats_report.find("rocketspeed.pilot.append_latency_us"),
    std::string::npos);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
