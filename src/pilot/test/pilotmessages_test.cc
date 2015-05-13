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

#include "src/test/test_cluster.h"
#include "src/pilot/pilot.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class PilotTest {
 public:
  // Create a new instance of the pilot
  PilotTest() : env_(Env::Default()) {
    // Create Logger
    ASSERT_OK(test::CreateLogger(env_, "PilotTest", &info_log_));
  }

  virtual ~PilotTest() {
    env_->WaitForJoin();  // This is good hygine
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;
  std::set<MsgId> sent_msgs_;
  std::set<MsgId> acked_msgs_;

  void ProcessDataAck(std::unique_ptr<Message> msg, StreamID origin) {
    const MessageDataAck* acks = static_cast<const MessageDataAck*>(msg.get());
    for (const auto& ack : acks->GetAcks()) {
      ASSERT_EQ(ack.status, MessageDataAck::AckStatus::Success);
      acked_msgs_.insert(ack.msgid);  // mark msgid as ack'd
    }
  }

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }
};

TEST(PilotTest, Publish) {
  // Create cluster with pilot only.
  LocalTestCluster cluster(info_log_, false, false, true);
  ASSERT_OK(cluster.GetStatus());

  port::Semaphore checkpoint;

  // create a client to communicate with the Pilot
  MsgLoop loop(env_, env_options_, 58499, 1, info_log_, "test");
  StreamSocket socket(loop.CreateOutboundStream(
      cluster.GetPilot()->GetClientId(), 0));
  loop.RegisterCallbacks({
      {MessageType::mDataAck, [&](std::unique_ptr<Message> msg,
                                  StreamID origin) {
        ASSERT_EQ(socket.GetStreamID(), origin);
        ProcessDataAck(std::move(msg), origin);
        if (sent_msgs_.size() == acked_msgs_.size()) {
          checkpoint.Post();
        }
      }},
  });
  ASSERT_OK(loop.Initialize());
  env_->StartThread(MsgLoopStart, &loop, "client");
  ASSERT_OK(loop.WaitUntilRunning());

  // send messages to pilot
  NamespaceID nsid = GuestNamespace;
  for (int i = 0; i < 100; ++i) {
    std::string payload = std::to_string(i);
    std::string topic = "test" + std::to_string(i);
    std::string serial;
    MessageData data(MessageType::mPublish,
                     Tenant::GuestTenant,
                     Slice(topic),
                     nsid,
                     Slice(payload));
    sent_msgs_.insert(data.GetMessageId());
    ASSERT_OK(loop.SendRequest(data, &socket, 0));
  }

  // Ensure all messages were ack'd
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(100)));
  ASSERT_TRUE(sent_msgs_ == acked_msgs_);

  const Statistics& stats = cluster.GetStatisticsSync();
  std::string stats_report = stats.Report();
  ASSERT_NE(stats_report.find("pilot.append_requests:                    100"),
            std::string::npos);
  ASSERT_NE(stats_report.find("pilot.failed_appends:                     0"),
            std::string::npos);
  ASSERT_NE(stats_report.find("pilot.append_latency_us"),
            std::string::npos);
}

TEST(PilotTest, NoLogger) {
  // Create cluster with pilot only (only need this for the log storage).
  LocalTestCluster cluster(info_log_, false, false, true);
  ASSERT_OK(cluster.GetStatus());

  MsgLoop loop(env_,
               env_options_,
               58499,
               1,
               std::make_shared<NullLogger>(),
               "test");
  Pilot* pilot = nullptr;
  PilotOptions options;
  options.msg_loop = &loop;
  options.log_dir = "///";  // invalid dir, will fail to create LOG file.
  options.storage = cluster.GetLogStorage();
  options.log_router = cluster.GetLogRouter();
  ASSERT_OK(Pilot::CreateNewInstance(options, &pilot));
  delete pilot;
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
