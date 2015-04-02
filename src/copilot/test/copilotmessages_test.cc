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
#include "src/test/test_cluster.h"
#include "src/util/testharness.h"
#include "src/rollcall/rollcall_impl.h"

namespace rocketspeed {

class CopilotTest {
 public:
  // Create a new instance of the copilot
  CopilotTest() : env_(Env::Default()) {
    // Create Logger
    ASSERT_OK(test::CreateLogger(env_, "CopilotTest", &info_log_));
  }

  virtual ~CopilotTest() {
    env_->WaitForJoin();  // This is good hygine
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;
  std::set<Topic> sent_msgs_;
  std::set<Topic> acked_msgs_;
  std::vector<RollcallEntry> rollcall_entries_;

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }

  // A method to process a subscribe response message
  void ProcessMetadata(std::unique_ptr<Message> msg) {
    ASSERT_EQ(msg->GetMessageType(), MessageType::mMetadata);
    MessageMetadata* metadata = static_cast<MessageMetadata*>(msg.get());
    ASSERT_EQ(metadata->GetMetaType(), MessageMetadata::MetaType::Response);
    ASSERT_EQ(metadata->GetTopicInfo().size(), 1);
    acked_msgs_.insert(metadata->GetTopicInfo()[0].topic_name);
  }

  // A method to process rollcall entries
  void ProcessRollcall(const RollcallEntry& entry) {
    rollcall_entries_.push_back(entry);
  }
};

TEST(CopilotTest, Subscribe) {
  // Create cluster with copilot and controltower only.
  LocalTestCluster cluster(info_log_, true, true, false);
  ASSERT_OK(cluster.GetStatus());

  port::Semaphore checkpoint;

  // create a client to communicate with the Copilot
  MsgLoop loop(env_, env_options_, 58499, 1, info_log_, "test");
  StreamSocket socket(loop.CreateOutboundStream(
      cluster.GetCopilotHostIds().front().ToClientId(), 0));
  loop.RegisterCallbacks({
      {MessageType::mMetadata, [&](std::unique_ptr<Message> msg) {
        ASSERT_EQ(socket.GetStreamID(), msg->GetOrigin());
        ProcessMetadata(std::move(msg));
        if (sent_msgs_.size() == acked_msgs_.size()) {
          checkpoint.Post();
        }
      }},
  });
  env_->StartThread(CopilotTest::MsgLoopStart, &loop,
                    "testc-" + std::to_string(loop.GetHostId().port));
  ASSERT_OK(loop.WaitUntilRunning());

  // send subscribe/unsubscribe messages to copilot
  int num_msg = 100;
  for (int i = 0; i < num_msg; ++i) {
    NamespaceID ns = "test" + std::to_string(i % 50);
    std::string topic = "copilot_test_" + std::to_string(i % 50);
    auto type = i < num_msg/2 ? MetadataType::mSubscribe :
                                MetadataType::mUnSubscribe;
    MessageMetadata msg(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        { TopicPair(0, topic, type, ns) });
    ASSERT_OK(loop.SendRequest(msg, &socket, 0));
    sent_msgs_.insert(topic);
  }

  // Ensure all messages were ack'd
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_TRUE(sent_msgs_ == acked_msgs_);
}

TEST(CopilotTest, WorkerMapping) {
  // Create cluster with copilot and controltower only.
  LocalTestCluster cluster(info_log_, true, true, false);
  ASSERT_OK(cluster.GetStatus());

  const int num_towers = 100;
  const int num_workers = 10;
  const int port = ControlTower::DEFAULT_PORT;

  MsgLoop loop(env_, EnvOptions(), port, num_workers, info_log_, "test");

  CopilotOptions options;
  auto log_range = std::pair<LogID, LogID>(1, 10000);
  options.info_log = info_log_;
  for (int i = 0; i < num_towers; ++i) {
    // Generate fake control towers.
    options.control_towers.push_back(HostId("tower", i).ToClientId());
  }
  options.pilots.push_back(HostId("fakepilot", 0));
  options.msg_loop = &loop;
  options.control_tower_connections = 4;
  options.log_router = cluster.GetLogRouter();
  Copilot* copilot;
  Status st = Copilot::CreateNewInstance(options, &copilot);
  ASSERT_TRUE(st.ok());

  // Now check that each control tower is mapped to one worker.
  std::unordered_map<const ClientID*, std::set<int>> tower_to_workers;
  const auto& router = copilot->GetControlTowerRouter();
  for (LogID logid = log_range.first;
       logid <= log_range.second;
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
  delete copilot;
}

TEST(CopilotTest, NoLogger) {
  // Create cluster with tower+copilot (only need this for the log router).
  LocalTestCluster cluster(info_log_, true, true, false);
  ASSERT_OK(cluster.GetStatus());

  MsgLoop loop(env_,
               env_options_,
               58499,
               1,
               std::make_shared<NullLogger>(),
               "test");
  Copilot* copilot = nullptr;
  CopilotOptions options;
  options.pilots.push_back(HostId("fakepilot", 0));
  options.msg_loop = &loop;
  options.log_dir = "///";  // invalid dir, will fail to create LOG file.
  options.log_router = cluster.GetLogRouter();
  ASSERT_OK(Copilot::CreateNewInstance(options, &copilot));
  delete copilot;
}

TEST(CopilotTest, Rollcall) {
  // Create cluster with pilot, copilot and controltower only.
  LocalTestCluster cluster(info_log_, true, true, true);
  ASSERT_OK(cluster.GetStatus());

  port::Semaphore checkpoint;
  port::Semaphore checkpoint2;

  size_t num_msg = 100;
  size_t expected = num_msg / 2;

  // create a client to communicate with the Copilot
  MsgLoop loop(env_, env_options_, 58499, 1, info_log_, "test");
  StreamSocket socket(loop.CreateOutboundStream(
      cluster.GetCopilotHostIds().front().ToClientId(), 0));
  loop.RegisterCallbacks({
      {MessageType::mMetadata, [&](std::unique_ptr<Message> msg) {
        ASSERT_EQ(socket.GetStreamID(), msg->GetOrigin());
        ProcessMetadata(std::move(msg));
        if (expected == acked_msgs_.size()) {
          checkpoint.Post();
        }
      }},
      {MessageType::mDeliver, [](std::unique_ptr<Message>) {}},
      {MessageType::mGap, [](std::unique_ptr<Message>) {}},
  });
  env_->StartThread(CopilotTest::MsgLoopStart, &loop,
                    "testc-" + std::to_string(loop.GetHostId().port));
  ASSERT_OK(loop.WaitUntilRunning());
  ASSERT_EQ(rollcall_entries_.size(), 0);

  // Create a rollcall client
  NamespaceID ns = GuestNamespace;
  std::unique_ptr<ClientImpl> client;
  ASSERT_OK(cluster.CreateClient("ClientId1", &client, true));
  auto rollcall_callback = [this, &checkpoint2, &num_msg]
    (RollcallEntry entry) {
    ProcessRollcall(entry);
    if (rollcall_entries_.size() == num_msg) {
      checkpoint2.Post();
    }
  };
  // subscribe to rollcall topic for any new entries
  // that appear in the rollcall topic.
  RollcallImpl rollcall1(std::move(client), ns,
                         SubscriptionStart(0), rollcall_callback);

  // send subscribe messages to copilot
  for (size_t i = 0; i < expected; ++i) {
    std::string topic = "copilot_test_" + std::to_string(i);
    auto type = MetadataType::mSubscribe;
    MessageMetadata msg(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        { TopicPair(0, topic, type, ns) });
    sent_msgs_.insert(topic);
    ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  }
  // Ensure that subscriptions were acked from server.
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_EQ(expected, acked_msgs_.size());
  ASSERT_TRUE(sent_msgs_ == acked_msgs_);

  acked_msgs_.clear();
  sent_msgs_.clear();

  // send unsubscribe messages to copilot
  for (size_t i = 0; i < expected; ++i) {
    std::string topic = "copilot_test_" + std::to_string(i);
    auto type = MetadataType::mUnSubscribe;
    MessageMetadata msg(Tenant::GuestTenant,
                        MessageMetadata::MetaType::Request,
                        { TopicPair(0, topic, type, ns) });
    ClientID host = cluster.GetCopilotHostIds().front().ToClientId();
    sent_msgs_.insert(topic);
    ASSERT_OK(loop.SendRequest(msg, &socket, 0));
  }
  // Ensure that unsubscriptions were acked from server.
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_TRUE(sent_msgs_ == acked_msgs_);
  ASSERT_EQ(expected, acked_msgs_.size());

  // Ensure that all subscriptions were found in the rollcall log
  ASSERT_TRUE(checkpoint2.TimedWait(std::chrono::seconds(5)));

  // Verify that all entries in the rollcall log match our
  // subscription and unsubscription request
  ASSERT_EQ(rollcall_entries_.size(), num_msg);
  for (size_t i = 0; i < num_msg; ++i) {
    std::string topic = "copilot_test_" + std::to_string(i % expected);
    auto type = i < num_msg/2 ?
                  RollcallEntry::EntryType::SubscriptionRequest :
                  RollcallEntry::EntryType::UnSubscriptionRequest;

    // find this entry in the rollcall topic
    bool found = false;
    for (size_t j = 0; j < rollcall_entries_.size(); j++) {
      if (rollcall_entries_[j].GetType() == type &&
          rollcall_entries_[j].GetTopicName() == topic) {
        found = true;
        // found the entry we were looking for. Make it invalid
        // so that it does not match succeeding entries any more.
        rollcall_entries_[j].SetType( RollcallEntry::EntryType::Error);
        break;
      }
    }
    ASSERT_TRUE(found);
  }

  // Verify that statistics are recorded accurately. Account for the
  // fact that subscribing to the rollcall topic shows up as one
  // additional subscription in the statistic.
  const Statistics& stats = cluster.GetStatistics();
  std::string stats_report = stats.Report();
  ASSERT_NE(
    stats_report.find("rocketspeed.copilot.numwrites_rollcall_total: " +
            std::to_string(num_msg + 1)),
            std::string::npos);
  ASSERT_NE(
    stats_report.find("rocketspeed.copilot.numwrites_rollcall_failed: 0"),
            std::string::npos);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
