//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <unistd.h>
#include <chrono>
#include <numeric>
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
#include "src/util/control_tower_router.h"
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

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }
};

TEST(CopilotTest, WorkerMapping) {
  // Create cluster with controltower only.
  LocalTestCluster cluster(info_log_, true, false, false);
  ASSERT_OK(cluster.GetStatus());

  const int num_towers = 100;
  const int num_workers = 10;
  const int port = Copilot::DEFAULT_PORT - 99;

  MsgLoop loop(env_, EnvOptions(), port, num_workers, info_log_, "test");
  ASSERT_OK(loop.Initialize());

  std::unordered_map<ControlTowerId, HostId> control_towers;
  for (int i = 0; i < num_towers; ++i) {
    // Generate fake control towers.
    control_towers.emplace(i, HostId::CreateLocal(static_cast<uint16_t>(i)));
  }

  CopilotOptions options;
  options.info_log = info_log_;
  options.pilots.push_back(HostId::CreateLocal(62777));
  options.msg_loop = &loop;
  options.control_tower_connections = 4;
  options.log_router = cluster.GetLogRouter();
  options.control_tower_router = std::make_shared<ConsistentHashTowerRouter>(
      std::move(control_towers), 20, 1);
  Copilot* copilot;
  Status st = Copilot::CreateNewInstance(options, &copilot);
  ASSERT_TRUE(st.ok());
  loop.Stop();
  loop.Run();

  // Now check that each control tower is mapped to one worker.
  std::unordered_map<HostId, std::set<int>> tower_to_workers;
  auto log_range = std::pair<LogID, LogID>(1, 10000);
  for (LogID logid = log_range.first;
       logid <= log_range.second;
       ++logid) {
    // Find the tower responsible for this log.
    std::vector<HostId const*> host_ids;
    ASSERT_OK(options.control_tower_router->GetControlTowers(logid, &host_ids));
    ASSERT_TRUE(!host_ids.empty());

    // Find the worker responsible for this log.
    int worker_id = copilot->GetTowerWorker(logid, *host_ids[0]);
    tower_to_workers[*host_ids[0]].insert(worker_id);

    // Check that the tower maps to only one worker.
    ASSERT_LE(tower_to_workers[*host_ids[0]].size(),
              options.control_tower_connections);
  }
  copilot->Stop();
  delete copilot;
}

TEST(CopilotTest, NoLogger) {
  // Create cluster with tower+copilot (only need this for the log router).
  LocalTestCluster cluster(info_log_, true, false, false);
  ASSERT_OK(cluster.GetStatus());

  MsgLoop loop(env_,
               env_options_,
               0,
               1,
               std::make_shared<NullLogger>(),
               "test");
  ASSERT_OK(loop.Initialize());
  Copilot* copilot = nullptr;
  CopilotOptions options;
  options.pilots.push_back(HostId::CreateLocal(62777));
  options.msg_loop = &loop;
  options.log_dir = "///";  // invalid dir, will fail to create LOG file.
  options.log_router = cluster.GetLogRouter();
  ASSERT_OK(Copilot::CreateNewInstance(options, &copilot));
  loop.Stop();
  loop.Run();
  copilot->Stop();
  delete copilot;
}

TEST(CopilotTest, Rollcall) {
  using namespace std::placeholders;

  // Create cluster with pilot, copilot and controltower only.
  LocalTestCluster cluster(info_log_, true, true, true);
  ASSERT_OK(cluster.GetStatus());

  // Create a Client mock.
  MsgLoop client(env_, env_options_, 0, 1, info_log_, "client_mock");
  StreamSocket socket(
      client.CreateOutboundStream(cluster.GetCopilot()->GetHostId(), 0));
  client.RegisterCallbacks({
      {MessageType::mSubscribe, [](std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mUnsubscribe, [](std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mDeliverGap, [](std::unique_ptr<Message>, StreamID) {}},
      {MessageType::mDeliverData, [](std::unique_ptr<Message>, StreamID) {}},
  });
  ASSERT_OK(client.Initialize());
  MsgLoopThread client_thread(env_, &client, "client_mock");
  ASSERT_OK(client.WaitUntilRunning());

  // Create Rollcall client.
  std::unique_ptr<RollcallImpl> rollcall;
  {
    std::unique_ptr<ClientImpl> rc_client;
    ASSERT_OK(cluster.CreateClient(&rc_client, true));
    rollcall.reset(new RollcallImpl(std::move(rc_client), GuestTenant));
  }

  const RollcallShard num_shards = rollcall->GetNumShards(GuestNamespace);
  ASSERT_GE(num_shards, 2);
  const size_t expected = 50, num_msg = 2 * expected;
  port::Semaphore checkpoint;
  std::mutex callback_mutex;
  std::vector<RollcallEntry> rollcall_entries;
  std::unordered_map<Topic, size_t> receiving_shards;
  std::vector<size_t> num_received_per_shard(num_shards, 0);
  auto generic_cb = [&](size_t shard_id, RollcallEntry entry) {
    std::lock_guard<std::mutex> lock(callback_mutex);
    ASSERT_TRUE(rollcall_entries.size() != num_msg);
    ASSERT_TRUE(entry.GetType() != RollcallEntry::EntryType::Error);
    // Verify that all entries on a topic arrive on the same client.
    auto it = receiving_shards.find(entry.GetTopicName());
    if (it != receiving_shards.end()) {
      ASSERT_EQ(shard_id, it->second);
    } else {
      receiving_shards.emplace(entry.GetTopicName(), shard_id);
    }
    // Count received per client.
    num_received_per_shard.at(shard_id)++;
    // Collect entries and notify if we've collected all expected ones.
    rollcall_entries.push_back(entry);
    if (rollcall_entries.size() == num_msg) {
      checkpoint.Post();
    }
  };

  // Subscribe to each Rollcall shard.
  for (RollcallShard shard_id = 0; shard_id < num_shards; ++shard_id) {
    ASSERT_OK(rollcall->Subscribe(
        GuestNamespace, shard_id, std::bind(generic_cb, shard_id, _1)));
  }

  // Wait a little so that the subscribe to 0 isn't affected by rollcall writes.
  env_->SleepForMicroseconds(500000);

  // send subscribe messages to copilot
  for (SubscriptionID i = 0; i < expected; ++i) {
    std::string topic = "copilot_test_" + std::to_string(i);
    MessageSubscribe msg(Tenant::GuestTenant, GuestNamespace, topic, 0, i);
    ASSERT_OK(client.SendRequest(msg, &socket, 0));
  }

  // send unsubscribe messages to copilot
  for (SubscriptionID i = 0; i < expected; ++i) {
    std::string topic = "copilot_test_" + std::to_string(i);
    MessageUnsubscribe msg(
        Tenant::GuestTenant, i, MessageUnsubscribe::Reason::kRequested);
    ASSERT_OK(client.SendRequest(msg, &socket, 0));
  }

  // Ensure that all expected entries were received by Rolcall clients.
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(5)));
  std::lock_guard<std::mutex> lock(callback_mutex);
  // Verify that every client got something. This might get flaky when we change
  // topic names or Rollcall sharding hash.
  for (size_t shard_id = 0; shard_id < num_shards; ++shard_id) {
    ASSERT_TRUE(num_received_per_shard.at(shard_id) > 0);
  }

  // Verify that all entries in the rollcall log match our
  // subscription and unsubscription request
  ASSERT_EQ(rollcall_entries.size(), num_msg);
  for (size_t i = 0; i < num_msg; ++i) {
    std::string topic = "copilot_test_" + std::to_string(i % expected);
    auto type = i < num_msg / 2
                    ? RollcallEntry::EntryType::SubscriptionRequest
                    : RollcallEntry::EntryType::UnSubscriptionRequest;

    // find this entry in the rollcall topic
    bool found = false;
    for (size_t j = 0; j < rollcall_entries.size(); j++) {
      if (rollcall_entries[j].GetType() == type &&
          rollcall_entries[j].GetTopicName() == topic) {
        found = true;
        // found the entry we were looking for. Make it invalid
        // so that it does not match succeeding entries any more.
        rollcall_entries[j].SetType(RollcallEntry::EntryType::Error);
        break;
      }
    }
    ASSERT_TRUE(found);
  }

  // Verify that statistics are recorded accurately. Account for the
  // fact that subscribing to the rollcall topic shows up as one
  // additional subscription in the statistic.
  Statistics stats = cluster.GetCopilot()->GetStatisticsSync();
  stats.Aggregate(cluster.GetCopilot()->GetMsgLoop()->GetStatisticsSync());
  std::string stats_report = stats.Report();
  ASSERT_EQ(stats.GetCounterValue("copilot.numwrites_rollcall_total"),
            num_msg + num_shards);
  ASSERT_EQ(stats.GetCounterValue("copilot.numwrites_rollcall_failed"), 0);
  ASSERT_EQ(stats.GetCounterValue("cockpit.messages_received.subscribe"),
            num_msg / 2 + num_shards);
  ASSERT_EQ(stats.GetCounterValue("cockpit.messages_received.unsubscribe"),
            num_msg / 2);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
