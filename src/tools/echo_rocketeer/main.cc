#include <stdio.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <sstream>
#include <thread>
#include "include/Env.h"
#include "include/Rocketeer.h"
#include "include/RocketeerServer.h"
#include "src/port/port.h"

using namespace rocketspeed;

DEFINE_uint64(port, 5834, "port to listen on");
DEFINE_uint64(threads, 16, "number of threads");
DEFINE_uint64(tick_micros, 500000, "microseconds between deliveries");

/**
 * Rocketeer that periodically echoes back the topic name on each subscription
 * until unsubscribe.
 */
class EchoRocketeer : public Rocketeer {
 public:
  explicit EchoRocketeer(RocketeerServer* server)
  : server_(server) {
    // Start a thread that will deliver a message on each topic periodically.
    thread_ = Env::Default()->StartThread(
      [this] () {
        while (!done_.TimedWait(std::chrono::microseconds(FLAGS_tick_micros))) {
          std::lock_guard<std::mutex> lock(task_lock_);
          for (auto& entry : tasks_) {
            Task& task = entry.second;
            if (server_->Deliver(entry.first, task.namespace_id, task.topic,
                task.seqno, task.topic)) {
              task.seqno++;
            }
          }
        }
      },
      "echotick");
  }

  void Stop() {
    done_.Post();
    Env::Default()->WaitForJoin(thread_);
  }

  void HandleNewSubscription(
      Flow* flow, InboundID id, SubscriptionParameters params) override {
    Task task;
    task.namespace_id = params.namespace_id;
    task.topic = params.topic_name;
    RS_ASSERT(params.cursors.size() == 0);
    RS_ASSERT(params.cursors[0].source == "");
    task.seqno = params.cursors[0].seqno + 1;
    std::lock_guard<std::mutex> lock(task_lock_);
    tasks_.emplace(id, std::move(task));
  }

  void HandleTermination(
      Flow*, InboundID id, TerminationSource) override {
    std::lock_guard<std::mutex> lock(task_lock_);
    tasks_.erase(id);
  }

 private:
  struct Task {
    NamespaceID namespace_id;
    Topic topic;
    SequenceNumber seqno;
  };
  std::mutex task_lock_;
  std::unordered_map<InboundID, Task> tasks_;
  RocketeerServer* server_;
  port::Semaphore done_;
  Env::ThreadId thread_;
};

int main(int argc, char** argv) {
  // Start AcceptAll Rocketeer listening on port supplied in flags.
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);
  rocketspeed::Env::InstallSignalHandlers();

  RocketeerOptions options;
  options.port = static_cast<uint16_t>(FLAGS_port);
  options.stats_prefix = "echo";

  std::vector<std::unique_ptr<EchoRocketeer>> rocketeers;
  auto server = RocketeerServer::Create(std::move(options));
  for (uint64_t i = 0; i < FLAGS_threads; ++i) {
    rocketeers.emplace_back(new EchoRocketeer(server.get()));
    server->Register(rocketeers.back().get());
  }

  auto st = server->Start();
  if (!st.ok()) {
    fprintf(stderr, "Failed to start server: %s\n", st.ToString().c_str());
    return 1;
  }
  pause();
  for (auto& r : rocketeers) {
    r->Stop();
  }
  server->Stop();
  return 0;
}
