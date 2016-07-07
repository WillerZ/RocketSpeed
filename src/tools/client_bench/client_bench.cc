#include "include/Logger.h"
#include "include/RocketeerServer.h"
#include "include/RocketSpeed.h"
#include "include/Types.h"

#include "external/folly/Memory.h"

#include "src/test/test_cluster.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/testutil.h"
#include "src/util/xxhash.h"

#include "stdlib.h"
#include "stdio.h"
#include "string.h"

#include <gflags/gflags.h>
#if defined(JEMALLOC) && defined(HAVE_JEMALLOC)
#include <jemalloc/jemalloc.h>
#endif

#include <unistd.h>
#include <memory>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>
#include <random>

DEFINE_uint64(seed, 0, "random seed");
DEFINE_uint64(subscriptions,
              10000000,
              "Amount of issued Subscribe calls");
DEFINE_bool(logging, false, "enable/disable logging");
DEFINE_uint64(topic_size, 20, "topic name size in bytes");
DEFINE_string(jemalloc_output,
              "/tmp/client_bench",
              "jemalloc stats output file.");
DEFINE_uint64(client_threads, 4, "number of client threads");
DEFINE_uint64(shards, 500000, "number of topic shards");
DEFINE_bool(round_robin_shard, false, "subscribe to shards using round robin");

using rocketspeed::HostId;
using rocketspeed::ShardingStrategy;
using rocketspeed::Slice;
using rocketspeed::Status;

class BadPublisherRouter : public rocketspeed::PublisherRouter {
 public:
  BadPublisherRouter() {}

  virtual ~BadPublisherRouter() {}

  Status GetPilot(HostId* pilot_out) const override {
    return Status::TimedOut();
  }
};

class SimpleShardingStrategy : public ShardingStrategy {
 public:
  size_t GetShard(Slice namespace_id, Slice topic_name) const override {
    if (FLAGS_round_robin_shard) {
      // Topic name's first 4 bytes will be set to shard number.
      return *reinterpret_cast<const uint32_t*>(topic_name.data()) %
        FLAGS_shards;
    } else {
      // Normally, just hash the topic name.
      return rocketspeed::XXH64(topic_name.data(), topic_name.size(), 0) %
        FLAGS_shards;
    }
  }

  size_t GetVersion() override { return 0; }
  HostId GetHost(size_t) override { return HostId(); }
  void MarkHostDown(const HostId& host_id) override {}
};

Status CreateClient(std::unique_ptr<rocketspeed::Client>& client) {
  rocketspeed::ClientOptions client_options;
  client_options.publisher.reset(new BadPublisherRouter());
  client_options.sharding.reset(new SimpleShardingStrategy());
  client_options.num_workers = static_cast<int>(FLAGS_client_threads);
  auto sharding = client_options.sharding;
  client_options.thread_selector =
    [sharding](size_t num_threads, Slice namespace_id, Slice topic_name) {
      return sharding->GetShard(namespace_id, topic_name) % num_threads;
    };
  if (FLAGS_logging) {
    std::shared_ptr<rocketspeed::Logger> info_log;
    auto st =
        rocketspeed::CreateLoggerFromOptions(rocketspeed::Env::Default(),
                                             "logs",
                                             "LOG.client_bench",
                                             0,
                                             0,
                                             rocketspeed::INFO_LEVEL,
                                             &info_log);

    if (!st.ok()) {
      fprintf(stderr, "Cannot create logger");
      return st;
    }
    client_options.info_log = info_log;
  }

  return rocketspeed::Client::Create(std::move(client_options), &client);
}

void PrintJEMallocStats() {
#if defined(JEMALLOC) && defined(HAVE_JEMALLOC)
  auto file_writer = [](void* cbopaque, const char* str) {
    std::ofstream* outfile = (std::ofstream*)cbopaque;
    *outfile << str;
  };

  std::ofstream outfile(FLAGS_jemalloc_output, std::ofstream::out);
  malloc_stats_print(file_writer, &outfile, nullptr);
  outfile.close();

  printf("jemalloc stats at '%s' !\n", FLAGS_jemalloc_output.c_str());
#else
  fprintf(stderr, "ERROR: jemalloc stats not available.\n");
#endif
}

int main(int argc, char** argv) {
  rocketspeed::Random rnd(static_cast<uint32_t>(FLAGS_seed));

  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_round_robin_shard) {
    RS_ASSERT(FLAGS_topic_size >= 4) << "Topic size must be at least 4";
  }

  std::unique_ptr<rocketspeed::Client> client;
  auto st = CreateClient(client);
  if (!st.ok()) {
    return 1;
  }

  size_t before = 0;
  st = rocketspeed::Env::Default()->GetVirtualMemoryUsed(&before);
  if (!st.ok()) {
    fprintf(stderr, "ERROR: Cannot get VMM, \"%s\"\n", st.ToString().c_str());
  }

  using clock = std::chrono::steady_clock;
  std::size_t i = 0;
  auto start_time = clock::now();
  std::chrono::seconds print_delay(1);
  auto next_time = start_time + print_delay;
  std::size_t last_subs = i;
  union {
    // Shard allocation for round robin.
    uint32_t rr_shard = 0;
    char shard_bytes[sizeof(rr_shard)];
  };
  for (; i < FLAGS_subscriptions; ++i) {
    std::string holder;
    auto slice = rocketspeed::test::RandomString(
        &rnd, static_cast<int>(FLAGS_topic_size), &holder);
    if (FLAGS_round_robin_shard) {
      // With round robin shard selection, we set the first 4 bytes of the
      // topic name to a uint32_t of the shard.
      for (size_t j = 0; j < sizeof(shard_bytes); ++j) {
        holder[j] = shard_bytes[j];
      }
      rr_shard = static_cast<uint32_t>((rr_shard + 1) % FLAGS_shards);
    }
    auto subscription_handle =
        client->Subscribe({rocketspeed::Tenant::GuestTenant,
                           rocketspeed::GuestNamespace,
                           slice.ToString(),
                           0},
                          folly::make_unique<rocketspeed::Observer>());
    if (subscription_handle == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      --i;
    }
    auto now = clock::now();
    if (now > next_time || i == FLAGS_subscriptions - 1) {
      auto total_time = now - start_time;
      auto total_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
          total_time).count();
      auto rate_now = i - last_subs;
      auto rate = (static_cast<double>(i) * 1000.0)
                  / static_cast<double>(total_time_ms);
      size_t after = 0;
      rocketspeed::Env::Default()->GetVirtualMemoryUsed(&after);
      printf("time: %-6.0lf "
             "subs: %-12zu "
             "rate-now: %-10zu "
             "rate-overall: %-10.0lf "
             "mem/sub: %-6s\n",
        double(total_time_ms) / 1000.0,
        i + 1,
        rate_now,
        rate,
        after ? rocketspeed::BytesToString((after - before) / i).c_str() :
          "---");
      next_time += print_delay;
      last_subs = i;
    }
  }

  PrintJEMallocStats();

  // You may also try the following for a deeper analysis of memory allocation.
  // 1. Uncomment the following lines
  //      const char *fileName = "jeprof.out";
  //      mallctl("prof.dump",
  //              nullptr,
  //              nullptr,
  //              &fileName,
  //              sizeof(const char *));
  // 2. Recompile the benchmark and run as following:
  //      $ export MALLOC_CONF=prof:true
  //      $ ./client_bench
  //      $ unset MALLOC_CONF
  //    You can explore other possible options for MALLOC_CONF here:
  //      www.canonware.com/download/jemalloc/jemalloc-latest/doc/jemalloc.html
  // 3. Generate PDF based on the collected data
  //      $ jeprof --pdf --lines ./client_bench jeprof.out > je.pdf
  //    You can explore other jeprof options by running
  //      $ jeprof --help
}
