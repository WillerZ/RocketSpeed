#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Types.h"

#include "external/folly/Memory.h"

#include "src/engine/rocketeer_server.h"
#include "src/test/test_cluster.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/testutil.h"

#include "stdlib.h"
#include "stdio.h"
#include "string.h"

#include <gflags/gflags.h>
#ifdef JEMALLOC
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
DEFINE_uint64(subscribe_calls_amount,
              10000000,
              "Amount of issued Subscribe calls");
DEFINE_bool(logging, false, "enable/disable logging");
DEFINE_uint64(topic_size, 20, "topic name size in bytes");
DEFINE_string(jemalloc_output,
              "/tmp/client_memory_usage_bench",
              "jemalloc stats output file.");

class BadConfiguration : public rocketspeed::Configuration {
 public:
  BadConfiguration() {}

  virtual ~BadConfiguration() {}

  virtual rocketspeed::Status GetPilot(rocketspeed::HostId* pilot_out) const {
    return rocketspeed::Status::TimedOut();
  }

  virtual rocketspeed::Status GetCopilot(
      rocketspeed::HostId* copilot_out) const {
    return rocketspeed::Status::TimedOut();
  }

  virtual uint64_t GetCopilotVersion() const { return 0; }
};

rocketspeed::Status CreateClient(std::unique_ptr<rocketspeed::Client>& client) {
  rocketspeed::ClientOptions client_options;
  client_options.config.reset(new BadConfiguration());
  if (FLAGS_logging) {
    std::shared_ptr<rocketspeed::Logger> info_log;
    auto st =
        rocketspeed::CreateLoggerFromOptions(rocketspeed::Env::Default(),
                                             "logs",
                                             "LOG.client_memory_used_bench",
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
#ifdef JEMALLOC
  auto file_writer = [](void* cbopaque, const char* str) {
    std::ofstream* outfile = (std::ofstream*)cbopaque;
    *outfile << str;
  };

  std::ofstream outfile(FLAGS_jemalloc_output, std::ofstream::out);
  malloc_stats_print(file_writer, &outfile, nullptr);
  outfile.close();

  printf("jemalloc stats at '%s' !\n", FLAGS_jemalloc_output.c_str());
#else
  printf("jemalloc stats not available.\n");
#endif
}

int main(int argc, char** argv) {
  rocketspeed::Random rnd(static_cast<uint32_t>(FLAGS_seed));

  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<rocketspeed::Client> client;
  auto st = CreateClient(client);
  if (!st.ok()) {
    return 1;
  }

  size_t before = -1;
  if (!rocketspeed::Env::Default()->GetVirtualMemoryUsed(&before).ok()) {
    printf("Cannot get used virtual memory amount\n");
    return -1;
  }

  std::size_t i = 0;
  for (; i < FLAGS_subscribe_calls_amount; ++i) {
    std::string holder;
    auto slice = rocketspeed::test::RandomString(
        &rnd, static_cast<int>(FLAGS_topic_size), &holder);
    auto subscription_handle =
        client->Subscribe({rocketspeed::Tenant::GuestTenant,
                           rocketspeed::GuestNamespace,
                           slice.ToString(),
                           0},
                          folly::make_unique<rocketspeed::Observer>());
    if (subscription_handle == 0) {
      fprintf(stderr,
              "Ran out of subscriptions. "
              "This is because of flow control. Sleep a bit\n");
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      --i;
    }
  }

  printf("Waiting some time to let subscription requests be processed...\n");
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  size_t after = -1;
  if (!rocketspeed::Env::Default()->GetVirtualMemoryUsed(&after).ok()) {
    printf("Cannot get used virtual memory amount\n");
    return -1;
  }
  printf("Subscriptions: %zu\n", i);
  printf("Memory consumption: %s\n",
         rocketspeed::BytesToString(after - before).c_str());
  printf("Memory consumption per subscription: %s\n",
         rocketspeed::BytesToString((after - before) / i).c_str());

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
  //      $ ./client_memory_used_bench
  //      $ unset MALLOC_CONF
  //    You can explore other possible options for MALLOC_CONF here:
  //      www.canonware.com/download/jemalloc/jemalloc-latest/doc/jemalloc.html
  // 3. Generate PDF based on the collected data
  //      $ jeprof --pdf --lines ./client_memory_used_bench jeprof.out > je.pdf
  //    You can explore other jeprof options by running
  //      $ jeprof --help
}
