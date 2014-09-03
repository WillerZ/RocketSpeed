// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <assert.h>
#include <gflags/gflags.h>
#include <signal.h>
#include <unistd.h>
#include <future>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "include/RocketSpeed.h"
#include "src/messages/messages.h"
#include "src/util/auto_roll_logger.h"

DEFINE_int32(num_threads, 16, "number of threads");
DEFINE_string(pilot_hostname, "localhost", "hostname of pilot");
DEFINE_int32(pilot_port, 58600, "port number of pilot");
DEFINE_int32(port, 58800, "first port number of producer");
DEFINE_int32(message_size, 100, "message size (bytes)");
DEFINE_int64(num_topics, 1000000000, "number of topics");
DEFINE_int64(num_messages, 10000, "number of messages to send");
DEFINE_bool(logging, true, "enable/disable logging");
DEFINE_bool(report, true, "report results to stdout");

std::shared_ptr<rocketspeed::Logger> info_log;

struct Result {
  bool succeeded;
  std::chrono::milliseconds time;
};

static const Result failed = { false, };

namespace rocketspeed {

Result ProducerWorker(int64_t num_messages, int port) {
  // Configuration for RocketSpeed.
  HostId pilot(FLAGS_pilot_hostname, FLAGS_pilot_port);
  Configuration* config = Configuration::Create(std::vector<HostId>{ pilot },
                                                Tenant(2),
                                                port);

  // Create RocketSpeed Producer.
  Producer* producer = nullptr;
  if (!Producer::Open(config, &producer).ok()) {
    Log(InfoLogLevel::WARN_LEVEL, info_log,
        "[port %d] Failed to connect to RocketSpeed", port);
    info_log->Flush();
    return failed;
  }

  // Random number generator.
  std::mt19937_64 rng;
  std::uniform_int_distribution<uint64_t> distr(0, FLAGS_num_topics - 1);

  // Generate some dummy data.
  std::vector<char> data(FLAGS_message_size);
  const char* data_message = "RocketSpeed ";
  for (int i = 0; i < FLAGS_message_size; ++i) {
    data[i] = data_message[i % strlen(data_message)];
  }
  Slice payload(data.data(), data.size());

  Log(InfoLogLevel::INFO_LEVEL, info_log,
      "[port %d] Starting message loop", port);
  info_log->Flush();

  auto start = std::chrono::high_resolution_clock::now();
  for (int64_t i = 0; i < num_messages; ++i) {
    // Start the clock after the first message is sent, since the
    // connection may be cold and skew the overall throughput.
    if (i == 1) {
      start = std::chrono::high_resolution_clock::now();
    }

    // Create random topic name
    char topic_name[64];
    snprintf(topic_name, sizeof(topic_name), "benchmark.%lu", distr(rng));

    // Random topic options
    TopicOptions topic_options;
    switch (rng() % Retention::Total) {
      case 0:
        topic_options.retention = Retention::OneHour;
        break;
      case 1:
        topic_options.retention = Retention::OneDay;
        break;
      case 2:
        topic_options.retention = Retention::OneWeek;
        break;
    }

    // Send the message
    ResultStatus rs = producer->Publish(topic_name, topic_options, payload);

    if (!rs.status.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log,
        "Failed to send message number %lu (%s)",
        i, rs.status.ToString().c_str());
      info_log->Flush();
      delete producer;
      return failed;
    }
  }
  auto end = std::chrono::high_resolution_clock::now();

  Log(InfoLogLevel::INFO_LEVEL, info_log,
      "[port %d] Successfully sent all messages", port);
  info_log->Flush();
  delete producer;

  return Result {
    true,
    std::chrono::duration_cast<std::chrono::milliseconds>(end - start) };
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  // Ignore SIGPIPE, we'll just handle the EPIPE returned by write.
  signal(SIGPIPE, SIG_IGN);

  // Validate flags
  if (FLAGS_num_threads <= 0) {
    fprintf(stderr, "num_threads must be greater than 0.\n");
    return 1;
  }

  if (FLAGS_pilot_port > 65535 || FLAGS_pilot_port < 0) {
    fprintf(stderr, "pilot_port must be 0-65535.\n");
    return 1;
  }

  if (FLAGS_port > 65535 || FLAGS_port < 0) {
    fprintf(stderr, "port must be 0-65535.\n");
    return 1;
  }

  if (FLAGS_message_size <= 0 || FLAGS_message_size > 1024*1024) {
    fprintf(stderr, "message_size must be 0-1MB.\n");
    return 1;
  }

  if (FLAGS_num_topics <= 0) {
    fprintf(stderr, "num_topics must be greater than 0.\n");
    return 1;
  }

  if (FLAGS_num_messages <= 0) {
    fprintf(stderr, "num_messages must be greater than 0.\n");
    return 1;
  }

  // Create logger
  if (FLAGS_logging) {
    if (!rocketspeed::CreateLoggerFromOptions(rocketspeed::Env::Default(),
                                              "",
                                              0,
                                              0,
                                              rocketspeed::INFO_LEVEL,
                                              &info_log).ok()) {
      fprintf(stderr, "Error creating logger, aborting.\n");
      return 1;
    }
  } else {
    info_log = std::make_shared<rocketspeed::NullLogger>();
  }

  // Create producer threads.
  // Distribute total number of messages among them.
  std::vector<std::future<Result>> futures;
  int64_t total_messages = FLAGS_num_messages;
  for (int32_t remaining = FLAGS_num_threads; remaining; --remaining) {
    int64_t num_messages = total_messages / remaining;
    int port = FLAGS_port + (FLAGS_num_threads - remaining);
    futures.emplace_back(std::async(std::launch::async,
                                    &rocketspeed::ProducerWorker,
                                    num_messages,
                                    port));
    total_messages -= num_messages;
  }
  assert(total_messages == 0);

  // Join all the threads to finish production.
  int ret = 0;
  std::chrono::milliseconds total_time(0);
  std::vector<Result> results;
  for (int i = 0; i < static_cast<int>(futures.size()); ++i) {
    Result result = futures[i].get();
    results.emplace_back(result);

    if (FLAGS_report) {
      if (result.succeeded) {
        total_time += result.time;
      } else {
        printf("Thread %d failed to send all messages\n", i);
      }
    }

    if (!result.succeeded) {
      ret = 1;
    }
  }

  if (ret == 0 && FLAGS_report) {
    uint32_t total_ms = static_cast<uint32_t>(total_time.count());
    uint32_t msg_per_sec = 1000 *
                           FLAGS_num_threads *
                           FLAGS_num_messages /
                           total_ms;
    uint32_t bytes_per_sec = 1000 *
                             FLAGS_num_messages *
                             FLAGS_message_size /
                             total_ms;
    printf("%u messages/s\n", msg_per_sec);
    printf("%.2lf MB/s\n", bytes_per_sec * 1e-6);
  }

  return ret;
}
