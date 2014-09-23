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
#include "src/port/port_posix.h"
#include "src/messages/messages.h"
#include "src/util/auto_roll_logger.h"

DEFINE_int32(num_threads, 16, "number of threads");
DEFINE_string(pilot_hostname, "localhost", "hostname of pilot");
DEFINE_int32(pilot_port, 58600, "port number of pilot");
DEFINE_int32(port, 58800, "port number of producer");
DEFINE_int32(message_size, 100, "message size (bytes)");
DEFINE_int64(num_topics, 1000000000, "number of topics");
DEFINE_int64(num_messages, 10000, "number of messages to send");
DEFINE_int64(message_rate, 100000, "messages per second (0 = unlimited)");
DEFINE_bool(await_ack, true, "wait for and include acks in times");
DEFINE_bool(logging, true, "enable/disable logging");
DEFINE_bool(report, true, "report results to stdout");

std::shared_ptr<rocketspeed::Logger> info_log;

struct Result {
  bool succeeded;
};

static const Result failed = { false, };

namespace rocketspeed {

Result ProducerWorker(int64_t num_messages, Client* producer) {
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

  Log(InfoLogLevel::INFO_LEVEL, info_log, "Starting message loop");
  info_log->Flush();

  // Calculate message rate for this worker.
  int64_t rate = FLAGS_message_rate / FLAGS_num_threads;

  // Number of messages we should have sent in 10ms
  int64_t rate_check = std::max<int64_t>(1, rate / 100);

  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < num_messages; ++i) {
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
    PublishStatus ps = producer->Publish(topic_name, topic_options, payload);

    if (!ps.status.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log,
        "Failed to send message number %lu (%s)",
        i, ps.status.ToString().c_str());
      info_log->Flush();
      return failed;
    }

    if (FLAGS_message_rate != 0 &&
        (i + 1) % rate_check == 0) {
      // Check if we are sending messages too fast, and if so, sleep.
      int64_t have_sent = i;

      // Time since we started sending messages.
      auto expired = std::chrono::steady_clock::now() - start;

      // Time that should have expired if we were sending at the desired rate.
      auto expected = std::chrono::microseconds(1000000 * have_sent / rate);

      // If we are ahead of schedule then sleep for the difference.
      if (expected > expired) {
        std::this_thread::sleep_for(expected - expired);
      }
    }
  }

  return Result { true };
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

  // Configuration for RocketSpeed.
  rocketspeed::HostId pilot(FLAGS_pilot_hostname, FLAGS_pilot_port);
  std::unique_ptr<rocketspeed::Configuration> config(
    rocketspeed::Configuration::Create(
      std::vector<rocketspeed::HostId>{ pilot },
      rocketspeed::Tenant(2),
      FLAGS_port));

  // Start/end time for benchmark.
  std::chrono::time_point<std::chrono::steady_clock> start, end;

  // Semaphore to signal when all messages have been ack'd
  rocketspeed::port::Semaphore all_messages_received;

  // Time last message was received.
  std::chrono::time_point<std::chrono::steady_clock> last_message;

  // Create callback for publish acks.
  int64_t messages_received = 0;
  auto callback = [&] (rocketspeed::ResultStatus rs) {
    ++messages_received;

    if (FLAGS_await_ack) {
      // This may be the last ack we receive, so set end to the time now.
      end = std::chrono::steady_clock::now();
      last_message = std::chrono::steady_clock::now();

      // If we've received all messages, let the main thread know to finish up.
      if (messages_received == FLAGS_num_messages) {
        all_messages_received.Post();
      }
    }
  };

  // Create RocketSpeed Client.
  rocketspeed::Client* producer = nullptr;
  if (!rocketspeed::Client::Open(config.get(), callback, &producer).ok()) {
    Log(rocketspeed::InfoLogLevel::WARN_LEVEL, info_log,
        "Failed to connect to RocketSpeed");
    info_log->Flush();
    return 1;
  }

  // Start the clock.
  start = std::chrono::steady_clock::now();

  // Create producer threads.
  // Distribute total number of messages among them.
  std::vector<std::future<Result>> futures;
  int64_t total_messages = FLAGS_num_messages;
  for (int32_t remaining = FLAGS_num_threads; remaining; --remaining) {
    int64_t num_messages = total_messages / remaining;
    futures.emplace_back(std::async(std::launch::async,
                                    &rocketspeed::ProducerWorker,
                                    num_messages,
                                    producer));
    total_messages -= num_messages;
  }
  assert(total_messages == 0);

  // Join all the threads to finish production.
  int ret = 0;
  for (int i = 0; i < static_cast<int>(futures.size()); ++i) {
    Result result = futures[i].get();
    if (!result.succeeded) {
      if (FLAGS_report) {
        printf("Thread %d failed to send all messages\n", i);
      }
      ret = 1;
    }
  }

  if (FLAGS_await_ack) {
    printf("Messages sent, awaiting acks...");
    fflush(stdout);

    // Wait for the all_messages_received semaphore to be posted.
    // Keep waiting as long as a message was received in the last second.
    auto timeout = std::chrono::seconds(1);
    do {
      all_messages_received.TimedWait(timeout);
    } while (messages_received != FLAGS_num_messages &&
             std::chrono::steady_clock::now() - last_message < timeout);
    printf(" done\n");
  } else {
    end = std::chrono::steady_clock::now();
  }

  // Calculate total time.
  auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
    end - start);

  if (ret == 0 && FLAGS_report) {
    uint32_t total_ms = static_cast<uint32_t>(total_time.count());
    if (total_ms == 0) {
      // To avoid divide-by-zero on near-instant benchmarks.
      total_ms = 1;
    }

    uint32_t msg_per_sec = 1000 *
                           FLAGS_num_messages /
                           total_ms;
    uint32_t bytes_per_sec = 1000 *
                             FLAGS_num_messages *
                             FLAGS_message_size /
                             total_ms;
    printf("%ld messages sent\n", FLAGS_num_messages);
    printf("%ld messages acked\n", messages_received);
    printf("%u messages/s\n", msg_per_sec);
    printf("%.2lf MB/s\n", bytes_per_sec * 1e-6);
  }

  delete producer;
  return ret;
}
