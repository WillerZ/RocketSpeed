// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <future>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <assert.h>
#include <gflags/gflags.h>
#include <signal.h>
#include <unistd.h>
#include "include/RocketSpeed.h"
#include "include/Types.h"
#include "src/port/port_posix.h"
#include "src/messages/messages.h"
#include "src/test/test_cluster.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/common/guid_generator.h"
#include "src/tools/rocketbench/random_distribution.h"
#include "src/client/client.h"

// This tool can behave as a standalone producer, a standalone
// consumer or both a producer and a consumer.
DEFINE_bool(start_producer, true, "starts the producer");
DEFINE_bool(start_consumer, true, "starts the consumer");
DEFINE_bool(start_local_server, true, "starts an embedded rocketspeed server");
DEFINE_string(storage_url, "", "Storage service URL for local server");

DEFINE_int32(num_threads, 4, "number of threads");
DEFINE_string(pilot_hostname, "localhost", "hostname of pilot");
DEFINE_string(copilot_hostname, "localhost", "hostname of copilot");
DEFINE_int32(pilot_port, 58600, "port number of pilot");
DEFINE_int32(copilot_port, 58600, "port number of copilot");
DEFINE_int32(message_size, 100, "message size (bytes)");
DEFINE_uint64(num_topics, 1000, "number of topics");
DEFINE_int64(num_messages, 10000, "number of messages to send");
DEFINE_int64(message_rate, 100000, "messages per second (0 = unlimited)");
DEFINE_bool(await_ack, true, "wait for and include acks in times");
DEFINE_bool(logging, true, "enable/disable logging");
DEFINE_bool(report, true, "report results to stdout");
DEFINE_int32(namespaceid, 101, "namespace id");
DEFINE_string(topics_distribution, "uniform",
"uniform, normal, poisson, fixed");
DEFINE_int64(topics_mean, 0,
"Mean for Normal and Poisson topic distributions (rounded to nearest int64)");
DEFINE_int64(topics_stddev, 0,
"Standard Deviation for Normal topic distribution (rounded to nearest int64)");

std::shared_ptr<rocketspeed::Logger> info_log;

typedef std::pair<rocketspeed::MsgId, uint64_t> MsgTime;

struct Result {
  bool succeeded;
  std::vector<MsgTime> msg_send_times;
};

static const Result failed = { false, {} };

namespace rocketspeed {

Result ProducerWorker(int64_t num_messages, NamespaceID namespaceid,
  Client* producer) {
  Env* env = Env::Default();

  // Random number generator.
  std::mt19937_64 rng;
  std::unique_ptr<rocketspeed::RandomDistributionBase>
    distr(GetDistributionByName(FLAGS_topics_distribution,
    0, FLAGS_num_topics - 1, FLAGS_topics_mean, FLAGS_topics_stddev));

  // Generate some dummy data.
  std::vector<char> data(FLAGS_message_size);
  const char* data_message = "RocketSpeed ";
  for (int i = 0; i < FLAGS_message_size; ++i) {
    data[i] = data_message[i % strlen(data_message)];
  }
  Slice payload(data.data(), data.size());

  LOG_INFO(info_log, "Starting message loop");
  info_log->Flush();

  // Calculate message rate for this worker.
  int64_t rate = FLAGS_message_rate / FLAGS_num_threads;

  // Record the send times for each message.
  std::vector<MsgTime> msg_send_times;
  msg_send_times.reserve(num_messages);

  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < num_messages; ++i) {
    // Create random topic name
    char topic_name[64];
    if (distr.get() != nullptr) {
      snprintf(topic_name, sizeof(topic_name),
               "benchmark.%lu",
               distr->generateRandomInt());
    } else {               // distribution is "fixed"
      snprintf(topic_name, sizeof(topic_name),
               "benchmark.%lu",
               i % 100);   // 100 messages per topic
    }

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

    // Add ID and timestamp to message ID.
    static std::atomic<uint64_t> message_index;
    uint64_t send_time = env->NowMicros();
    snprintf(data.data(), data.size(),
             "%lu %lu", message_index++, send_time);

    // Send the message
    PublishStatus ps = producer->Publish(topic_name,
                                         namespaceid,
                                         topic_options,
                                         payload);

    if (!ps.status.ok()) {
      LOG_WARN(info_log,
        "Failed to send message number %lu (%s)",
        i, ps.status.ToString().c_str());
      info_log->Flush();
      return failed;
    }

    msg_send_times.emplace_back(ps.msgid, send_time);

    if (FLAGS_message_rate) {
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
  return Result { true, std::move(msg_send_times) };
}

/*
 ** Produce messages
 */
int DoProduce(Client* producer,
              rocketspeed::NamespaceID nsid,
              rocketspeed::port::Semaphore* all_ack_messages_received,
              std::atomic<int64_t>* ack_messages_received,
              std::chrono::time_point<std::chrono::steady_clock>*
                                                     last_ack_message,
              std::vector<std::vector<MsgTime>>* all_msg_send_times){
  // Distribute total number of messages among them.
  std::vector<std::future<Result>> futures;
  int64_t total_messages = FLAGS_num_messages;
  for (int32_t remaining = FLAGS_num_threads; remaining; --remaining) {
    int64_t num_messages = total_messages / remaining;
    futures.emplace_back(std::async(std::launch::async,
                                    &rocketspeed::ProducerWorker,
                                    num_messages,
                                    nsid,
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
    all_msg_send_times->emplace_back(std::move(result.msg_send_times));
  }

  if (FLAGS_await_ack) {
    // Wait for the all_ack_messages_received semaphore to be posted.
    // Keep waiting as long as a message was received in the last 5 seconds.
    auto timeout = std::chrono::seconds(5);
    do {
      all_ack_messages_received->TimedWait(timeout);
    } while (ack_messages_received->load() != FLAGS_num_messages &&
             std::chrono::steady_clock::now() - *last_ack_message < timeout);

    ret = ack_messages_received->load() == FLAGS_num_messages ? 0 : 1;
  }
  return ret;
}

/**
 * Subscribe to topics.
 */
void DoSubscribe(Client* consumer, NamespaceID nsid) {
  SequenceNumber start = 0;   // start sequence number (0 = only new records)

  // subscribe 10K topics at a time
  unsigned int batch_size = 10240;

  std::vector<SubscriptionRequest> topics;
  topics.reserve(batch_size);

  // create all subscriptions from seqno 1
  SubscriptionRequest request(nsid, "", start);
  for (uint64_t i = 0; i < FLAGS_num_topics; i++) {
    request.topic_name.assign("benchmark." + std::to_string(i));
    topics.push_back(request);
    if (i % batch_size == batch_size - 1) {
      // send a subscription request
      consumer->ListenTopics(topics);
      topics.clear();
    }
  }

  // subscribe to all remaining topics
  if (topics.size() != 0) {
    consumer->ListenTopics(topics);
  }
}

/*
 ** Receive messages
 */
int DoConsume(rocketspeed::port::Semaphore* all_messages_received,
              std::atomic<int64_t>* messages_received,
              std::chrono::time_point<std::chrono::steady_clock>*
                                                     last_data_message) {
  // Wait for the all_messages_received semaphore to be posted.
  // Keep waiting as long as a message was received in the last 5 seconds.
  auto timeout = std::chrono::seconds(5);
  do {
    all_messages_received->TimedWait(timeout);
  } while (messages_received->load() != FLAGS_num_messages &&
           std::chrono::steady_clock::now() - *last_data_message < timeout);

  return messages_received->load() == FLAGS_num_messages ? 0 : 1;
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  rocketspeed::Env* env = rocketspeed::Env::Default();
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

  if (FLAGS_copilot_port > 65535 || FLAGS_copilot_port < 0) {
    fprintf(stderr, "copilot_port must be 0-65535.\n");
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
  if (!FLAGS_start_consumer && !FLAGS_start_producer) {
    fprintf(stderr, "You must specify at least one --start_producer "
            "or --start_consumer\n");
    return 1;
  }

  // Create logger
  if (FLAGS_logging) {
    if (!rocketspeed::CreateLoggerFromOptions(rocketspeed::Env::Default(),
                                              "",
                                              "LOG.rocketbench",
                                              0,
                                              0,
#ifdef NDEBUG
                                              rocketspeed::WARN_LEVEL,
#else
                                              rocketspeed::INFO_LEVEL,
#endif
                                              &info_log).ok()) {
      fprintf(stderr, "Error creating logger, aborting.\n");
      return 1;
    }
  } else {
    info_log = std::make_shared<rocketspeed::NullLogger>();
  }

  std::unique_ptr<rocketspeed::LocalTestCluster> test_cluster;
  if (FLAGS_start_local_server) {
    test_cluster.reset(new rocketspeed::LocalTestCluster(
                           info_log, FLAGS_storage_url));
  }

  // Configuration for RocketSpeed.
  rocketspeed::HostId pilot(FLAGS_pilot_hostname, FLAGS_pilot_port);
  rocketspeed::HostId copilot(FLAGS_copilot_hostname, FLAGS_copilot_port);

  // Create two configs: one for the prpducer and one for the consumer
  std::unique_ptr<rocketspeed::Configuration> pconfig(
    rocketspeed::Configuration::Create(
      std::vector<rocketspeed::HostId>{ pilot },
      std::vector<rocketspeed::HostId>{ copilot },
      rocketspeed::Tenant(102)));
  std::unique_ptr<rocketspeed::Configuration> cconfig(
    rocketspeed::Configuration::Create(
      std::vector<rocketspeed::HostId>{ pilot },
      std::vector<rocketspeed::HostId>{ copilot },
      rocketspeed::Tenant(102)));

  // Start/end time for benchmark.
  std::chrono::time_point<std::chrono::steady_clock> start, end;

  // Semaphore to signal when all messages have been ack'd
  rocketspeed::port::Semaphore all_ack_messages_received;

  // Time last ack/data message was received.
  std::chrono::time_point<std::chrono::steady_clock> last_ack_message;
  std::chrono::time_point<std::chrono::steady_clock> last_data_message;

  // Message ack times
  std::vector<MsgTime> msg_ack_times;
  msg_ack_times.reserve(FLAGS_num_messages);

  // Create callback for publish acks.
  std::atomic<int64_t> ack_messages_received{0};
  std::atomic<int64_t> failed_publishes{0};
  auto publish_callback = [&] (rocketspeed::ResultStatus rs) {
    ++ack_messages_received;

    if (FLAGS_await_ack) {
      // This may be the last ack we receive, so set end to the time now.
      end = std::chrono::steady_clock::now();
      last_ack_message = std::chrono::steady_clock::now();

      // Append receive time.
      msg_ack_times.emplace_back(rs.msgid, env->NowMicros());

      // If we've received all messages, let the main thread know to finish up.
      if (ack_messages_received.load() == FLAGS_num_messages) {
        all_ack_messages_received.Post();
      }
    }

    if (!rs.status.ok()) {
      ++failed_publishes;
      LOG_WARN(info_log, "Received publish failure response");
    }
  };

  // Semaphore to signal when all data messages have been received
  rocketspeed::port::Semaphore all_messages_received;

  // Data structure to track received message indices and latencies (microsecs).
  uint64_t latency_max = static_cast<uint64_t>(-1);
  std::vector<uint64_t> recv_latencies(FLAGS_num_messages, latency_max);

  // Create callback for processing messages received
  std::atomic<int64_t> messages_received{0};
  auto receive_callback = [&]
    (std::unique_ptr<rocketspeed::MessageReceived> rs) {
    uint64_t now = env->NowMicros();
    ++messages_received;
    last_data_message = std::chrono::steady_clock::now();

    // Parse message data to get received index.
    rocketspeed::Slice data = rs->GetContents();
    uint64_t message_index, send_time;
    std::sscanf(data.data(), "%lu %lu", &message_index, &send_time);
    if (message_index < recv_latencies.size()) {
      if (recv_latencies[message_index] == latency_max) {
        recv_latencies[message_index] = now - send_time;
      } else {
        LOG_WARN(info_log,
            "Received duplicate message index (%lu)",
            message_index);
      }
    } else {
      LOG_WARN(info_log,
          "Received out of bounds message index (%lu)",
          message_index);
    }

    // If we've received all messages, let the main thread know to finish up.
    if (messages_received.load() == FLAGS_num_messages) {
      all_messages_received.Post();
    }
  };

  // Subscribe callback.
  std::atomic<uint64_t> num_topics_subscribed{0};
  rocketspeed::port::Semaphore all_topics_subscribed;
  auto subscribe_callback = [&] (rocketspeed::SubscriptionStatus ss) {
    if (ss.subscribed) {
      if (++num_topics_subscribed == FLAGS_num_topics) {
        all_topics_subscribed.Post();
      }
    } else {
      LOG_WARN(info_log, "Received an unsubscribe response");
    }
  };

  rocketspeed::ClientImpl* producer = nullptr;
  rocketspeed::ClientImpl* consumer = nullptr;
  std::string clientid = rocketspeed::GUIDGenerator().GenerateString();

  // If the producer port and the consumer port are the same, then we
  // use a single client object. This allows the producer and the
  // consumer to share the same connections to the Cloud.
  if (FLAGS_start_producer && FLAGS_start_consumer) {
    producer = new rocketspeed::ClientImpl(clientid,
                                           pilot,
                                           copilot,
                                           rocketspeed::Tenant(102),
                                           publish_callback,
                                           subscribe_callback,
                                           receive_callback,
                                           nullptr,
                                           info_log);
    consumer = producer;
  } else {
    if (FLAGS_start_producer) {
      producer = new rocketspeed::ClientImpl(clientid,
                                             pilot,
                                             copilot,
                                             rocketspeed::Tenant(102),
                                             publish_callback,
                                             nullptr,
                                             nullptr,
                                             nullptr,
                                             info_log);
    }
    if (FLAGS_start_consumer) {
      consumer = new rocketspeed::ClientImpl(clientid,
                                             pilot,
                                             copilot,
                                             rocketspeed::Tenant(102),
                                             nullptr,
                                             subscribe_callback,
                                             receive_callback,
                                             nullptr,
                                             info_log);
    }
  }
  rocketspeed::NamespaceID nsid =
    static_cast<rocketspeed::NamespaceID>(FLAGS_namespaceid);

  // Subscribe to topics (don't count this as part of the time)
  // Also waits for the subscription responses.
  if (FLAGS_start_consumer) {
    DoSubscribe(consumer, nsid);
    if (!all_topics_subscribed.TimedWait(std::chrono::seconds(5))) {
      LOG_WARN(info_log, "Failed to subscribe to all topics");
      info_log->Flush();
      printf("Failed to subscribe to all topics (%lu/%lu)\n",
        num_topics_subscribed.load(),
        FLAGS_num_topics);

      return 1;
    }
  }

  // Vector of message IDs and the time they were sent.
  std::vector<std::vector<MsgTime>> all_msg_send_times;

  // Start the clock.
  start = std::chrono::steady_clock::now();

  std::future<int> producer_ret;
  std::future<int> consumer_ret;
  if (FLAGS_start_producer) {
    producer_ret = std::async(std::launch::async,
                              &rocketspeed::DoProduce,
                              producer,
                              nsid,
                              &all_ack_messages_received,
                              &ack_messages_received,
                              &last_ack_message,
                              &all_msg_send_times);
  }
  if (FLAGS_start_consumer) {
    consumer_ret = std::async(std::launch::async,
                              &rocketspeed::DoConsume,
                              &all_messages_received,
                              &messages_received,
                              &last_data_message);
  }

  int ret = 0;
  if (FLAGS_start_producer) {
    printf("Messages sent, awaiting acks...");
    fflush(stdout);

    ret = producer_ret.get();

    if (ack_messages_received.load() == FLAGS_num_messages) {
      printf(" done\n");
    } else {
      printf(" time out\n");
    }
  }
  if (FLAGS_start_consumer) {
    printf("Waiting for Messages to be received...");
    fflush(stdout);

    ret = consumer_ret.get();

    if (messages_received.load() == FLAGS_num_messages) {
      printf(" done\n");
    } else {
      printf(" time out\n");
    }
  }

  end = std::chrono::steady_clock::now();

  // Calculate total time.
  auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
    end - start);

  // Process the publish-ack latencies.
  // Accumulate individual worker results into single vector.
  std::vector<MsgTime> msg_send_times;
  for (const auto& worker_batch : all_msg_send_times) {
    // Insert this worker's results into msg_send_times.
    msg_send_times.insert(msg_send_times.end(),
                          worker_batch.begin(),
                          worker_batch.end());
  }

  // Now sort both the send and ack times so that the GUIDs are in order.
  std::sort(msg_send_times.begin(), msg_send_times.end());
  std::sort(msg_ack_times.begin(), msg_ack_times.end());

  // Now generate the latencies.
  std::vector<uint64_t> ack_latencies;
  size_t num_acks = std::min(msg_send_times.size(), msg_ack_times.size());
  ack_latencies.reserve(num_acks);
  for (size_t i = 0; i < num_acks; ++i) {
    ack_latencies.push_back(msg_ack_times[i].second - msg_send_times[i].second);
  }

  // Sort the latencies so we can work out percentiles.
  std::sort(ack_latencies.begin(), ack_latencies.end());
  std::sort(recv_latencies.begin(), recv_latencies.end());

  if (FLAGS_report) {
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

    printf("\n");
    printf("Results\n");
    printf("%ld messages sent\n", FLAGS_num_messages);
    printf("%ld messages sends acked\n", ack_messages_received.load());
    if (failed_publishes != 0) {
      printf("%ld published failed\n", failed_publishes.load());
    }
    if (FLAGS_start_consumer) {
      printf("%ld messages received\n", messages_received.load());
    }

    if (FLAGS_start_consumer &&
        messages_received.load() != FLAGS_num_messages) {
      // Print out dropped messages if there are any. This helps when
      // debugging problems.
      printf("\n");
      printf("Messages failed to receive\n");

      for (uint64_t i = 0; i < recv_latencies.size(); ++i) {
        // If latency is latency_max then it wasn't received.
        if (recv_latencies[i] == latency_max) {
          // Find the range of message IDs dropped (e.g. 100-200)
          uint64_t j;
          for (j = i; j < recv_latencies.size(); ++j) {
            if (recv_latencies[j] != latency_max) {
              break;
            }
          }
          // Print the dropped messages, either individal (e.g. 100) or
          // as a range (e.g. 100-200)
          if (i == j - 1) {
            printf("%lu\n", i);
          } else {
            printf("%lu-%lu\n", i, j - 1);
          }
          i = j;
        }
      }
    }

    // Only report results if everything succeeded.
    // Otherwise, they don't make sense.
    if (ret == 0) {
      printf("\n");
      printf("Throughput\n");
      printf("%u messages/s\n", msg_per_sec);
      printf("%.2lf MB/s\n", bytes_per_sec * 1e-6);

      // Report ack latencies.
      printf("\n");
      printf("Publish->Ack Latency (microseconds)\n");
      printf("p50   = %lu\n", ack_latencies[ack_latencies.size() * 0.50]);
      printf("p90   = %lu\n", ack_latencies[ack_latencies.size() * 0.90]);
      printf("p99   = %lu\n", ack_latencies[ack_latencies.size() * 0.99]);
      printf("p99.9 = %lu\n", ack_latencies[ack_latencies.size() * 0.999]);

      // Report receive latencies.
      if (FLAGS_start_consumer) {
        printf("\n");
        printf("Publish->Receive Latency (microseconds)\n");
        printf("p50   = %lu\n", recv_latencies[recv_latencies.size() * 0.50]);
        printf("p90   = %lu\n", recv_latencies[recv_latencies.size() * 0.90]);
        printf("p99   = %lu\n", recv_latencies[recv_latencies.size() * 0.99]);
        printf("p99.9 = %lu\n", recv_latencies[recv_latencies.size() * 0.999]);
      }

      if (FLAGS_start_local_server) {
        rocketspeed::Statistics stats(test_cluster->GetStatistics());
        if (producer) {
          stats.Aggregate(producer->GetStatistics());
        }
        if (consumer && consumer != producer) {
          stats.Aggregate(consumer->GetStatistics());
        }

        printf("\n");
        printf("Statistics\n");
        printf(stats.Report().c_str());
      }
    }
  }

  delete producer;
  if (producer != consumer) {
    delete consumer;
  }
  return ret;
}
