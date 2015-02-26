// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
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
#include "include/WakeLock.h"
#include "src/port/port.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#if !defined(OS_ANDROID)
#include "src/test/test_cluster.h"
#endif
#include "src/util/auto_roll_logger.h"
#include "src/util/common/guid_generator.h"
#include "src/util/parsing.h"
#include "src/tools/rocketbench/random_distribution.h"
#include "src/client/client.h"
#include "src/client/client_env.h"

// This tool can behave as a standalone producer, a standalone
// consumer or both a producer and a consumer.
DEFINE_bool(start_producer, true, "starts the producer");
DEFINE_bool(start_consumer, true, "starts the consumer");
DEFINE_bool(start_local_server, true, "starts an embedded rocketspeed server");
DEFINE_string(storage_url, "", "Storage service URL for local server");

DEFINE_int32(num_threads, 8, "number of threads");
DEFINE_string(pilot_hostnames, "localhost", "hostnames of pilots");
DEFINE_string(copilot_hostnames, "localhost", "hostnames of copilots");
DEFINE_int32(pilot_port, 58600, "port number of pilot");
DEFINE_int32(copilot_port, 58600, "port number of copilot");
DEFINE_int32(client_workers, 32, "number of client workers");
DEFINE_int32(message_size, 100, "message size (bytes)");
DEFINE_uint64(num_topics, 1000000, "number of topics");
DEFINE_int64(num_messages, 10000, "number of messages to send");
DEFINE_int64(message_rate, 100000, "messages per second (0 = unlimited)");
DEFINE_bool(await_ack, true, "wait for and include acks in times");
DEFINE_bool(delay_subscribe, false, "start reading after publishing");
DEFINE_bool(logging, true, "enable/disable logging");
DEFINE_bool(report, true, "report results to stdout");
DEFINE_int32(namespaceid, 101, "namespace id");
DEFINE_string(topics_distribution, "uniform",
"uniform, normal, poisson, fixed");
DEFINE_int64(topics_mean, 0,
"Mean for Normal and Poisson topic distributions (rounded to nearest int64)");
DEFINE_int64(topics_stddev, 0,
"Standard Deviation for Normal topic distribution (rounded to nearest int64)");
DEFINE_int32(wait_for_debugger, 0, "wait for debugger to attach to me");

std::shared_ptr<rocketspeed::Logger> info_log;


typedef std::pair<rocketspeed::MsgId, uint64_t> MsgTime;

struct Result {
  bool succeeded;
};

// wait for 5 seconds of inactivity before declaring that something is
// not working.
static int idle_timeout = 5;

struct ProducerArgs {
  std::vector<std::unique_ptr<rocketspeed::ClientImpl>>* producers;
  rocketspeed::NamespaceID nsid;
  rocketspeed::port::Semaphore* all_ack_messages_received;
  std::atomic<int64_t>* ack_messages_received;
  std::chrono::time_point<std::chrono::steady_clock>* last_ack_message;
  rocketspeed::PublishCallback publish_callback;
  bool result;
};

struct  ProducerWorkerArgs {
  int64_t num_messages;
  rocketspeed::NamespaceID namespaceid;
  rocketspeed::Client* producer;
  rocketspeed::PublishCallback publish_callback;
  bool result;
};

struct ConsumerArgs {
  rocketspeed::port::Semaphore* all_messages_received;
  std::atomic<int64_t>* messages_received;
  std::chrono::time_point<std::chrono::steady_clock>* last_data_message;
  bool result;
};

namespace rocketspeed {

static void ProducerWorker(void* param) {
  ProducerWorkerArgs* args = static_cast<ProducerWorkerArgs*>(param);
  int64_t num_messages = args->num_messages;
  NamespaceID namespaceid = args->namespaceid;
  Client* producer = args->producer;
  PublishCallback publish_callback = args->publish_callback;
  args->result = false;

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

  auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < num_messages; ++i) {
    // Create random topic name
    char topic_name[64];
    uint64_t topic_num = distr.get() != nullptr ?
                         distr->generateRandomInt() :
                         i % 100;  // "fixed", 100 messages per topic
    snprintf(topic_name, sizeof(topic_name),
             "benchmark.%llu",
             static_cast<long long unsigned int>(topic_num));

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
    uint64_t index = message_index++;
    snprintf(data.data(), data.size(),
             "%llu %llu",
             static_cast<long long unsigned int>(index),
             static_cast<long long unsigned int>(send_time));

    // Send the message
    PublishStatus ps = producer->Publish(topic_name,
                                         namespaceid,
                                         topic_options,
                                         payload,
                                         publish_callback);

    if (!ps.status.ok()) {
      LOG_WARN(info_log,
        "Failed to send message number %llu (%s)",
        static_cast<long long unsigned int>(i),
        ps.status.ToString().c_str());
      info_log->Flush();
      args->result = false;
    }

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
  args->result = true;
}

/*
 ** Produce messages
 */
static void DoProduce(void* params) {

  struct ProducerArgs* args = static_cast<ProducerArgs*>(params);
  std::vector<std::unique_ptr<ClientImpl>>* producers =
    args->producers;
  rocketspeed::NamespaceID namespaceid = args->nsid;
  rocketspeed::port::Semaphore* all_ack_messages_received =
    args->all_ack_messages_received;
  std::atomic<int64_t>* ack_messages_received = args->ack_messages_received;
  std::chrono::time_point<std::chrono::steady_clock>* last_ack_message =
    args->last_ack_message;
  rocketspeed::PublishCallback publish_callback = args->publish_callback;
  Env* env = Env::Default();

  // Distribute total number of messages among them.
  std::vector<Env::ThreadId> thread_ids;
  int64_t total_messages = FLAGS_num_messages;
  size_t p = 0;
  ProducerWorkerArgs pargs[1024]; // no more than 1K threads

  for (int32_t remaining = FLAGS_num_threads; remaining; --remaining) {
    int64_t num_messages = total_messages / remaining;
    ProducerWorkerArgs* parg = &pargs[p];
    parg->num_messages = num_messages;
    parg->namespaceid = namespaceid;
    parg->producer = (*producers)[p % producers->size()].get();
    parg->publish_callback = publish_callback;

    thread_ids.push_back(env->StartThread(rocketspeed::ProducerWorker, parg));
    total_messages -= num_messages;
    p++;
    if (p > sizeof(pargs)/sizeof(pargs[0])) {
      break;
    }
  }
  assert(total_messages == 0);
  assert(p == thread_ids.size());

  // Join all the threads to finish production.
  int ret = 0;
  for (unsigned int i = 0; i < thread_ids.size(); ++i) {
    env->WaitForJoin(thread_ids[i]);
    if (!pargs[i].result) {
      if (FLAGS_report) {
        printf("Thread %d failed to send all messages\n", i);
      }
      ret = 1;
    }
  }

  if (FLAGS_await_ack) {
    // Wait for the all_ack_messages_received semaphore to be posted.
    // Keep waiting as long as a message was received in the last 5 seconds.
    auto timeout = std::chrono::seconds(idle_timeout);
    do {
      all_ack_messages_received->TimedWait(timeout);
    } while (ack_messages_received->load() != FLAGS_num_messages &&
             std::chrono::steady_clock::now() - *last_ack_message < timeout);

    ret = ack_messages_received->load() == FLAGS_num_messages ? 0 : 1;
  }
  args->result = ret; // 0: success, 1: error
}

/**
 * Subscribe to topics.
 */
void DoSubscribe(std::vector<std::unique_ptr<ClientImpl>>& consumers,
                 NamespaceID nsid,
                 std::unordered_map<std::string, SequenceNumber> first_seqno) {
  SequenceNumber start = 0;   // start sequence number (0 = only new records)

  // This needs to be low enough so that subscriptions are evenly distributed
  // among client threads.
  auto total_threads = consumers.size() * FLAGS_client_workers;
  unsigned int batch_size =
    std::min(100, std::max(1,
      static_cast<int>(FLAGS_num_topics / total_threads / 10)));

  std::vector<SubscriptionRequest> topics;
  topics.reserve(batch_size);

  // create all subscriptions from seqno 1
  SubscriptionRequest request(nsid, "", true, start);
  size_t c = 0;
  for (uint64_t i = 0; i < FLAGS_num_topics; i++) {
    request.topic_name.assign("benchmark." + std::to_string(i));
    if (FLAGS_delay_subscribe) {
      // Find the first seqno published to this topic (or 0 if none published).
      auto it = first_seqno.find(request.topic_name);
      if (it == first_seqno.end()) {
        request.start = 0;
      } else {
        request.start = it->second;
      }
    }

    topics.push_back(request);
    if (i % batch_size == batch_size - 1) {
      // send a subscription request
      consumers[c++ % consumers.size()]->ListenTopics(topics);
      topics.clear();
    }
  }

  // subscribe to all remaining topics
  if (topics.size() != 0) {
    consumers[c++ % consumers.size()]->ListenTopics(topics);
  }
}

/*
 ** Receive messages
 */
static void DoConsume(void* param) {
  ConsumerArgs* args = static_cast<ConsumerArgs*>(param);
  rocketspeed::port::Semaphore* all_messages_received =
    args->all_messages_received;
  std::atomic<int64_t>* messages_received = args->messages_received;
  std::chrono::time_point<std::chrono::steady_clock>* last_data_message =
    args->last_data_message;
  // Wait for the all_messages_received semaphore to be posted.
  // Keep waiting as long as a message was received in the last 5 seconds.
  auto timeout = std::chrono::seconds(idle_timeout);
  do {
    all_messages_received->TimedWait(timeout);
  } while (messages_received->load() != FLAGS_num_messages &&
           std::chrono::steady_clock::now() - *last_data_message < timeout);

  args->result = messages_received->load() == FLAGS_num_messages ? 0 : 1;
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  rocketspeed::Env::InstallSignalHandlers();
  rocketspeed::Env* env = rocketspeed::Env::Default();
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  // This loop is needed so that we can attach to this process via
  // the remote gdb debugger on Android systems.
  while (FLAGS_wait_for_debugger) {
    sleep(1);
  }

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

#if defined(OS_ANDROID)
  if (FLAGS_start_local_server) {
    fprintf(stderr, "Servers not supported on Android.\n");
    return 1;
  }
#else
  std::unique_ptr<rocketspeed::LocalTestCluster> test_cluster;
  if (FLAGS_start_local_server) {
    test_cluster.reset(new rocketspeed::LocalTestCluster(
                           info_log, true, true, true, FLAGS_storage_url));
  }
#endif

  // Configuration for RocketSpeed.
  std::vector<rocketspeed::HostId> pilots;
  for (auto hostname : rocketspeed::SplitString(FLAGS_pilot_hostnames)) {
    pilots.emplace_back(hostname, FLAGS_pilot_port);
  }

  std::vector<rocketspeed::HostId> copilots;
  for (auto hostname : rocketspeed::SplitString(FLAGS_copilot_hostnames)) {
    copilots.emplace_back(hostname, FLAGS_copilot_port);
  }

  // Start/end time for benchmark.
  std::chrono::time_point<std::chrono::steady_clock> start, end;

  // Semaphore to signal when all messages have been ack'd
  rocketspeed::port::Semaphore all_ack_messages_received;

  // Time last ack/data message was received.
  std::chrono::time_point<std::chrono::steady_clock> last_ack_message;
  std::chrono::time_point<std::chrono::steady_clock> last_data_message;

  // Benchmark statistics.
  rocketspeed::Statistics stats;
  rocketspeed::Histogram* ack_latency = stats.AddLatency("ack-latency");
  rocketspeed::Histogram* recv_latency = stats.AddLatency("recv-latency");

  // Create callback for publish acks.
  std::atomic<int64_t> ack_messages_received{0};
  std::atomic<int64_t> failed_publishes{0};

  // Map of topics to the first sequence number in that topic.
  std::unordered_map<std::string, rocketspeed::SequenceNumber> first_seqno;
  std::mutex first_seqno_mutex;

  auto publish_callback =
    [&] (std::unique_ptr<rocketspeed::ResultStatus> rs) {
    uint64_t now = env->NowMicros();

    // Parse message data to get received index.
    rocketspeed::Slice data = rs->GetContents();
    unsigned long long int message_index, send_time;
    std::sscanf(data.data(), "%llu %llu", &message_index, &send_time);
    ack_latency->Record(now - send_time);

    if (FLAGS_delay_subscribe) {
      if (rs->GetStatus().ok()) {
        // Get the minimum sequence number for this topic to subscribe to later.
        std::string topic = rs->GetTopicName().ToString();
        std::lock_guard<std::mutex> lock(first_seqno_mutex);
        auto it = first_seqno.find(topic);
        if (it == first_seqno.end()) {
          first_seqno[topic] = rs->GetSequenceNumber();
        } else {
          it->second = std::min(it->second, rs->GetSequenceNumber());
        }
      }
    }

    if (FLAGS_await_ack) {
      // This may be the last ack we receive, so set end to the time now.
      end = std::chrono::steady_clock::now();
      last_ack_message = std::chrono::steady_clock::now();

      // If we've received all messages, let the main thread know to finish up.
      if (++ack_messages_received == FLAGS_num_messages) {
        all_ack_messages_received.Post();
      }
    }

    if (!rs->GetStatus().ok()) {
      ++failed_publishes;
      LOG_WARN(info_log, "Received publish failure response");
    }
  };

  // Semaphore to signal when all data messages have been received
  rocketspeed::port::Semaphore all_messages_received;

  // Create callback for processing messages received
  std::atomic<int64_t> messages_received{0};
  std::vector<bool> is_received(FLAGS_num_messages, false);
  std::mutex is_received_mutex;
  auto receive_callback = [&]
    (std::unique_ptr<rocketspeed::MessageReceived> rs) {
    uint64_t now = env->NowMicros();
    ++messages_received;
    last_data_message = std::chrono::steady_clock::now();

    // Parse message data to get received index.
    rocketspeed::Slice data = rs->GetContents();
    unsigned long long int message_index, send_time;
    std::sscanf(data.data(), "%llu %llu", &message_index, &send_time);
    if (message_index < static_cast<uint64_t>(FLAGS_num_messages)) {
      LOG_INFO(info_log,
          "Received message %llu with timestamp %llu",
          static_cast<long long unsigned int>(message_index),
          static_cast<long long unsigned int>(send_time));
      recv_latency->Record(now - send_time);
      std::lock_guard<std::mutex> lock(is_received_mutex);
      if (is_received[message_index]) {
        LOG_WARN(info_log,
          "Received message %llu twice.",
          static_cast<long long unsigned int>(message_index));
      }
      is_received[message_index] = true;
    } else {
      LOG_WARN(info_log,
          "Received out of bounds message index (%llu), message was (%s)",
          static_cast<long long unsigned int>(message_index),
          data.data());
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


  std::vector<std::unique_ptr<rocketspeed::ClientImpl>> clients;
  size_t num_clients = std::max(pilots.size(), copilots.size());
  for (size_t i = 0; i < num_clients; ++i) {
    // Create config for this client by picking pilot and copilot in a round
    // robin fashion.
    std::unique_ptr<rocketspeed::Configuration> config(
        rocketspeed::Configuration::Create({pilots[i % pilots.size()]},
                                           {copilots[i % copilots.size()]},
                                           rocketspeed::Tenant(102),
                                           FLAGS_client_workers));
    // Configure the client.
    rocketspeed::ClientOptions options(
        *config, rocketspeed::GUIDGenerator().GenerateString());
    options.subscription_callback = subscribe_callback;
    options.info_log = info_log;
    std::unique_ptr<rocketspeed::ClientImpl> client;
    // Create the client.
    auto st = rocketspeed::ClientImpl::Create(std::move(options), &client);
    if (!st.ok()) {
      LOG_ERROR(info_log, "Failed to open client: %s.", st.ToString().c_str());
      return 1;
    }
    st = client->Start(receive_callback,
                       rocketspeed::Client::RestoreStrategy::kDontRestore);
    if (!st.ok()) {
      LOG_ERROR(info_log, "Failed to start client: %s.", st.ToString().c_str());
      return 1;
    }
    clients.emplace_back(client.release());
  }
  rocketspeed::NamespaceID nsid =
    static_cast<rocketspeed::NamespaceID>(FLAGS_namespaceid);

  // Subscribe to topics (don't count this as part of the time)
  // Also waits for the subscription responses.
  if (!FLAGS_delay_subscribe) {
    if (FLAGS_start_consumer) {
      printf("Subscribing to topics... ");
      fflush(stdout);
      DoSubscribe(clients, nsid, std::move(first_seqno));
      if (!all_topics_subscribed.TimedWait(std::chrono::seconds(5))) {
        printf("time out\n");
        LOG_WARN(info_log, "Failed to subscribe to all topics");
        info_log->Flush();
        printf("Failed to subscribe to all topics (%llu/%llu)\n",
          static_cast<long long unsigned int>(num_topics_subscribed.load()),
          static_cast<long long unsigned int>(FLAGS_num_topics));
        return 1;
      }
      printf("done\n");
    }

    // Start the clock.
    start = std::chrono::steady_clock::now();
  }

  ProducerArgs pargs;
  ConsumerArgs cargs;
  rocketspeed::Env::ThreadId producer_threadid = 0;
  rocketspeed::Env::ThreadId consumer_threadid = 0;

  // Start producing messages
  if (FLAGS_start_producer) {
    printf("Publishing messages.\n");
    pargs.producers = &clients;
    pargs.nsid = nsid;
    pargs.all_ack_messages_received = &all_ack_messages_received;
    pargs.ack_messages_received = &ack_messages_received;
    pargs.last_ack_message = &last_ack_message;
    pargs.publish_callback = publish_callback;
    producer_threadid = env->StartThread(rocketspeed::DoProduce,
                                         static_cast<void*>(&pargs),
                                         "ProducerMain");
  }

  // If we are not 'delayed', then we are already subscribed to
  // topics, simply starts threads to consume
  if (FLAGS_start_consumer && !FLAGS_delay_subscribe) {
    printf("Waiting for messages.\n");
    cargs.all_messages_received = &all_messages_received;
    cargs.messages_received = &messages_received;
    cargs.last_data_message = &last_data_message;
    consumer_threadid = env->StartThread(rocketspeed::DoConsume,
                                         &cargs,
                                         "ConsumerMain");
  }

  // Wait for all producers to finish
  int ret = 0;
  if (FLAGS_start_producer) {
    env->WaitForJoin(producer_threadid);
    ret = pargs.result;
    if (ack_messages_received.load() != FLAGS_num_messages) {
      printf("Time out awaiting publish acks.\n");
      ret = 1;
    } else {
      printf("All messages published.\n");
    }
  }

  // If we are delayed, then start subscriptions after all
  // publishers are completed.
  if (FLAGS_delay_subscribe) {
    assert(FLAGS_start_consumer);
    printf("Subscribing (delayed) to topics.\n");

    // Start the clock.
    start = std::chrono::steady_clock::now();

    // Subscribe to topics
    DoSubscribe(clients, nsid, std::move(first_seqno));

    // Wait for all messages to be received
    printf("Waiting (delayed) for messages.\n");
    cargs.all_messages_received = &all_messages_received;
    cargs.messages_received = &messages_received;
    cargs.last_data_message = &last_data_message;
    consumer_threadid = env->StartThread(rocketspeed::DoConsume,
                                         &cargs,
                                         "ConsumerMain");
  }

  if (FLAGS_start_consumer) {
    // Wait for Consumer thread to exit
    env->WaitForJoin(consumer_threadid);
    ret = cargs.result;
    if (messages_received.load() != FLAGS_num_messages) {
      printf("Time out awaiting messages.\n");
    } else {
      printf("All messages received.\n");
    }
  }

  end = std::chrono::steady_clock::now();

  // Calculate total time.
  auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
    end - start);

  if (FLAGS_report) {
    uint32_t total_ms = static_cast<uint32_t>(total_time.count());
    if (total_ms == 0) {
      // To avoid divide-by-zero on near-instant benchmarks.
      total_ms = 1;
    }

    uint64_t msg_per_sec = 1000 *
                           FLAGS_num_messages /
                           total_ms;
    uint64_t bytes_per_sec = 1000 *
                             FLAGS_num_messages *
                             FLAGS_message_size /
                             total_ms;

    printf("\n");
    printf("Results\n");
    printf("%lld messages sent\n",
           static_cast<long long unsigned int>(FLAGS_num_messages));
    printf("%lld messages sends acked\n",
           static_cast<long long unsigned int>(ack_messages_received.load()));
    if (failed_publishes != 0) {
      printf("%lld published failed\n",
             static_cast<long long unsigned int>(failed_publishes.load()));
    }
    if (FLAGS_start_consumer) {
      printf("%lld messages received\n",
             static_cast<long long unsigned int>(messages_received.load()));
    }

    if (FLAGS_start_consumer &&
        messages_received.load() != FLAGS_num_messages) {
      // Print out dropped messages if there are any. This helps when
      // debugging problems.
      printf("\n");
      printf("Messages failed to receive\n");

      for (uint64_t i = 0; i < is_received.size(); ++i) {
        if (!is_received[i]) {
          // Find the range of message IDs dropped (e.g. 100-200)
          uint64_t j;
          for (j = i; j < is_received.size(); ++j) {
            if (is_received[j]) {
              break;
            }
          }
          // Print the dropped messages, either individal (e.g. 100) or
          // as a range (e.g. 100-200)
          if (i == j - 1) {
            printf("%llu\n", static_cast<long long unsigned int>(i));
          } else {
            printf("%llu-%llu\n", static_cast<long long unsigned int>(i),
                   static_cast<long long unsigned int>(j - 1));
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
      printf("%" PRIu64 " messages/s\n", msg_per_sec);
      printf("%.2lf MB/s\n", bytes_per_sec * 1e-6);

#if !defined(OS_ANDROID)
      if (FLAGS_start_local_server) {
        stats.Aggregate(test_cluster->GetStatistics());
      }
#endif
      for (auto& client : clients) {
        stats.Aggregate(client->GetStatistics());
      }

      printf("\n");
      printf("Statistics\n");
      printf("%s", stats.Report().c_str());
    }
  }
  fflush(stdout);

  return ret;
}
