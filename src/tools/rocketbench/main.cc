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
#include "src/util/common/fixed_configuration.h"
#include "src/util/common/guid_generator.h"
#include "src/util/common/host_id.h"
#include "src/util/common/parsing.h"
#include "src/tools/rocketbench/random_distribution.h"
#include "src/client/client.h"
#include "src/util/common/client_env.h"
#include "src/util/pacer.h"

/**
 * This tool can behave as a standalone producer, a standalone
 * consumer or both a producer and a consumer.
 */
DEFINE_bool(start_producer, true, "starts the producer");
DEFINE_bool(start_consumer, true, "starts the consumer");
DEFINE_bool(await_ack, true, "wait for and include acks in times");
DEFINE_bool(delay_subscribe, false, "start reading after publishing");
DEFINE_bool(subscriptionchurn, false, "start constant rate of "
  "subscriptions with random unsubscriptions");

/**
 * RocketBench can optionally start an instance of RocketSpeed locally.
 * It will talk to the LogDevice cluster specified by storage_url, and use
 * the cache_size specified.
 */
DEFINE_bool(start_local_server, false, "starts an embedded rocketspeed server");
DEFINE_string(storage_url, "", "Storage service URL for local server");
DEFINE_uint64(cache_size, 0, "size of cache in bytes");

/**
 * Configuration for the RocketSpeed machines to talk to.
 * If config is not provided then (co)pilot_{hostnames,port} are used.
 */
DEFINE_string(config,
              "",
              "Configuration string, if absent uses pilot and copilot lists");
DEFINE_string(pilot_hostnames, "localhost", "hostnames of pilots");
DEFINE_string(copilot_hostnames, "localhost", "hostnames of copilots");
DEFINE_int32(pilot_port, 58600, "port number of pilot");
DEFINE_int32(copilot_port, 58600, "port number of copilot");

DEFINE_int32(num_threads, 40, "number of threads");
DEFINE_uint64(client_workers, 32, "number of client workers");

/**
 * These parameters control the load generated.
 */
DEFINE_int32(message_size, 100, "message size (bytes)");
DEFINE_uint64(num_topics, 100, "number of topics");
DEFINE_int64(num_messages, 1000, "number of messages to send");
DEFINE_string(namespaceid, rocketspeed::GuestNamespace, "namespace id");
DEFINE_string(topics_distribution, "uniform",
"uniform, normal, poisson, fixed");
DEFINE_string(subscription_length_distribution, "weibull",
"distribution used for generating unsubscribe time in subscription churn");
DEFINE_string(subscription_backlog_distribution, "fixed",
"distribution used for generating subscription request with a backlog");
DEFINE_int64(topics_mean, 0,
"Mean for Normal and Poisson topic distributions (rounded to nearest int64)");
DEFINE_int64(topics_stddev, 0,
"Standard Deviation for Normal topic distribution (rounded to nearest int64)");
DEFINE_int64(subscription_backlog_mean, 0,
"Mean for Normal and Poisson topic distributions (rounded to nearest int64)");
DEFINE_int64(subscription_backlog_stddev, 0,
"Standard Deviation for Normal topic distribution (rounded to nearest int64)");
DEFINE_int64(subscribe_rate, 10, "subscribes per second ");
// shape > 1 implies negative aging . scale ~ 100 ensures the milliseconds
// are not too low or too huge
DEFINE_double(weibull_scale, 100.0, "scale of weibull distribution");
DEFINE_double(weibull_shape, 1.5, "scale of weibull distribution");
DEFINE_uint64(subscriptionchurn_max_time, 200, "max time of generated "
  "value from dist.");

/**
 * These parameters control the rate of publish requests sent to RocketSpeed.
 * They are complementary.
 * message_rate control the maximum number of publishes to send per second.
 * max_inflight controls the maximum number of unacknowledged publishes to have
 * "in-flight" at once.
 */
DEFINE_int64(message_rate, 100, "messages per second (0 = unlimited)");
DEFINE_uint64(max_inflight, 10000, "maximum publishes in flight");

/**
 * Miscellaneous parameters.
 */
DEFINE_bool(logging, true, "enable/disable logging");
DEFINE_bool(report, true, "report results to stdout");
DEFINE_int32(wait_for_debugger, 0, "wait for debugger to attach to me");
DEFINE_int32(idle_timeout, 5, "wait for X seconds until declaring timeout");
DEFINE_string(save_path, "./RocketBenchProducer.dat",
              "Name of file where producer stores per-topic information");
DEFINE_int64(progress_period, 10,
              "Number of milliseconds between updates to progress bar");

using namespace rocketspeed;

rocketspeed::Env* env;
std::shared_ptr<rocketspeed::Logger> info_log;

typedef std::pair<rocketspeed::MsgId, uint64_t> MsgTime;

struct Result {
  bool succeeded;
};

struct ProducerArgs {
  std::vector<std::unique_ptr<rocketspeed::ClientImpl>>* producers;
  rocketspeed::NamespaceID nsid;
  rocketspeed::port::Semaphore* all_ack_messages_received;
  rocketspeed::PublishCallback publish_callback;
  bool result;
};

struct ProducerWorkerArgs {
  rocketspeed::NamespaceID namespaceid;
  rocketspeed::Client* producer;
  rocketspeed::PublishCallback publish_callback;
  bool result;
  uint64_t seed;
  uint64_t max_inflight;
};

struct ConsumerArgs {
  rocketspeed::port::Semaphore* all_messages_received;
  std::atomic<int64_t>* messages_received;
  std::chrono::time_point<std::chrono::steady_clock>* last_data_message;
  bool result;
};

struct SubscriptionChurnArgs {
  rocketspeed::NamespaceID nsid;
  std::vector<std::unique_ptr<rocketspeed::ClientImpl>>* subscribers;
  rocketspeed::port::Semaphore* producer_thread_over;
};

struct TopicInfo {
  SequenceNumber first;
  SequenceNumber last;
  uint64_t total_num;
  TopicInfo(SequenceNumber f, SequenceNumber l,
            uint64_t count) : first(f), last(l), total_num(count) {
  }
  TopicInfo() : TopicInfo(0, 0, 0) {
  }
};

namespace rocketspeed {

struct SubscriptionChurnTimeout {
  bool is_subscribe;
  SubscriptionHandle sh;
  std::chrono::time_point<std::chrono::steady_clock> event_time;
  uint64_t client_number;

  SubscriptionChurnTimeout(
    bool is_sub,
    SubscriptionHandle handle,
    std::chrono::time_point<std::chrono::steady_clock> ev,
    uint64_t c_no
  ): is_subscribe{is_sub},
     sh{handle},
     event_time{ev},
     client_number{c_no}
    {}

    bool operator<(const SubscriptionChurnTimeout& rhs) const {
      return ((event_time) > (rhs.event_time));
  }
};

static void ProducerWorker(void* param) {
  ProducerWorkerArgs* args = static_cast<ProducerWorkerArgs*>(param);
  if (args->max_inflight == 0) {
    // Not allowed to send any messages, so immediately return.
    args->result = true;
    return;
  }
  args->result = false;

  NamespaceID namespaceid = args->namespaceid;
  Client* producer = args->producer;
  PublishCallback publish_callback = args->publish_callback;

  // Random number generator.
  std::unique_ptr<rocketspeed::RandomDistributionBase>
    distr(GetDistributionByName(FLAGS_topics_distribution,
                                0,
                                FLAGS_num_topics - 1,
                                static_cast<double>(FLAGS_topics_mean),
                                static_cast<double>(FLAGS_topics_stddev),
                                args->seed));

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
  int64_t rate = FLAGS_message_rate / FLAGS_num_threads + 1;
  auto pacer = std::make_shared<Pacer>(rate, args->max_inflight);
  uint64_t index = 0;
  static std::atomic<uint64_t> message_index{0};
  while (static_cast<int64_t>(index = message_index++) < FLAGS_num_messages) {
    // Create random topic name
    char topic_name[64];
    uint64_t topic_num = distr.get() != nullptr ?
                         distr->generateRandomInt() :
                         index % 100;  // "fixed", 100 messages per topic
    snprintf(topic_name, sizeof(topic_name),
             "benchmark.%llu",
             static_cast<long long unsigned int>(topic_num));

    TopicOptions topic_options;

    // Wait until we are allowed to send another message.
    pacer->Wait();

    // Add ID and timestamp to message ID.
    uint64_t send_time = env->NowMicros();
    snprintf(data.data(), data.size(),
             "%llu %llu",
             static_cast<long long unsigned int>(index),
             static_cast<long long unsigned int>(send_time));

    // Send the message
    PublishStatus ps = producer->Publish(
      GuestTenant,
      topic_name,
      namespaceid,
      topic_options,
      payload,
      [publish_callback, pacer] (std::unique_ptr<ResultStatus> rs) {
        // Allow another message now.
        publish_callback(std::move(rs));
        pacer->EndRequest();
      });

    if (!ps.status.ok()) {
      LOG_WARN(info_log,
        "Failed to send message number %llu (%s)",
        static_cast<long long unsigned int>(index),
        ps.status.ToString().c_str());
      info_log->Flush();
      args->result = false;
      pacer->EndRequest();
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
  rocketspeed::PublishCallback publish_callback = args->publish_callback;

  // Distribute total number of messages among them.
  std::vector<Env::ThreadId> thread_ids;
  size_t p = 0;
  ProducerWorkerArgs pargs[1024]; // no more than 1K threads
  assert(FLAGS_num_threads <= 1024);
  uint64_t max_inflight = FLAGS_max_inflight;

  for (int32_t remaining = FLAGS_num_threads; remaining; --remaining) {
    ProducerWorkerArgs* parg = &pargs[p];
    parg->namespaceid = namespaceid;
    parg->producer = (*producers)[p % producers->size()].get();
    parg->publish_callback = publish_callback;
    parg->seed = p << 32;  // should be consistent between runs.
    parg->max_inflight = max_inflight / remaining;

    thread_ids.push_back(env->StartThread(rocketspeed::ProducerWorker, parg));
    max_inflight -= parg->max_inflight;
    p++;
  }

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
    all_ack_messages_received->Wait();
  }
  args->result = ret; // 0: success, 1: error
}

/**
 * Subscribe to topics. If this instance of the test run did not
 * publish any messages, then start to subscribe all topics from
 * the start. Otherwise, start to subscribe from a random point
 * on messages in that topic.
 */
void DoSubscribe(std::vector<std::unique_ptr<ClientImpl>>& consumers,
                 NamespaceID nsid,
                 std::unordered_map<std::string, TopicInfo>& first_seqno) {
  size_t c = 0;
  Pacer pacer(FLAGS_subscribe_rate, 1);

  for (uint64_t i = 0; i < FLAGS_num_topics; i++) {
    std::string topic_name("benchmark." + std::to_string(i));
    SequenceNumber seqno = 0;   // start sequence number (0 = only new records)
    if (FLAGS_delay_subscribe) {
      // Find the first seqno published to this topic (or 0 if none published).
      auto it = first_seqno.find(topic_name);
      if (it == first_seqno.end()) {
        seqno = 0;
      } else if (it->second.first == 0 ||
                 it->second.first == it->second.last ||
                 FLAGS_subscription_backlog_distribution == "fixed") {
        seqno = it->second.first;
      } else {
        uint64_t seed = i << 32;  // should be consistent between runs.

        // backlog subscription point
        std::unique_ptr<RandomDistributionBase>
          distr(GetDistributionByName(FLAGS_subscription_backlog_distribution,
                it->second.first,
                it->second.last,
                static_cast<double>(FLAGS_subscription_backlog_mean),
                static_cast<double>(FLAGS_subscription_backlog_stddev),
                seed));
        seqno = distr->generateRandomInt();
        assert(seqno >= it->second.first);
        assert(seqno <= it->second.last);
      }
    }
    pacer.Wait();
    LOG_DEBUG(info_log, "Subscribing to %s at %llu",
              topic_name.c_str(),
              static_cast<long long unsigned int>(seqno));
    consumers[c++ % consumers.size()]->Subscribe(
        GuestTenant, nsid, topic_name, seqno);
    pacer.EndRequest();
  }
}

/*
 * Generate the sub and unsub time for subscription churn
 */
void PushSubUnsubTimeToQueue(
  std::unique_ptr<rocketspeed::RandomDistributionBase>& distr,
  std::priority_queue<SubscriptionChurnTimeout>& pq,
  int client_index,
  SubscriptionHandle sub_handle) {
  auto curtime = std::chrono::steady_clock::now();
  std::chrono::microseconds interval(1000000/FLAGS_subscribe_rate);
  uint64_t gen_number = 0;

  do {
    gen_number = distr->generateRandomInt();
  } while (gen_number > FLAGS_subscriptionchurn_max_time);

  auto gen_time = std::chrono::milliseconds(gen_number);
  auto sub_time(curtime + interval);
  auto unsub_time(curtime + gen_time);

  //push the next subscribe to the queue after interval time
  pq.push(SubscriptionChurnTimeout(true, 0, sub_time, client_index));
  //push the next unsubscribe to the queue after random time
  pq.push(SubscriptionChurnTimeout(false, sub_handle, unsub_time,
    client_index));
}

/**
* Subscription churn thread
*/
void DoSubscriptionChurn(void* params) {
  struct SubscriptionChurnArgs* args =
    static_cast<SubscriptionChurnArgs*>(params);

  std::vector<std::unique_ptr<ClientImpl>>* subscribers =
    args->subscribers;
  rocketspeed::NamespaceID nsid = args->nsid;
  rocketspeed::port::Semaphore* producer_thread_over =
    args->producer_thread_over;

  // the priority queue is to store all sub and unsub events
  // earliest event is at the top
  std::priority_queue<SubscriptionChurnTimeout> pq;
  int seq = 0;
  int client_index = 0;
  SubscriptionHandle sub_handle;
  auto curtime = std::chrono::steady_clock::now();
  const uint64_t seed = 279470273; // using constant seed for comparable result
                                   // across benchmark runs

  // seed and generate a random number distribution , currently using:
  // en.cppreference.com/w/cpp/numeric/random/weibull_distribution
  std::unique_ptr<rocketspeed::RandomDistributionBase>
    distr(GetDistributionByName(FLAGS_subscription_length_distribution,
                                0,
                                0,
                                static_cast<double>(FLAGS_weibull_shape),
                                static_cast<double>(FLAGS_weibull_scale),
                                seed));

  // topic_distr is to generate a random topic number
  std::unique_ptr<rocketspeed::RandomDistributionBase>
    topic_distr(GetDistributionByName(FLAGS_topics_distribution,
                                0,
                                FLAGS_num_topics - 1,
                                static_cast<double>(FLAGS_topics_mean),
                                static_cast<double>(FLAGS_topics_stddev),
                                seed));

  /*
   * subscribes client 0 initally to topic 0,
   * and pushes the initial sub and unsub time to pq
   */
  sub_handle = (*subscribers)[0]->Subscribe(GuestTenant, nsid, "benchmark.0",
    seq);
  PushSubUnsubTimeToQueue(distr, pq, 0,sub_handle);
  do {
    curtime = std::chrono::steady_clock::now();
    // top of the q should contain oldest event
    auto w = pq.top();
    /*
     * there are 2 types of events : subscribe and unsubscribe . If
     * subscribe then push the next subscribe in the queue after an interval
     * and generate the unsub time for this sub , pushing the handle in the q
     * If its a unsubscribe event , just unsubscribe
     */
    if (w.is_subscribe) {
      // if event is subscribe : subscribe and generate the unsub time
      // use the difference to calculate all intermediate sub events

      //generate a random topic to subscribe to
      char topic_name[64];
      uint64_t topic_num = topic_distr.get() != nullptr ?
                           topic_distr->generateRandomInt() :
                           client_index % 100;
      snprintf(topic_name, sizeof(topic_name),
              "benchmark.%llu",
              static_cast<long long unsigned int>(topic_num));
      sub_handle = (*subscribers)[w.client_number]->
        Subscribe(GuestTenant, nsid, topic_name, seq);
      PushSubUnsubTimeToQueue(distr,
                              pq,
                              (int)(client_index++ % (subscribers->size())),
                              sub_handle);
    } else {
      //if its unsubscribe : just unsubscribe, no other action needed
      (*subscribers)[w.client_number]->Unsubscribe((w.sh));
    }
    pq.pop();
    assert(!pq.empty());
  } while (!producer_thread_over->TimedWait(
      std::chrono::duration_cast<std::chrono::microseconds>(
          pq.top().event_time - curtime)));
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
  auto timeout = std::chrono::seconds(FLAGS_idle_timeout);
  do {
    all_messages_received->TimedWait(timeout);
  } while (messages_received->load() != FLAGS_num_messages &&
           std::chrono::steady_clock::now() - *last_data_message < timeout);

  args->result = messages_received->load() == FLAGS_num_messages ? 0 : 1;
}

/*
 * Save topicmap to a file on disk. Returns 0 on success, errno on error.
 * Each line of the file is of the form
 *   topic_name<tab>first_seqno<tab>last_seqno<tab>number_of_messages</n>
 */
static int SaveFile(std::string filename,
                    std::unordered_map<std::string, TopicInfo>& tinfo) {
  FILE* fh = fopen((const char*)filename.c_str(), "w");
  if (fh == nullptr) {
    return errno;
  }
  for (auto it = tinfo.begin(); it != tinfo.end(); ++it ) {
    fprintf(fh, "topic=%s\tfirst=%" PRIu64 "\tlast=%" PRIu64
            "\ttotal=%" PRIu64 "\n",
            it->first.c_str(),
            it->second.first,
            it->second.last,
            it->second.total_num);
  }
  if (fclose(fh)) {
    return errno;
  }
  return 0; // success
}
/*
 * Read topicmap to a file on disk. Returns 0 on success, errno on error.
 * Each line of the file is of the form
 *   topic_name<tab>first_seqno<tab>last_seqno<tab>number_of_messages</n>
 */
static int ReadFile(std::string filename,
                    std::unordered_map<std::string, TopicInfo>& tinfo) {
  char buffer[10240];
  SequenceNumber num1, num2;
  uint64_t total_num;
  FILE* fh = fopen(filename.c_str(), "r");
  if (fh == nullptr) {
    return errno;
  }
  while (true) {
    int val = fscanf(fh,
                     "topic=%s\tfirst=%" PRIu64 "\tlast=%" PRIu64
                     "\ttotal=%" PRIu64 "\n",
                     buffer,
                     &num1,
                     &num2,
                     &total_num);
    if (val == EOF) {
      break;
    }
    if (val != 4) {
      return -1;
    }
    tinfo[buffer] =  TopicInfo(num1, num2, total_num);
  }
  if (fclose(fh)) {
    return errno;
  }
  return 0; // success
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  rocketspeed::Env::InstallSignalHandlers();
  env = rocketspeed::Env::Default();
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
  LocalTestCluster::Options test_options;
  if (FLAGS_start_local_server) {
    test_options.info_log = info_log;
    test_options.start_controltower = true;
    test_options.start_copilot = true;
    test_options.start_pilot = true;
    test_options.storage_url = FLAGS_storage_url;
    if (FLAGS_cache_size) {
      test_options.tower.cache_size = FLAGS_cache_size;
    }
    test_cluster.reset(new rocketspeed::LocalTestCluster(test_options));
  }
#endif

  // Configuration for RocketSpeed.
  std::vector<rocketspeed::HostId> pilots;
  for (auto hostname : rocketspeed::SplitString(FLAGS_pilot_hostnames)) {
    HostId resolved;
    auto st = HostId::Resolve(
        hostname, static_cast<uint16_t>(FLAGS_pilot_port), &resolved);
    if (!st.ok()) {
      LOG_ERROR(info_log, "%s", st.ToString().c_str());
      return 1;
    }
    pilots.emplace_back(std::move(resolved));
  }

  std::vector<rocketspeed::HostId> copilots;
  for (auto hostname : rocketspeed::SplitString(FLAGS_copilot_hostnames)) {
    HostId resolved;
    auto st = HostId::Resolve(
        hostname, static_cast<uint16_t>(FLAGS_copilot_port), &resolved);
    if (!st.ok()) {
      LOG_ERROR(info_log, "%s", st.ToString().c_str());
      return 1;
    }
    copilots.emplace_back(std::move(resolved));
  }

  // Start/end time for benchmark.
  std::chrono::time_point<std::chrono::steady_clock> start, end;

  // Semaphore to signal when all messages have been ack'd
  rocketspeed::port::Semaphore all_ack_messages_received;

  // Semaphore to signal subscriptionchurn thread when producer is over
  rocketspeed::port::Semaphore producer_thread_over;

  // Time last data message was received.
  std::chrono::time_point<std::chrono::steady_clock> last_data_message;

  // Benchmark statistics.
  std::mutex all_stats_mutex;
  std::vector<std::unique_ptr<rocketspeed::Statistics>> all_stats;
  rocketspeed::ThreadLocalPtr per_thread_stats;
  rocketspeed::ThreadLocalPtr ack_latency;
  rocketspeed::ThreadLocalPtr recv_latency;

  // Do not show receive latency if we delayed subscription.
  const bool show_recv_latency = !FLAGS_delay_subscribe;

  // Initializes stats for current thread.
  auto InitThreadLocalStats = [&] () {
    if (!per_thread_stats.Get()) {
      std::unique_ptr<rocketspeed::Statistics> stats(
        new rocketspeed::Statistics());
      per_thread_stats.Reset(stats.get());
      ack_latency.Reset(stats->AddLatency("ack-latency"));
      if (show_recv_latency) {
        recv_latency.Reset(stats->AddLatency("recv-latency"));
      }
      std::lock_guard<std::mutex> lock(all_stats_mutex);
      all_stats.emplace_back(std::move(stats));
    }
  };

  // Get thread local ack latency histogram.
  auto GetAckLatency = [&] () {
    InitThreadLocalStats();
    return static_cast<rocketspeed::Histogram*>(ack_latency.Get());
  };

  // Get thread local recv latency histogram.
  auto GetRecvLatency = [&] () {
    assert(show_recv_latency);
    InitThreadLocalStats();
    return static_cast<rocketspeed::Histogram*>(recv_latency.Get());
  };

  // Create callback for publish acks.
  std::atomic<int64_t> ack_messages_received{0};
  std::atomic<int64_t> failed_publishes{0};

  // Map of topics to the (first, last, total#msg) in that topic.
  std::unordered_map<std::string, TopicInfo> first_seqno;
  std::mutex first_seqno_mutex;

  auto publish_callback =
    [&] (std::unique_ptr<rocketspeed::ResultStatus> rs) {
    uint64_t now = env->NowMicros();

    if (rs->GetStatus().ok()) {
      // Parse message data to get received index.
      rocketspeed::Slice data = rs->GetContents();
      unsigned long long int message_index, send_time;
      std::sscanf(data.data(), "%llu %llu", &message_index, &send_time);
      GetAckLatency()->Record(static_cast<uint64_t>(now - send_time));

      if (FLAGS_delay_subscribe || !FLAGS_save_path.empty()) {
        if (rs->GetStatus().ok()) {
          // Get the min sequence number for this topic to subscribe to later.
          std::string topic = rs->GetTopicName().ToString();
          SequenceNumber seq = rs->GetSequenceNumber();
          std::lock_guard<std::mutex> lock(first_seqno_mutex);
          auto it = first_seqno.find(topic);
          if (it == first_seqno.end()) {
            first_seqno[topic] = TopicInfo(seq, seq, 1);
          } else {
            it->second.first = std::min(it->second.first, seq);
            it->second.last = std::max(it->second.last, seq);
          }
        }
      }
    } else {
      ++failed_publishes;
      LOG_WARN(info_log, "Received publish failure response");
    }

    if (FLAGS_await_ack) {
      // This may be the last ack we receive, so set end to the time now.
      end = std::chrono::steady_clock::now();

      // If we've received all messages, let the main thread know to finish up.
      if (++ack_messages_received == FLAGS_num_messages) {
        all_ack_messages_received.Post();
      }
    }
  };

  // Semaphore to signal when all data messages have been received
  rocketspeed::port::Semaphore all_messages_received;

  // Create callback for processing messages received
  std::atomic<int64_t> messages_received{0};
  std::vector<bool> is_received(FLAGS_num_messages, false);
  std::mutex is_received_mutex;

  auto receive_callback = [&]
    (std::unique_ptr<rocketspeed::MessageReceived>& rs) {
    uint64_t now = env->NowMicros();
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
      if (show_recv_latency) {
        GetRecvLatency()->Record(static_cast<uint64_t>(now - send_time));
      }
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
    if (++messages_received == FLAGS_num_messages) {
      all_messages_received.Post();
    }
  };

  // Subscribe callback.
  std::atomic<uint64_t> num_topics_subscribed{0};
  auto subscribe_callback = [&](const rocketspeed::SubscriptionStatus& ss) {
    if (!ss.IsSubscribed()) {
      LOG_WARN(info_log, "Received an unsubscribe response");
    }
  };

  // Data loss callback.
  auto data_loss_callback = [&](std::unique_ptr<DataLossInfo>& msg) {
    LOG_ERROR(info_log, "Data loss has been detected.");
  };

  std::vector<std::unique_ptr<rocketspeed::ClientImpl>> clients;
  for (size_t i = 0; i < FLAGS_client_workers; ++i) {
    rocketspeed::ClientOptions options;
    options.info_log = info_log;
    options.num_workers = 1;

    if (!FLAGS_config.empty()) {
      // Use provided configuration string.
      auto st = rocketspeed::Configuration::CreateConfiguration(
          info_log, FLAGS_config, &options.config);
      if (!st.ok()) {
        LOG_FATAL(info_log,
                  "Failed to parse configuration: %s",
                  st.ToString().c_str());
        return 1;
      }
    } else {
      // Fall back to picking pilot and copilot in a round robin fashion.
      options.config = std::make_shared<rocketspeed::FixedConfiguration>(
          pilots[i % pilots.size()], copilots[i % copilots.size()]);
    }

    std::unique_ptr<rocketspeed::ClientImpl> client;
    // Create the client.
    auto st = rocketspeed::ClientImpl::Create(std::move(options), &client);
    if (!st.ok()) {
      LOG_ERROR(info_log, "Failed to open client: %s.", st.ToString().c_str());
      return 1;
    }
    client->SetDefaultCallbacks(subscribe_callback,
                                receive_callback,
                                data_loss_callback);
    clients.emplace_back(client.release());
  }
  rocketspeed::NamespaceID nsid =
    static_cast<rocketspeed::NamespaceID>(FLAGS_namespaceid);

  // Subscribe to topics (don't count this as part of the time)
  // Also waits for the subscription responses.
  if (!FLAGS_delay_subscribe && !FLAGS_subscriptionchurn) {
    if (FLAGS_start_consumer) {
      printf("Subscribing to topics... ");
      fflush(stdout);
      DoSubscribe(clients, nsid, first_seqno);
      env->SleepForMicroseconds(1000000);  // allow 1 seconds for subscribe
      printf("done\n");
    }

    // Start the clock.
    start = std::chrono::steady_clock::now();
  }

  ProducerArgs pargs;
  ConsumerArgs cargs;
  SubscriptionChurnArgs scargs;
  rocketspeed::Env::ThreadId producer_threadid = 0;
  rocketspeed::Env::ThreadId consumer_threadid = 0;
  rocketspeed::Env::ThreadId subscriptionchurn_threadid = 0;

  port::Semaphore progress_stop;
  auto progress_thread = env->StartThread(
    [&] () {
      while (!progress_stop.TimedWait(std::chrono::milliseconds
                                      (FLAGS_progress_period))) {
        const uint64_t pubacks = ack_messages_received.load();
        const uint64_t received = messages_received.load();
        const uint64_t failed = failed_publishes.load();
        printf("publish-ack'd: %" PRIu64 "/%" PRIu64 " (%.1lf%%)"
             "  received: %" PRIu64 "/%" PRIu64 " (%.1lf%%)"
             "  failed: %" PRIu64
             "\r",
          pubacks,
          FLAGS_num_messages,
          100.0 * static_cast<double>(pubacks) /
            static_cast<double>(FLAGS_num_messages),
          received,
          FLAGS_num_messages,
          100.0 * static_cast<double>(received) /
            static_cast<double>(FLAGS_num_messages),
          failed);
        fflush(stdout);
      }
    },
    "progress");

  // Start producing messages
  if (FLAGS_start_producer) {
    printf("Publishing messages.\n");
    fflush(stdout);
    pargs.producers = &clients;
    pargs.nsid = nsid;
    pargs.all_ack_messages_received = &all_ack_messages_received;
    pargs.publish_callback = publish_callback;
    producer_threadid = env->StartThread(rocketspeed::DoProduce,
                                         static_cast<void*>(&pargs),
                                         "ProducerMain");
  }

  // If we are not 'delayed', then we are already subscribed to
  // topics, simply starts threads to consume
  if (FLAGS_start_consumer && !FLAGS_delay_subscribe) {
    if (FLAGS_subscriptionchurn) {
      printf("Starting the subscription churn .\n");
      fflush(stdout);

      // Start the clock.
      start = std::chrono::steady_clock::now();

      fflush(stdout);
      scargs.producer_thread_over = &producer_thread_over;
      scargs.subscribers = &clients;
      scargs.nsid = nsid;
      subscriptionchurn_threadid = env->StartThread(
                                     rocketspeed::DoSubscriptionChurn,
                                     &scargs,
                                     "Subscription Churn");
    } else { // regular case
      printf("Waiting for messages.\n");
      fflush(stdout);
      cargs.all_messages_received = &all_messages_received;
      cargs.messages_received = &messages_received;
      cargs.last_data_message = &last_data_message;
      consumer_threadid = env->StartThread(rocketspeed::DoConsume,
        &cargs,
        "ConsumerMain");
      }
    }

  // Wait for all producers to finish
  int ret = 0;
  if (FLAGS_start_producer) {
    env->WaitForJoin(producer_threadid);
    ret = pargs.result;
    if (ack_messages_received.load() != FLAGS_num_messages) {
      printf("Time out awaiting publish acks.\n");
      fflush(stdout);
      ret = 1;
    } else {
      printf("All messages published.\n");
      fflush(stdout);
    }
  }

  if (FLAGS_subscriptionchurn) {
    printf("Publisher over. Signalling subscription churn to stop. \n");
    producer_thread_over.Post();
    env->WaitForJoin(subscriptionchurn_threadid);
  }

  // If we are delayed, then start subscriptions after all
  // publishers are completed.
  uint64_t subscribe_time = 0;
  if (FLAGS_delay_subscribe) {

    // If we did not produce any message in this current run but have
    // saved topic-metadata in a file in some previous run, then
    // use that topic-metadata to start subscriptions.
    if (!FLAGS_start_producer) {
      int val = ReadFile(FLAGS_save_path, first_seqno);
      if (val == 0) {
        printf("Restored topic metadata from file %s\n",
               FLAGS_save_path.c_str());
      } else {
        printf("Error (%d) in reading topic metadat from file %s\n",
               val, FLAGS_save_path.c_str());
      }
    }
    assert(FLAGS_start_consumer);
    printf("Subscribing (delayed) to topics.\n");
    fflush(stdout);

    // Start the clock.
    start = std::chrono::steady_clock::now();

    // Subscribe to topics
    subscribe_time = env->NowMicros();
    DoSubscribe(clients, nsid, first_seqno);
    subscribe_time = env->NowMicros() - subscribe_time;
    printf("Took %" PRIu64 "ms to subscribe to %" PRIu64 " topics\n",
      subscribe_time / 1000,
      FLAGS_num_topics);

    // Wait for all messages to be received
    printf("Waiting (delayed) for messages.\n");
    fflush(stdout);
    cargs.all_messages_received = &all_messages_received;
    cargs.messages_received = &messages_received;
    cargs.last_data_message = &last_data_message;
    consumer_threadid = env->StartThread(rocketspeed::DoConsume,
                                         &cargs,
                                         "ConsumerMain");
  }

  if (FLAGS_start_consumer && !FLAGS_subscriptionchurn) {
    // Wait for Consumer thread to exit
    env->WaitForJoin(consumer_threadid);
    ret = cargs.result;
    if (messages_received.load() != FLAGS_num_messages) {
      printf("Time out awaiting messages.\n");
      fflush(stdout);
    } else {
      printf("All messages received.\n");
      fflush(stdout);
    }
  }

  end = std::chrono::steady_clock::now();

  progress_stop.Post();
  env->WaitForJoin(progress_thread);

  // Calculate total time.
  auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
    end - start);

  if (FLAGS_report) {
    uint32_t total_ms = static_cast<uint32_t>(total_time.count());
    if (total_ms == 0) {
      // To avoid divide-by-zero on near-instant benchmarks.
      total_ms = 1;
    }

    if (FLAGS_delay_subscribe) {
      // Check that subscribe time wasn't a significant portion of total time.
      uint32_t subscribe_ms = static_cast<uint32_t>(subscribe_time / 1000);
      double subscribe_pct = double(subscribe_ms) / double(total_ms);
      if (subscribe_pct > 0.01) {
        printf(
          "\n"
          "WARNING: Time waiting for subscription was %.2lf%% of total time.\n"
          "         Consider subscribing to fewer topics.\n",
          100.0 * subscribe_pct);
      }
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
        messages_received.load() != FLAGS_num_messages &&
        !FLAGS_subscriptionchurn &&
        FLAGS_subscription_backlog_distribution == "fixed") {
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
      printf("%.2lf MB/s\n", static_cast<double>(bytes_per_sec) * 1e-6);

      rocketspeed::Statistics stats;

      // Aggregate per-thread stats.
      std::lock_guard<std::mutex> lock(all_stats_mutex);
      for (auto& s : all_stats) {
        stats.Aggregate(s->MoveThread());
      }

#if !defined(OS_ANDROID)
      if (FLAGS_start_local_server) {
        stats.Aggregate(test_cluster->GetStatisticsSync());
      }
#endif
      for (auto& client : clients) {
        stats.Aggregate(client->GetStatisticsSync());
      }

      printf("\n");
      printf("Statistics\n");
      printf("%s", stats.Report().c_str());
    }

    // Save metadata about the published topics into a file
    if (FLAGS_start_producer && !FLAGS_save_path.empty()) {
      int val = SaveFile(FLAGS_save_path, first_seqno);
      if (val == 0) {
        printf("Saved topic metadata into file %s\n", FLAGS_save_path.c_str());
      } else {
        printf("Error (%d) in saving topic metadat into file %s\n",
               val, FLAGS_save_path.c_str());
        ret = val;
      }
    }
  }
  fflush(stdout);

  return ret;
}
