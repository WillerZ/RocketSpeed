// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Centrifuge.h"
#include "include/Env.h"
#include "src/tools/centrifuge/centrifuge.h"
#include "src/util/timeout_list.h"
#include "src/util/pacer.h"
#include <gflags/gflags.h>
#include <thread>
#include <cstdlib>

namespace rocketspeed {

/** Flags used by the generic RunCentrifugeClient runner. */
DEFINE_string(mode, "", "Which behaviour to use. Options are "
  "subscribe-rapid,"
  "subscribe-unsubscribe-rapid,"
  "slow-consumer");

DEFINE_uint64(num_subscriptions, 1000000,
  "Number of times to subscribe");

DEFINE_uint64(subscribe_rate, 10000,
  "Maximum subscribes per second");

DEFINE_uint64(num_bursts, 10,
  "Number of subscription bursts (num_subscriptions is divided among bursts)");

DEFINE_uint64(ms_between_bursts, 5000,
  "Milliseconds between subscription bursts");

DEFINE_uint64(receive_sleep_ms, 1000,
  "Milliseconds to sleep on receiving a message");

DEFINE_uint64(subscription_ttl, 100,
  "Milliseconds before unsubscribing");

DEFINE_uint64(ms_between_config_changes, 5000,
  "Milliseconds between config changes (0 to disable)");

DEFINE_double(shard_failure_ratio, 0,
  "Ratio of shards to fail on config change (e.g. 0.1 == 10%%)");


namespace {
/** Sets the client and generator for a specicific behavior's options. */
template <typename BehaviorOptions>
void SetupGeneralOptions(CentrifugeOptions& general_options,
                         BehaviorOptions& behavior_options) {
  // Setup volatile sharding.
  using namespace std::chrono;
  if (FLAGS_ms_between_config_changes) {
    VolatileShardingOptions volatile_sharding_options;
    volatile_sharding_options.next_setup = []() {
      VolatileSetup setup;
      setup.duration = milliseconds(FLAGS_ms_between_config_changes);
      setup.failure_rate = FLAGS_shard_failure_ratio;
      return setup;
    };
    auto sharding = std::move(general_options.client_options.sharding);
    general_options.client_options.sharding =
        CreateVolatileShardingStrategy(std::move(sharding),
                                       std::move(volatile_sharding_options));
  }

  // Create client.
  auto st = Client::Create(std::move(general_options.client_options),
                           &behavior_options.client);
  if (!st.ok()) {
    CentrifugeFatal(st);
  }

  // Setup centrifuge options.
  behavior_options.generator = std::move(general_options.generator);
}
}

int RunCentrifugeClient(CentrifugeOptions options, int argc, char** argv) {
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  auto env = Env::Default();
  Status st = env->StdErrLogger(&centrifuge_logger);
  if (!st.ok()) {
    return 1;
  }

  int result = 0;
  if (FLAGS_mode == "subscribe-rapid") {
    SubscribeRapidOptions opts;
    SetupGeneralOptions(options, opts);
    opts.num_subscriptions = FLAGS_num_subscriptions;
    opts.subscribe_rate = FLAGS_subscribe_rate;
    result = SubscribeRapid(std::move(opts));
  } else if (FLAGS_mode == "subscribe-burst") {
    SubscribeBurstOptions opts;
    SetupGeneralOptions(options, opts);
    opts.num_subscriptions = FLAGS_num_subscriptions;
    opts.num_bursts = FLAGS_num_bursts;
    opts.between_bursts = std::chrono::milliseconds(FLAGS_ms_between_bursts);
    result = SubscribeBurst(std::move(opts));
  } else if (FLAGS_mode == "subscribe-unsubscribe-rapid") {
    SubscribeUnsubscribeRapidOptions opts;
    SetupGeneralOptions(options, opts);
    opts.num_subscriptions = FLAGS_num_subscriptions;
    opts.subscribe_rate = FLAGS_subscribe_rate;
    opts.subscription_ttl = std::chrono::milliseconds(FLAGS_subscription_ttl);
    result = SubscribeUnsubscribeRapid(std::move(opts));
  } else if (FLAGS_mode == "slow-consumer") {
    SlowConsumerOptions opts;
    SetupGeneralOptions(options, opts);
    opts.num_subscriptions = FLAGS_num_subscriptions;
    opts.subscribe_rate = FLAGS_subscribe_rate;
    opts.receive_sleep_time = std::chrono::milliseconds(FLAGS_receive_sleep_ms);
    result = SlowConsumer(std::move(opts));
  } else {
    CentrifugeFatal(Status::InvalidArgument("Unknown mode flag"));
    return 1;
  }

  if (!result) {
    fprintf(stderr, "Centrifuge completed successfully.\n");
    fflush(stderr);
  }
  return result;
}

SubscriptionHandle SubscribeWithRetries(
    Client* client,
    const SubscriptionParameters& params,
    std::unique_ptr<Observer>& observer) {
  auto start = std::chrono::steady_clock::now();
  SubscriptionHandle handle = 0;
  while (!(handle = client->Subscribe(params, observer))) {
    // Wait for backpressure to be lifted.
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(10)) {
      CentrifugeFatal(Status::TimedOut("Unable to subscribe for 10 seconds"));
      return 0;
    }
  }
  return handle;
}

SubscribeRapidOptions::SubscribeRapidOptions()
: num_subscriptions(10000000)
, subscribe_rate(10000) {}

int SubscribeRapid(SubscribeRapidOptions options) {
  auto num_subscriptions = options.num_subscriptions;
  std::unique_ptr<CentrifugeSubscription> sub;
  Pacer pacer(options.subscribe_rate, 1);
  while (num_subscriptions-- && (sub = options.generator->Next())) {
    pacer.Wait();
    pacer.EndRequest();
    SubscribeWithRetries(options.client.get(), sub->params, sub->observer);
  }
  return 0;
}

SubscribeBurstOptions::SubscribeBurstOptions()
: num_subscriptions(10000000)
, num_bursts(10)
, between_bursts(5000) {}

int SubscribeBurst(SubscribeBurstOptions options) {
  auto num_subscriptions = options.num_subscriptions;
  auto num_bursts = options.num_bursts;
  std::unique_ptr<CentrifugeSubscription> sub;
  while (num_bursts) {
    auto subs_this_burst = num_subscriptions / num_bursts;
    --num_bursts;
    num_subscriptions -= subs_this_burst;
    while (subs_this_burst-- && (sub = options.generator->Next())) {
      SubscribeWithRetries(options.client.get(), sub->params, sub->observer);
    }
    /* sleep override */
    std::this_thread::sleep_for(options.between_bursts);
  }
  return 0;
}

SubscribeUnsubscribeRapidOptions::SubscribeUnsubscribeRapidOptions()
: num_subscriptions(1000000)
, subscribe_rate(10000) {}

int SubscribeUnsubscribeRapid(SubscribeUnsubscribeRapidOptions options) {
  auto num_subscriptions = options.num_subscriptions;
  auto subscription_ttl = options.subscription_ttl;
  TimeoutList<SubscriptionHandle> handles;
  std::unique_ptr<CentrifugeSubscription> sub;
  Pacer pacer(options.subscribe_rate, 1);
  while (num_subscriptions-- && (sub = options.generator->Next())) {
    pacer.Wait();
    pacer.EndRequest();
    SubscriptionHandle handle =
      SubscribeWithRetries(options.client.get(), sub->params, sub->observer);
    handles.Add(handle);
    handles.ProcessExpired(
      subscription_ttl,
      [&] (SubscriptionHandle to_unsubscribe) {
        options.client->Unsubscribe(to_unsubscribe);
      },
      -1 /* no limit to number unsubscribed at once */);
  }
  return 0;
}

namespace {
// Transforms a SubscriptionGenerator to slow down message receipt.
class SlowConsumerGenerator : public SubscriptionGenerator {
 public:
  SlowConsumerGenerator(std::unique_ptr<SubscriptionGenerator> gen,
                        std::chrono::milliseconds receive_sleep_time)
  : gen_(std::move(gen))
  , receive_sleep_time_(receive_sleep_time) {}

  std::unique_ptr<CentrifugeSubscription> Next() override {
    auto sub = gen_->Next();
    if (sub) {
      auto obs = std::move(sub->observer);
      sub->observer = SlowConsumerObserver(std::move(obs), receive_sleep_time_);
    }
    return sub;
  }

 private:
  std::unique_ptr<SubscriptionGenerator> gen_;
  const std::chrono::milliseconds receive_sleep_time_;
};
}

SlowConsumerOptions::SlowConsumerOptions()
: receive_sleep_time(1000) {}

int SlowConsumer(SlowConsumerOptions options) {
  auto gen = std::move(options.generator);
  options.generator.reset(
    new SlowConsumerGenerator(std::move(gen), options.receive_sleep_time));
  return SubscribeRapid(std::move(options));
}

}  // namespace rocketspeed
