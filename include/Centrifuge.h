// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "RocketSpeed.h"
#include "Types.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif

/**
 * Centrifuge is a suite of stress testing utilities for testing the
 * RocketSpeed client and Rocketeer-based servers.
 *
 * src/tools/centrifuge is a tool that generates a number of centrifuge clients
 * to produce load against one or more servers, and perform basic integrity
 * checks. This header declares libraries for building the clients.
 *
 * Usage:
 *
 * A Centrifuge client primarily needs two things: RocketSpeed client options,
 * and a subscription generator. To produce a client binary, you must
 * provide these to RunCentrifugeClient.
 *
 * Example:
 *
 * #include "rocketspeed/include/Centrifuge.h"
 *
 * int main(int argc, char** argv) {
 *   using namespace rocketspeed;
 *   CentrifigeOptions opts;
 *   opts.client_options = ...;
 *   opts.generator = ...;
 *   return RunCentrifugeClient(std::move(opts), argc, argv);
 * }
 *
 * The resultant binary can then be given to the Centrifuge tool to spawn
 * client processes.
 */

namespace rocketspeed {

class SubscriptionGenerator;

/**
 * Configuration for the Centrifuge client.
 */
struct CentrifugeOptions {
  CentrifugeOptions();

  /// RocketSpeed client options for creating subscriptions.
  /// Users of Centrifuge should setup anything that is specific to their
  /// client (e.g. routing, threading), but note that Centrifuge may override
  /// some settings for different kinds of tests.
  ClientOptions client_options;

  /// Source of subscription scenarios for centrifuge clients.
  std::unique_ptr<SubscriptionGenerator> generator;
};

/**
 * This is the main entry point for centrifuge clients. Setup the options
 * to provide a RocketSpeed client and subscription generator and then invoke
 * this function to start the client. Does not return until the client has
 * finished running. This allows the client to do setup and teardown.
 *
 * @param options Options for centrifuge client.
 * @param argc From main - client may parse options from here.
 * @param argv From main - client may parse options from here.
 * @return 0 if succeeded, otherwise 1.
 */
int RunCentrifugeClient(CentrifugeOptions options, int argc, char** argv);

/**
 * The subscribe_rapid centrifuge client simply subscribes to some number of
 * topics as fast as it can, and then stops (without explicit unsubscribe).
 *
 * Flags for RunCentrifugeClient:
 * --mode=subscribe-rapid
 * --num_subscriptions
 * --ms_between_config_changes
 * --shard_failure_ratio
 */
struct SubscribeRapidOptions {
  SubscribeRapidOptions();
  std::unique_ptr<Client> client;
  std::unique_ptr<SubscriptionGenerator> generator;
  uint64_t num_subscriptions;
};
int SubscribeRapid(SubscribeRapidOptions options);

/**
 * The subscribe_unsubscribe_rapid centrifuge client rapidly subscribes and
 * then immediately unsubscribes on the same thread, with no delay between
 * the subscribe and unsubscribe.
 *
 * Flags for RunCentrifugeClient:
 * --mode=subscribe-unsubscribe-rapid
 * --num_subscriptions
 * --subscription_ttl
 * --ms_between_config_changes
 * --shard_failure_ratio
 */
struct SubscribeUnsubscribeRapidOptions {
  SubscribeUnsubscribeRapidOptions();
  std::unique_ptr<Client> client;
  std::unique_ptr<SubscriptionGenerator> generator;
  uint64_t num_subscriptions;
  std::chrono::milliseconds subscription_ttl;
};
int SubscribeUnsubscribeRapid(SubscribeUnsubscribeRapidOptions options);

/**
 * The slow_consumer centrifuge client rapidly subscribes to a number of topics
 * but with a consumer that sleeps inside OnMessageReceived. This should
 * eventually cause backpressure on the server.
 *
 * Flags for RunCentrifugeClient:
 * --mode=slow-consumer
 * --num_subscriptions
 * --receive_sleep_ms
 * --ms_between_config_changes
 * --shard_failure_ratio
 */
struct SlowConsumerOptions : public SubscribeRapidOptions {
  SlowConsumerOptions();
  std::unique_ptr<Client> client;
  std::unique_ptr<SubscriptionGenerator> generator;
  std::chrono::milliseconds receive_sleep_time;
};
int SlowConsumer(SlowConsumerOptions options);

/**
 * Test scenario for a single subscription. Contains the usual subscription
 * parameters and accepts an Observer that can be used for testing the expected
 * result of a scenario.
 */
class CentrifugeSubscription {
 public:
  CentrifugeSubscription(TenantID tenant_id,
                         NamespaceID namespace_id,
                         Topic topic,
                         SequenceNumber seqno = 0,
                         std::unique_ptr<Observer> _observer = nullptr);

  SubscriptionParameters params;
  std::unique_ptr<Observer> observer;
};

/**
 * Observer that expects an Invalid subscription status. Will fail if it
 * receives any updates, or a subscription status that is not Invalid.
 */
std::unique_ptr<Observer> ExpectInvalidObserver();

/**
 * Observer that should be valid. Will fail if it receives a subscription
 * status that is Invalid.
 */
std::unique_ptr<Observer> ExpectNotInvalidObserver();

/**
 * Observer that expects no updates. Will fail if it receives an update.
 */
std::unique_ptr<Observer> ExpectNoUpdatesObserver();

/**
 * Observer that extends another observer by adding a sleep after receiving
 * messages.
 *
 * @param observer The observer to extend.
 * @param sleep_time The time to sleep for after receiving a message.
 */
std::unique_ptr<Observer> SlowConsumerObserver(
    std::unique_ptr<Observer> observer = nullptr,
    std::chrono::milliseconds sleep_time = std::chrono::seconds(1));


/**
 * Interface for generating a sequence of random subscription
 * scenarios. These should try to cover as many edge cases as possible.
 */
class SubscriptionGenerator {
 public:
  virtual ~SubscriptionGenerator() {}

  /**
   * Returns a new subscription, or null if there are no more topics.
   * Centrifuge clients should stop subscribing when null has been reached.
   * Not required to be thread safe (will be called from one thread only).
   */
  virtual std::unique_ptr<CentrifugeSubscription> Next() = 0;
};

/**
 * Describes the state of the config manipulation by a volatile sharding
 * strategy.
 */
struct VolatileSetup {
  /// Function that returns time until next setup change.
  std::chrono::milliseconds duration{1};

  /// Function that returns the average ratio of shards to re-route to no host.
  double failure_rate{0.0};
};

/** Options for CreateVolatileShardingStrategy */
struct VolatileShardingOptions {
  VolatileShardingOptions();

  /// Function that returns the next config setup to use.
  /// Default: 10% failures for 5 seconds.
  std::function<VolatileSetup()> next_setup;
};

/**
 * An implementation of ShardingStrategy that wraps another ShardingStrategy
 * and invokes various volatile behaviours designed to test a clients
 * resilience to unstable tiers.
 */
std::shared_ptr<ShardingStrategy> CreateVolatileShardingStrategy(
    std::shared_ptr<ShardingStrategy> strategy,
    VolatileShardingOptions&& options);

/**
 * To be called when an invariant has been violated, but execution can
 * continue. The status will be printed to stderr.
 */
void CentrifugeError(Status st);

/**
 * To be called when an invariant has been violated, and execution cannot
 * continue (e.g. failed to create a client at the test start).
 * The status will be printed to stderr, and exit(1) called.
 */
void CentrifugeFatal(Status st);

}  // namespace rocketspeed

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
