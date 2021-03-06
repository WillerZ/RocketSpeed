// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Centrifuge.h"
#include "include/Env.h"
#include "include/HostId.h"
#include "include/RocketSpeed.h"
#include <gflags/gflags.h>
#include <memory>

DEFINE_string(config,
    "one-host;pilot=localhost:5834;copilot=localhost:5834;shards=1024",
    "Server configuration");
DEFINE_string(log_level, "info", "Logging level");

using namespace rocketspeed;

namespace {
class Generator : public SubscriptionGenerator {
 public:
  std::unique_ptr<CentrifugeSubscription> Next() override {
    auto topic = std::to_string(topic_++);
    std::unique_ptr<CentrifugeSubscription> sub(
      new CentrifugeSubscription(GuestTenant, GuestNamespace, topic, 0));
    return sub;
  }

 private:
  uint64_t topic_ = 0;
};
} // anonymous namespace

int main(int argc, char** argv) {
  Env::InstallSignalHandlers();

  // Parse command line flags.
  // This is done in RunCentrifugeClient as well, but it's idempotent, so ok.
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  CentrifugeOptions options;
  Env* env = Env::Default();
  options.client_options.env = env;

  auto st = env->StdErrLogger(&options.client_options.info_log);
  if (!st.ok()) {
    CentrifugeFatal(st);
    return 1;
  }
  options.client_options.info_log->SetInfoLogLevel(
    StringToLogLevel(FLAGS_log_level.c_str()));

  st = ShardingStrategy::Create(options.client_options.info_log,
                                FLAGS_config,
                                &options.client_options.sharding);
  if (!st.ok()) {
    CentrifugeFatal(st);
    return 1;
  }

  options.generator.reset(new Generator());
  return RunCentrifugeClient(std::move(options), argc, argv);
}
