#include <stdio.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include "include/Env.h"
#include "include/Rocketeer.h"
#include "include/RocketeerServer.h"

using namespace rocketspeed;

DEFINE_uint64(port, 5834, "port to listen on");
DEFINE_uint64(threads, 16, "number of threads");

/**
 * Rocketeer that just accepts subscriptions, but does nothing else.
 */
class AcceptAllRocketeer : public Rocketeer {
 public:
  AcceptAllRocketeer() {}

  void HandleNewSubscription(
      Flow*, InboundID, SubscriptionParameters) override {
  }

  void HandleUnsubscribe(
      Flow*, InboundID, NamespaceID, Topic, TerminationSource) override {}
};

int main(int argc, char** argv) {
  // Start AcceptAll Rocketeer listening on port supplied in flags.
  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);
  rocketspeed::Env::InstallSignalHandlers();

  RocketeerOptions options;
  options.port = static_cast<uint16_t>(FLAGS_port);
  options.stats_prefix = "acceptall";

  auto server = RocketeerServer::Create(std::move(options));
  std::vector<AcceptAllRocketeer> rocketeers(FLAGS_threads);
  for (auto& rocketeer : rocketeers) {
    server->Register(&rocketeer);
  }

  auto st = server->Start();
  if (!st.ok()) {
    fprintf(stderr, "Failed to start server: %s\n", st.ToString().c_str());
    return 1;
  }
  pause();
  server->Stop();
  return 0;
}
