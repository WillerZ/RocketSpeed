#pragma GCC diagnostic ignored "-Wshadow" // Djinni isn't -Wshadow compatible

#include <memory>
#include "./include/RocketSpeed.h"
#include "./src/djinni/generated/c++/Configuration.hpp"

namespace rocketglue {

class ConfigurationImpl : public Configuration {
 public:
  // constructor
  ConfigurationImpl(std::unique_ptr<rocketspeed::Configuration> conf) :
    conf_(std::move(conf)) {}

  std::vector<HostId> GetPilotHostIds() override;
  std::vector<HostId> GetCopilotHostIds() override;
  TenantID GetTenantID() override;

 private:
  // The base rocketspedd config object
  std::unique_ptr<rocketspeed::Configuration> conf_;

};

}
