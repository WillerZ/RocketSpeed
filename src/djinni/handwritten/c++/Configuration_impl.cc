#include "Configuration_impl.h"

namespace rocketglue {

std::shared_ptr<Configuration> Configuration::CreateNewInstance(
  const std::vector<HostId>& pilots,
  const std::vector<HostId>& copilots,
  const TenantID& tenant_id) {

  std::vector<rocketspeed::HostId> pilot_rs;
  std::vector<rocketspeed::HostId> copilot_rs;

  // translate from rocketglue structures to rocketspeed structures
  for (auto host : pilots) {
    pilot_rs.push_back(rocketspeed::HostId(host.hostname, host.port));
  }
  for (auto host : copilots) {
    copilot_rs.push_back(rocketspeed::HostId(host.hostname, host.port));
  }
  rocketspeed::TenantID tenant = static_cast<rocketspeed::TenantID>(tenant_id.tenantid);

  // create a rocketspeed configuration object
  std::unique_ptr<rocketspeed::Configuration> pconfig(
                               rocketspeed::Configuration::Create(
                                 pilot_rs,
                                 copilot_rs,
                                 tenant));
  return std::make_shared<ConfigurationImpl>(std::move(pconfig));
};

std::vector<HostId> ConfigurationImpl::GetPilotHostIds() {
  std::vector<HostId> ret;
  for (auto host : conf_.get()->GetPilotHostIds()) {
    ret.push_back(HostId(host.hostname, host.port));
  }
  return ret;
};

std::vector<HostId> ConfigurationImpl::GetCopilotHostIds() {
  std::vector<HostId> ret;
  for (auto host : conf_.get()->GetCopilotHostIds()) {
    ret.push_back(HostId(host.hostname, (int16_t)host.port));
  }
  return ret;
};

TenantID ConfigurationImpl::GetTenantID() {
  return TenantID(conf_.get()->GetTenantID());
};

}
