// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "fixed_configuration.h"

#include <memory>

#include "include/Status.h"
#include "include/Types.h"
#include "src/util/common/host_id.h"
#include "src/util/common/parsing.h"

namespace rocketspeed {

Status FixedConfiguration::CreateConfiguration(
    const std::string& config_str,
    std::unique_ptr<Configuration>* out) {
  assert(out);

  auto config = ParseMap(config_str);
  if (config.find("one-host") == config.end()) {
    return Status::InvalidArgument("Not a FixedConfiguration");
  }

  std::vector<HostId> hosts;
  for (const auto& name : {"pilot", "copilot"}) {
    HostId host;
    auto it = config.find(name);
    if (it != config.end()) {
      Status st = HostId::Resolve(it->second, &host);
      if (!st.ok()) {
        return st;
      }
    }
    hosts.emplace_back(std::move(host));
  }

  assert(hosts.size() == 2);
  out->reset(new FixedConfiguration(std::move(hosts[0]), std::move(hosts[1])));
  return Status::OK();
}

FixedConfiguration::FixedConfiguration(HostId pilot, HostId copilot)
: pilot_(std::move(pilot)), copilot_(std::move(copilot)) {}

Status FixedConfiguration::GetPilot(HostId* host_out) const {
  if (!pilot_) {
    return Status::NotFound();
  }
  *host_out = pilot_;
  return Status::OK();
}

Status FixedConfiguration::GetCopilot(HostId* host_out) const {
  if (!copilot_) {
    return Status::NotFound();
  }
  *host_out = copilot_;
  return Status::OK();
}

}  // namespace rocketspeed
