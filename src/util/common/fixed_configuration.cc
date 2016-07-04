// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "fixed_configuration.h"

#include <memory>

#include "external/folly/Memory.h"

#include "include/Status.h"
#include "include/Types.h"
#include "include/HostId.h"
#include "src/util/common/parsing.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
Status CreateFixedConfiguration(
    const std::string& config_str,
    std::unique_ptr<PublisherRouter>* out_publisher,
    std::unique_ptr<ShardingStrategy>* out_sharding) {
  RS_ASSERT(out_publisher || out_sharding);

  auto config = ParseMap(config_str);
  if (config.find("one-host") == config.end()) {
    return Status::InvalidArgument("Not a FixedPublisherRouter");
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

  RS_ASSERT(hosts.size() == 2);
  if (out_publisher) {
    out_publisher->reset(new FixedPublisherRouter(std::move(hosts[0])));
  }
  if (out_sharding) {
    out_sharding->reset(new FixedShardingStrategy(std::move(hosts[1])));
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
Status FixedPublisherRouter::GetPilot(HostId* host_out) const {
  if (!pilot_) {
    return Status::NotFound();
  }
  *host_out = pilot_;
  return Status::OK();
}

}  // namespace rocketspeed
