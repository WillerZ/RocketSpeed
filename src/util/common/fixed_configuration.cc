// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "fixed_configuration.h"

#include <memory>

#include "include/Status.h"
#include "include/Types.h"
#include "include/HostId.h"
#include "src/util/common/parsing.h"
#include <xxhash.h>

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
  size_t shards = 1;
  {
    auto it = config.find("shards");
    if (it != config.end()) {
      std::istringstream ss(it->second);
      ss >> shards;
    }
  }

  RS_ASSERT(hosts.size() == 2);
  if (out_publisher) {
    out_publisher->reset(new FixedPublisherRouter(std::move(hosts[0])));
  }
  if (out_sharding) {
    out_sharding->reset(new FixedShardingStrategy(std::move(hosts[1]), shards));
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

////////////////////////////////////////////////////////////////////////////////
size_t FixedShardingStrategy::GetShardWithParams(
    Slice, Slice topic, const IntroParameters&) const {
  return XXH64(topic.data(), topic.size(), 0xFA3A228DC86EA1B6ULL) % shards_;
}

}  // namespace rocketspeed
