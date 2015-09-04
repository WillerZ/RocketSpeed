// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/util/control_tower_router.h"

#include "src/util/common/host_id.h"

namespace rocketspeed {

ConsistentHashTowerRouter::ConsistentHashTowerRouter(
  std::unordered_map<ControlTowerId, HostId> control_towers,
  unsigned int replicas, size_t control_towers_per_log)
: host_ids_(std::move(control_towers))
, control_towers_per_log_(control_towers_per_log) {
  for (auto const& node_host : host_ids_) {
    mapping_.Add(node_host.first, replicas);
  }
}

Status ConsistentHashTowerRouter::GetControlTowers(
    LogID logID,
    std::vector<const HostId*>* out) const {
  size_t count = std::min(control_towers_per_log_, mapping_.SlotCount());
  out->resize(count);
  if (count == 0) {
    return Status::NotFound();
  }

  std::vector<ControlTowerId> node_ids;
  node_ids.resize(count);
  mapping_.MultiGet(logID, count, node_ids.begin());
  for (size_t i = 0; i < count; ++i) {
    // Map node IDs to host IDs.
    auto it = host_ids_.find(node_ids[i]);
    if (it == host_ids_.end()) {
      return Status::NotFound();
    }
    (*out)[i] = &it->second;
  }
  return Status::OK();
}

}  // namespace rocketspeed
