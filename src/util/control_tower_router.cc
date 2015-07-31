// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/util/control_tower_router.h"

#include "src/util/common/host_id.h"

namespace rocketspeed {

ControlTowerRouter::ControlTowerRouter(
  std::unordered_map<ControlTowerId, HostId> control_towers,
  unsigned int replicas, size_t control_towers_per_log)
: host_ids_(std::move(control_towers))
, replication_(replicas)
, control_towers_per_log_(control_towers_per_log) {
  for (auto const& node_host : host_ids_) {
    mapping_.Add(node_host.first, replicas);
  }
}

Status ControlTowerRouter::GetControlTower(LogID logID,
                                           const HostId** out) const {
  std::vector<const HostId*> towers;
  Status status = GetControlTowers(logID, &towers);
  if (status.ok()) {
    *out = towers[0];
  }
  return status;
}

Status ControlTowerRouter::GetControlTowers(
    LogID logID,
    std::vector<const HostId*>* out) const {

  auto control_towers_cache_it = control_towers_by_log_id_cache_.find(logID);
  if(control_towers_cache_it != control_towers_by_log_id_cache_.end()) {
    *out = control_towers_cache_it->second;
    return Status::OK(); // result was populated using cached values.
  }

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

  control_towers_by_log_id_cache_[logID] = *out;

  return Status::OK();
}

Status ControlTowerRouter::AddControlTower(ControlTowerId node_id,
                                           HostId host_id) {
  control_towers_by_log_id_cache_.clear(); // invalidate cache.

  auto result = host_ids_.emplace(node_id, std::move(host_id));
  if (!result.second) {
    return Status::InvalidArgument("Node ID already in router");
  }
  mapping_.Add(node_id, replication_);
  return Status::OK();
}

Status ControlTowerRouter::RemoveControlTower(ControlTowerId node_id) {
  control_towers_by_log_id_cache_.clear(); // invalidate cache.

  auto iter = host_ids_.find(node_id);
  if (iter == host_ids_.end()) {
    return Status::InvalidArgument("Node ID not in router");
  }
  mapping_.Remove(node_id);
  host_ids_.erase(iter);
  return Status::OK();
}

}  // namespace rocketspeed
