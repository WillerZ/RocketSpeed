// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/util/control_tower_router.h"

namespace rocketspeed {

ControlTowerRouter::ControlTowerRouter(
  const std::vector<ClientID>& control_towers,
  unsigned int replicas, size_t control_towers_per_log)
: host_ids_(control_towers.begin(), control_towers.end())
, replication_(replicas), control_towers_per_log_(control_towers_per_log) {
  for (const ClientID& host_id : host_ids_) {
    mapping_.Add(&host_id, replicas);
  }
}

Status ControlTowerRouter::GetControlTower(LogID logID,
                                           const ClientID** out) const {
  std::vector<const ClientID*> towers;
  Status status = GetControlTowers(logID, &towers);
  if (status.ok())
    *out = towers[0];
  return status;
}

Status ControlTowerRouter::GetControlTowers(
    LogID logID,
    std::vector<const ClientID*>* out) const {
  size_t count = std::min(control_towers_per_log_, mapping_.SlotCount());
  out->resize(count);
  if (count == 0)
    return Status::NotFound();

  mapping_.MultiGet(logID, count, out->begin());
  return Status::OK();
}

Status ControlTowerRouter::AddControlTower(const ClientID& host_id) {
  auto result = host_ids_.insert(host_id);
  if (!result.second) {
    return Status::InvalidArgument("ClientID already in router");
  }
  mapping_.Add(&*result.first, replication_);
  return Status::OK();
}

Status ControlTowerRouter::RemoveControlTower(const ClientID& host_id) {
  auto iter = host_ids_.find(host_id);
  if (iter == host_ids_.end()) {
    return Status::InvalidArgument("ClientID not in router");
  }
  mapping_.Remove(&*iter);
  return Status::OK();
}

}  // namespace rocketspeed
