// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/util/control_tower_router.h"

namespace rocketspeed {

ControlTowerRouter::ControlTowerRouter(
  const std::vector<HostId>& control_towers,
  unsigned int replicas)
: host_ids_(control_towers.begin(), control_towers.end())
, replication_(replicas) {
  for (const HostId& host_id : host_ids_) {
    mapping_.Add(&host_id, replicas);
  }
}

Status ControlTowerRouter::GetControlTower(LogID logID,
                                           const HostId** out) const {
  *out = mapping_.Get(logID);
  return Status::OK();
}

Status ControlTowerRouter::AddControlTower(const HostId& host_id) {
  auto result = host_ids_.insert(host_id);
  if (!result.second) {
    return Status::InvalidArgument("HostId already in router");
  }
  mapping_.Add(&*result.first, replication_);
  return Status::OK();
}

Status ControlTowerRouter::RemoveControlTower(const HostId& host_id) {
  auto iter = host_ids_.find(host_id);
  if (iter == host_ids_.end()) {
    return Status::InvalidArgument("HostId not in router");
  }
  mapping_.Remove(&*iter);
  return Status::OK();
}

}  // namespace rocketspeed
