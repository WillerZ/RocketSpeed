// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/util/control_tower_router.h"

namespace rocketspeed {

ControlTowerRouter::ControlTowerRouter(const std::vector<URL>& control_towers,
                                       unsigned int replicas)
: replication_(replicas) {
  for (const URL& url : control_towers) {
    mapping_.Add(url, replicas);
  }
}

Status ControlTowerRouter::GetControlTower(LogID logID, URL* out) const {
  *out = mapping_.Get(logID);
  return Status::OK();
}

Status ControlTowerRouter::AddControlTower(const URL& url) {
  mapping_.Add(url, replication_);
  return Status::OK();
}

Status ControlTowerRouter::RemoveControlTower(const URL& url) {
  mapping_.Remove(url);
  return Status::OK();
}

}  // namespace rocketspeed
