// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/util/control_tower_router.h"

#include "src/util/common/autovector.h"
#include "include/HostId.h"
#include "src/util/xxhash.h"

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

RendezvousHashTowerRouter::RendezvousHashTowerRouter(
  std::unordered_map<ControlTowerId, HostId> control_towers,
  size_t control_towers_per_log)
: host_ids_(std::move(control_towers))
, control_towers_per_log_(control_towers_per_log) {
}

Status RendezvousHashTowerRouter::GetControlTowers(
    LogID log_id,
    std::vector<const HostId*>* out) const {
  size_t count = std::min(control_towers_per_log_, host_ids_.size());
  out->resize(count);
  if (count == 0) {
    return Status::NotFound();
  }

  // Compute weights for each control tower.
  // Note: This can be done with asymptotically better performance using a
  // virtual hierarchy of nodes, but this is unnecessary since typically the
  // number of hosts at each level is small, and we cache the results.
  autovector<std::pair<size_t, const HostId*>, 10> weights;
  for (const auto& entry : host_ids_) {
    // Compute hash of (log_id, host_id).
    const uint64_t seed = 0x388b9301c3cd063c;
    XXH64_state_t state;
    XXH64_reset(&state, seed);
    XXH64_update(&state, &log_id, sizeof(log_id));
    XXH64_update(&state, &entry.first, sizeof(entry.first));
    size_t hash = XXH64_digest(&state);
    weights.emplace_back(hash, &entry.second);
  }

  // Find the lowest 'count' hashes, and use corresponding control towers.
  std::nth_element(weights.begin(), weights.begin() + count, weights.end());
  for (size_t i = 0; i < count; ++i) {
    (*out)[i] = weights[i].second;
  }
  return Status::OK();
}

}  // namespace rocketspeed
