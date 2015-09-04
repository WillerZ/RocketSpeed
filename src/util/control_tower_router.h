// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <vector>
#include <unordered_map>

#include "include/Types.h"
#include "src/copilot/control_tower_router.h"
#include "src/util/consistent_hash.h"
#include "src/util/common/hash.h"
#include "src/util/common/host_id.h"

namespace rocketspeed {

/**
 * The log to control tower mapping which uses ring consistent hashing, that
 * distributes logs to control towers evenly, and in a way that changes the
 * mapping minimally when control towers are added or lost.
 */
class ConsistentHashTowerRouter : public ControlTowerRouter {
 public:
  /**
   * Constructs a new ConsistentHashTowerRouter.
   *
   * @param control_towers Map of control tower IDs to hosts.
   * @param replicas Number of hash ring replicas (higher means better
   *        distribution, but more memory usage)
   * @param control_towers_per_log Each log is mapped to this many
   *        control towers.
   */
  explicit ConsistentHashTowerRouter(
    std::unordered_map<ControlTowerId, HostId> control_towers,
    unsigned int replicas,
    size_t control_towers_per_log);

  /** Copyable and movable */
  ConsistentHashTowerRouter(const ConsistentHashTowerRouter&) = default;
  ConsistentHashTowerRouter& operator=(const ConsistentHashTowerRouter&) =
      default;
  ConsistentHashTowerRouter(ConsistentHashTowerRouter&&) = default;
  ConsistentHashTowerRouter& operator=(ConsistentHashTowerRouter&&) = default;

  Status GetControlTowers(LogID logID,
                          std::vector<HostId const*>* out) const override;

  size_t GetNumTowersPerLog(LogID log_id) const override {
    return control_towers_per_log_;
  }

 private:
  struct ControlTowerIdHash {
    size_t operator()(ControlTowerId id) const {
      return MurmurHash2<ControlTowerId>()(id);
    }
  };

  std::unordered_map<ControlTowerId, HostId> host_ids_;
  ConsistentHash<LogID,
                 ControlTowerId,
                 MurmurHash2<LogID>,
                 ControlTowerIdHash> mapping_;
  unsigned int replication_;
  size_t control_towers_per_log_;
};

}  // namespace rocketspeed
