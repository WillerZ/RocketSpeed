// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <vector>
#include <unordered_map>
#include "include/Types.h"
#include "src/util/consistent_hash.h"
#include "src/util/common/hash.h"
#include "src/util/common/host_id.h"
#include "src/util/storage.h"

namespace rocketspeed {

using ControlTowerId = uint64_t;

/**
 * Class that provides logic for routing logs to control towers.
 * This will primarily be used by the CoPilots when subscribing to topics.
 *
 * The log to control tower mapping uses ring consistent hashing, which
 * distributes logs to control towers evenly, and in a way that changes the
 * mapping minimally when control towers are added or lost.
 */
class ControlTowerRouter {
 public:
  /**
   * Constructs a new ControlTowerRouter.
   *
   * @param control_towers Map of control tower IDs to hosts.
   * @param replicas Number of hash ring replicas (higher means better
   *        distribution, but more memory usage)
   * @param control_towers_per_log Each log is mapped to this many
   *        control towers.
   */
  explicit ControlTowerRouter(
    std::unordered_map<ControlTowerId, HostId> control_towers,
    unsigned int replicas,
    size_t control_towers_per_log);

  /** Copyable and movable */
  ControlTowerRouter(const ControlTowerRouter&) = default;
  ControlTowerRouter& operator=(const ControlTowerRouter&) = default;
  ControlTowerRouter(ControlTowerRouter&&) = default;
  ControlTowerRouter& operator=(ControlTowerRouter&&) = default;

  /**
    * Gets the host ID of the control tower ring that is tailing this log.
    *
    * @param logID The ID of the log to lookup.
    * @param out Where to place the resulting control tower ring host ID.
    * @return on success OK(), otherwise errorcode.
    */
  Status GetControlTower(LogID logID, HostId const** out) const;

  /**
   * Gets the IDs of the control tower rings that are tailing this log.
   *
   * @param logID The ID of the log to lookup.
   * @param out Where to place the resulting control tower ring host IDs.
   * @return on success OK(), otherwise errorcode.
   */
  Status GetControlTowers(LogID logID, std::vector<HostId const*>* out) const;

  /**
   * @return Number of unique control towers.
   */
  size_t GetNumTowers() const {
    return host_ids_.size();
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
