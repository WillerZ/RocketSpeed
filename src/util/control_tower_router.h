// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <set>
#include <string>
#include <vector>
#include "include/Types.h"
#include "src/util/consistent_hash.h"
#include "src/util/hash.h"
#include "src/util/storage.h"

namespace rocketspeed {

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
   * @param control_towers List of control towers to map to.
   * @param replicas Number of hash ring replicas (higher means better
   *        distribution, but more memory usage)
   */
  explicit ControlTowerRouter(const std::vector<HostId>& control_towers,
                              unsigned int replicas);

  /**
   * Gets the host ID of the control tower ring that is tailing this log.
   *
   * @param logID The ID of the log to lookup.
   * @param out Where to place the resulting control tower ring host ID.
   * @return on success OK(), otherwise errorcode.
   */
  Status GetControlTower(LogID logID, HostId const** out) const;

  /**
   * Adds a control tower from the mapping.
   *
   * @param url The host ID of the control tower to add.
   * @return on success OK(), otherwise errorcode.
   */
  Status AddControlTower(const HostId& host_id);

  /**
   * Removes a control tower from the mapping.
   *
   * @param url The host ID of the control tower to remove.
   * @return on success OK(), otherwise errorcode.
   */
  Status RemoveControlTower(const HostId& host_id);

 private:
  struct HostIdHash {
    size_t operator()(const HostId* host_id) const {
      return MurmurHash2<std::string, size_t>()(
        host_id->hostname,
        static_cast<size_t>(host_id->port));
    }
  };

  std::set<HostId> host_ids_;
  ConsistentHash<LogID,
                 const HostId*,
                 MurmurHash2<LogID>,
                 HostIdHash> mapping_;
  unsigned int replication_;
};

}  // namespace rocketspeed
