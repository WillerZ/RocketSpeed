// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <memory>
#include <functional>
#include <limits>
#include <vector>

#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"

namespace rocketspeed {

using ControlTowerId = uint64_t;
class HostId;
using LogID = uint64_t;

/**
 * Interface that provides logic for routing logs to control towers.
 * This will primarily be used by the CoPilots when subscribing to topics.
 */
class ControlTowerRouter {
 public:
  virtual ~ControlTowerRouter() = default;

  /**
   * Gets the IDs of the control tower rings that are tailing this log.
   *
   * @param logID The ID of the log to lookup.
   * @param out Where to place the resulting control tower ring host IDs.
   * @return on success OK(), otherwise errorcode.
   */
  virtual Status GetControlTowers(LogID logID,
                                  std::vector<HostId const*>* out) const = 0;
};

}  // namespace rocketspeed
