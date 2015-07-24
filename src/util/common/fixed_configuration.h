// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Status.h"
#include "include/Types.h"
#include "src/util/common/host_id.h"

namespace rocketspeed {

/**
 * Simple implementation of Configuration where there is a single pilot and
 * copilot host to connect to. This is useful for controlled situations like
 * testing and benchmarking where the hosts are known beforehand, and are
 * unlikely to change.
 */
class FixedConfiguration : public Configuration {
 public:
  FixedConfiguration(HostId pilot, HostId copilot);

  Status GetPilot(HostId* host_out) const override;

  Status GetCopilot(HostId* host_out) const override;

  uint64_t GetCopilotVersion() const override { return 0; }

 private:
  HostId pilot_;
  HostId copilot_;
};

}  // namespace rocketspeed
