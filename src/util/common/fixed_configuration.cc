// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/fixed_configuration.h"
#include "include/Status.h"
#include "include/Types.h"

namespace rocketspeed {

FixedConfiguration::FixedConfiguration(HostId pilot, HostId copilot)
: pilot_(std::move(pilot))
, copilot_(std::move(copilot)) {
}

Status FixedConfiguration::GetPilot(HostId* host_out) const {
  *host_out = pilot_;
  return Status::OK();
}

Status FixedConfiguration::GetCopilot(HostId* host_out) const {
  *host_out = copilot_;
  return Status::OK();
}

}  // namespace rocketspeed
