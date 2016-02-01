// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Types.h"
#include "src/util/common/fixed_configuration.h"

namespace rocketspeed {

Status ShardingStrategy::Create(
    const std::shared_ptr<Logger>& info_log,
    const std::string& config_str,
    std::unique_ptr<ShardingStrategy>* out) {
  return CreateFixedConfiguration(config_str, nullptr /* config */, out);
}

Status PublisherRouter::Create(
    const std::shared_ptr<Logger>& info_log,
    const std::string& config_str,
    std::unique_ptr<PublisherRouter>* out) {
  return CreateFixedConfiguration(config_str, out, nullptr /* sharding */);
}

}  // namespace rocketspeed
