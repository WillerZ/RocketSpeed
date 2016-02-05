// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Types.h"
#include "src/util/common/fixed_configuration.h"

namespace rocketspeed {

Status Configuration::CreateConfiguration(
    const std::shared_ptr<Logger>& info_log,
    const std::string& config_str,
    std::unique_ptr<Configuration>* out) {
  return FixedConfiguration::CreateConfiguration(config_str, out);
}

}  // namespace rocketspeed
