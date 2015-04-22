// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "include/Logger.h"
#include "include/Status.h"

#include "src-gen/djinni/cpp/LogLevel.hpp"
#include "src-gen/djinni/cpp/Status.hpp"

namespace rocketspeed {

class Configuration;

namespace djinni {

class ConfigurationImpl;

std::shared_ptr<rocketspeed::Configuration> ToConfiguration(
    ConfigurationImpl config);

rocketspeed::InfoLogLevel ToInfoLogLevel(LogLevel log_level);

Status FromStatus(rocketspeed::Status status);

}  // namespace djinni
}  // namespace rocketspeed
