// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "conversions.h"

#include <cassert>
#include <stdexcept>
#include <memory>
#include <limits>
#include <string>

#include "include/Logger.h"
#include "include/Types.h"

#include "src-gen/djinni/cpp/ConfigurationImpl.hpp"
#include "src-gen/djinni/cpp/HostId.hpp"
#include "src-gen/djinni/cpp/LogLevel.hpp"
#include "src-gen/djinni/cpp/Status.hpp"
#include "src-gen/djinni/cpp/StatusCode.hpp"

namespace rocketspeed {
namespace djinni {

std::shared_ptr<rocketspeed::Configuration> ToConfiguration(
    ConfigurationImpl config) {
  return std::make_shared<FixedConfiguration>(
    HostId(config.pilots[0].hostname, config.pilots[0].port),
    HostId(config.copilots[0].hostname, config.copilots[0].port));
}

rocketspeed::InfoLogLevel ToInfoLogLevel(LogLevel log_level) {
  using rocketspeed::InfoLogLevel;
  static_assert(InfoLogLevel::DEBUG_LEVEL ==
                    static_cast<InfoLogLevel>(LogLevel::DEBUG_LEVEL),
                "Enum representations do not match.");
  static_assert(InfoLogLevel::NUM_INFO_LOG_LEVELS ==
                    static_cast<InfoLogLevel>(LogLevel::NUM_INFO_LOG_LEVELS),
                "Enum representations do not match.");
  return static_cast<InfoLogLevel>(log_level);
}

Status FromStatus(rocketspeed::Status status) {
  StatusCode code = StatusCode::INTERNAL;
  if (status.ok()) {
    code = StatusCode::OK;
  } else if (status.IsNotFound()) {
    code = StatusCode::NOTFOUND;
  } else if (status.IsNotSupported()) {
    code = StatusCode::NOTSUPPORTED;
  } else if (status.IsInvalidArgument()) {
    code = StatusCode::INVALIDARGUMENT;
  } else if (status.IsIOError()) {
    code = StatusCode::IOERROR;
  } else if (status.IsNotInitialized()) {
    code = StatusCode::NOTINITIALIZED;
  } else if (status.IsUnauthorized()) {
    code = StatusCode::UNAUTHORIZED;
  } else if (status.IsTimedOut()) {
    code = StatusCode::TIMEDOUT;
  } else if (status.IsInternal()) {
    code = StatusCode::INTERNAL;
  } else {
    assert(false);
  }
  return Status(code, std::move(status.ToString()));
}

}  // namespace djinni
}  // namespace rocketspeed
