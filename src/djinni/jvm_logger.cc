// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "jvm_env.h"

#include "include/Logger.h"

#if defined(OS_ANDROID)
#include "src/util/android/logcat_logger.h"
#else
#include "src/util/posix_logger.h"
#endif

namespace rocketspeed {

std::shared_ptr<Logger> JvmEnv::CreatePlatformLogger(InfoLogLevel log_level) {
#if defined(OS_ANDROID)
  return std::make_shared<LogcatLogger>(log_level);
#else
  auto info_log = std::make_shared<PosixLogger>(stderr, false, this);
  info_log->SetInfoLogLevel(log_level);
  return info_log;
#endif
}

}  // namespace rocketspeed
