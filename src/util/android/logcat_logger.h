// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#if !defined(OS_ANDROID)
#error This is Android-only code.
#endif

#include <cstdarg>
#include <memory>

#include "include/Logger.h"

#include <android/log.h>

#pragma GCC visibility push(default)
namespace rocketspeed {

/**
 * Logger implementation which sends all messages to logcat (Android system
 * logger). You can read logs from attached device via ADB.
 */
class LogcatLogger final : public Logger {
 public:
  explicit LogcatLogger(const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
      : Logger(log_level) {
  }

  void Logv(const InfoLogLevel log_level, const char* format, va_list ap) {
    if (log_level < GetInfoLogLevel()) {
      return;
    }
    int prio;
    switch (log_level) {
      case InfoLogLevel::DEBUG_LEVEL:
        prio = ANDROID_LOG_DEBUG;
        break;
      case InfoLogLevel::INFO_LEVEL:
        prio = ANDROID_LOG_INFO;
        break;
      case InfoLogLevel::WARN_LEVEL:
        prio = ANDROID_LOG_WARN;
        break;
      case InfoLogLevel::ERROR_LEVEL:
        prio = ANDROID_LOG_ERROR;
        break;
      case InfoLogLevel::FATAL_LEVEL:
        prio = ANDROID_LOG_FATAL;
        break;
      case InfoLogLevel::VITAL_LEVEL:
        prio = ANDROID_LOG_SILENT;
        break;
      default:
        prio = ANDROID_LOG_DEFAULT;
    }
    __android_log_vprint(prio, "RocketSpeed", format, ap);
  }

  void Append(const char* format, va_list ap) {
    RS_ASSERT(false);
  }
};

}  // namespace rocketspeed
#pragma GCC visibility pop
