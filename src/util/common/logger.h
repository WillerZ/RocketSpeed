// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdarg>
#include <memory>

#define RS_LOG(log_level_expr, info_log_expr, ...) \
  do { \
    ::rocketspeed::InfoLogLevel _log_level = (log_level_expr); \
    const auto& _info_log = (info_log_expr); \
    if (_info_log && _log_level >= _info_log->GetInfoLogLevel()) { \
      _info_log->Log(_log_level, __VA_ARGS__); \
    } \
  } while (0)

#define LOG_DEBUG(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::DEBUG_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_INFO(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::INFO_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_WARN(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::WARN_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_ERROR(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::ERROR_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_FATAL(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::FATAL_LEVEL, \
      info_log_expr, __VA_ARGS__)

namespace rocketspeed {

enum InfoLogLevel : unsigned char {
  DEBUG_LEVEL = 0,
  INFO_LEVEL,
  WARN_LEVEL,
  ERROR_LEVEL,
  FATAL_LEVEL,
  NONE_LEVEL,
  NUM_INFO_LOG_LEVELS,
};

// An interface for writing log messages. It is recommended to use the LOG
// macro instead of directly calling methods of this class because the macro
// makes sure the format arguments are not evaluated if the specified log level
// is lower than the current minimal log level, which can improve performance.
class Logger {
 public:
  enum { DO_NOT_SUPPORT_GET_LOG_FILE_SIZE = -1 };

  explicit Logger(const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
      : log_level_(log_level) {}

  virtual ~Logger() {}

  // Write an entry to the log file with the specified format.
  virtual void Logv(const char* format, va_list ap) = 0;

  // Write an entry to the log file with the specified log level
  // and format.  Any log with level under the internal log level
  // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
  // printed.
  void Logv(const InfoLogLevel log_level, const char* format, va_list ap) {
    static const char* kInfoLogLevelNames[5] = {"DEBUG", "INFO", "WARN",
                                                "ERROR", "FATAL"};
    if (log_level < log_level_) {
      return;
    }

    if (log_level == InfoLogLevel::INFO_LEVEL) {
      // Doesn't print log level if it is INFO level.
      // This is to avoid unexpected performance regression after we add
      // the feature of log level. All the logs before we add the feature
      // are INFO level. We don't want to add extra costs to those existing
      // logging.
      Logv(format, ap);
    } else {
      char new_format[500];
      snprintf(new_format, sizeof(new_format) - 1, "[%s] %s",
               kInfoLogLevelNames[log_level], format);
      Logv(new_format, ap);
    }
  }

  void Log(const InfoLogLevel log_level, const char* format, ...)
#   if defined(__GNUC__) || defined(__clang__)
      __attribute__((__format__ (__printf__, 3, 4)))
#   endif
  {
    va_list ap;
    va_start(ap, format);
    Logv(log_level, format, ap);
    va_end(ap);
  }

  virtual size_t GetLogFileSize() const {
    return DO_NOT_SUPPORT_GET_LOG_FILE_SIZE;
  }

  // Flush to the OS buffers
  virtual void Flush() {}

  virtual InfoLogLevel GetInfoLogLevel() const { return log_level_; }

  virtual void SetInfoLogLevel(const InfoLogLevel log_level) {
    log_level_ = log_level;
  }

 private:
  // No copying allowed
  Logger(const Logger&);
  void operator=(const Logger&);

  InfoLogLevel log_level_;
};

// "Blackhole" logger implementation - doesn't log anything.
// Used when a Logger object is needed, but no logging is desired.
class NullLogger : public Logger {
 public:
  NullLogger() : Logger(NONE_LEVEL) {}

  virtual void Logv(const char* format, va_list ap) {}

  virtual void SetInfoLogLevel(const InfoLogLevel log_level) {}
};

extern void LogFlush(const std::shared_ptr<Logger>& info_log);

extern void LogFlush(Logger *info_log);

}  // namespace rocketspeed
