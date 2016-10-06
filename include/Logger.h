// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include "Assert.h"
#include "RateLimiter.h"

#define RS_LOG(log_level_expr, info_log_expr, ...) \
  do { \
    ::rocketspeed::InfoLogLevel _log_level = (log_level_expr); \
    const auto& _info_log = (info_log_expr); \
    if (_info_log && _log_level >= _info_log->GetInfoLogLevel()) { \
      _info_log->Log(_log_level, __FILE__, __LINE__, __VA_ARGS__); \
      if (_log_level >= ::rocketspeed::InfoLogLevel::FATAL_LEVEL) { \
        _info_log->Flush(); \
      } \
    } \
  } while (0)

#define RS_LOG_RATELIMIT(log_level_expr, info_log_expr, \
                         limit_events, duration, ...) \
  do { \
    static RateLimiter rs_log_limiter(limit_events, duration); \
    static std::atomic<bool> rs_log_lock; \
    \
    /* If someone is holding the lock, we don't print a message and */ \
    /* don't even increment skipped. This would increase overhead   */ \
    /* considerably because of cache contention.                    */ \
    if (rs_log_lock.load(std::memory_order_acquire) || \
        rs_log_lock.exchange(true, std::memory_order_acquire)) { \
      break; \
    } \
    static size_t skipped; \
    if (rs_log_limiter.IsAllowed()) { \
      rs_log_limiter.TakeOne(); \
      if (skipped > 0) { \
        RS_LOG(log_level_expr, info_log_expr, \
            "Skipped at least %zu log entries as below", skipped); \
        skipped = 0; \
      } \
      RS_LOG(log_level_expr, info_log_expr, __VA_ARGS__); \
    } else { \
      ++skipped; \
    } \
    rs_log_lock.store(false); \
  } while (0)

#define LOG_DEBUG(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::DEBUG_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_INFO(info_log_expr, ...) \
  RS_LOG_RATELIMIT(::rocketspeed::InfoLogLevel::INFO_LEVEL, \
                   info_log_expr, \
                   10, std::chrono::seconds(10), \
                   __VA_ARGS__)

#define LOG_WARN(info_log_expr, ...) \
  RS_LOG_RATELIMIT(::rocketspeed::InfoLogLevel::WARN_LEVEL, \
                   info_log_expr, \
                   10, std::chrono::seconds(10), \
                   __VA_ARGS__)

#define LOG_ERROR(info_log_expr, ...) \
  RS_LOG_RATELIMIT(::rocketspeed::InfoLogLevel::ERROR_LEVEL, \
                   info_log_expr, \
                   10, std::chrono::seconds(10), \
                   __VA_ARGS__)

#define LOG_FATAL(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::FATAL_LEVEL, \
      info_log_expr, __VA_ARGS__); \

#define LOG_VITAL(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::VITAL_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_INFO_NOLIMIT(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::INFO_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_WARN_NOLIMIT(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::WARN_LEVEL, \
      info_log_expr, __VA_ARGS__)

#define LOG_ERROR_NOLIMIT(info_log_expr, ...) \
  RS_LOG(::rocketspeed::InfoLogLevel::ERROR_LEVEL, \
      info_log_expr, __VA_ARGS__)

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif
namespace rocketspeed {

enum InfoLogLevel : unsigned char {
  DEBUG_LEVEL = 0,
  INFO_LEVEL,
  WARN_LEVEL,
  ERROR_LEVEL,
  FATAL_LEVEL,
  VITAL_LEVEL,
  NONE_LEVEL,
  NUM_INFO_LOG_LEVELS,
};

/**
 * Convert an InfoLogLevel to string.
 *
 * @param level Log level to convert.
 * @return Corresponding string.
 */
inline const char* LogLevelToString(InfoLogLevel level) {
  static const char* kInfoLogLevelNames[NUM_INFO_LOG_LEVELS] = {
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "FATAL",
    "VITAL",
    "NONE"
  };
  RS_ASSERT(level >= 0 && level < NUM_INFO_LOG_LEVELS);
  return kInfoLogLevelNames[level];
}

/**
 * Checks whether the passed string is a correct name for the log level.
 */
inline bool IsValidLogLevel(const std::string& str) {
  for (int i = 0; i < NUM_INFO_LOG_LEVELS; ++i) {
    InfoLogLevel level = static_cast<InfoLogLevel>(i);
    if (strcasecmp(LogLevelToString(level), str.c_str()) == 0) {
      return true;
    }
  }
  return false;
}

/**
 * Convert a string to an InfoLogLevel. Not case sensitive.
 *
 * @param str String to convert.
 * @return Corresponding info log level, or WARN if no match.
 */
inline InfoLogLevel StringToLogLevel(const char* str) {
  for (int i = 0; i < NUM_INFO_LOG_LEVELS; ++i) {
    InfoLogLevel level = static_cast<InfoLogLevel>(i);
    if (strcasecmp(LogLevelToString(level), str) == 0) {
      return level;
    }
  }
  return WARN_LEVEL;
}

// An interface for writing log messages. It is recommended to use the LOG
// macro instead of directly calling methods of this class because the macro
// makes sure the format arguments are not evaluated if the specified log level
// is lower than the current minimal log level, which can improve performance.
class Logger {
 public:
  enum { DO_NOT_SUPPORT_GET_LOG_FILE_SIZE = -1 };

  explicit Logger(InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
      : log_level_(log_level) {}

  virtual ~Logger() {}

  /**
   * Write an entry to the log file with the specified log level and format.
   * Default implementation ensures that log statement with level under the
   * internal log level will not be appended.
   * By default entries are appended using pure virtual Append function.
   */
  virtual void Logv(const InfoLogLevel log_level,
                    const char* filename,
                    int line,
                    const char* format,
                    va_list ap) {

    if (log_level < GetInfoLogLevel()) {
      return;
    }

    char new_format[500];
    snprintf(new_format,
             sizeof(new_format) - 1,
             "[%s] %s:%d: %s",
             LogLevelToString(log_level),
             Basename(filename),
             line,
             format);
    Append(new_format, ap);
  }

  virtual void Log(const InfoLogLevel log_level,
                   const char* filename,
                   int line,
                   const char* format,
                   ...) final
#if defined(__GNUC__) || defined(__clang__)
      __attribute__((__format__(__printf__, 5, 6)))
#endif
  {
    va_list ap;
    va_start(ap, format);
    Logv(log_level, filename, line, format, ap);
    va_end(ap);
  }

  virtual size_t GetLogFileSize() const {
    return DO_NOT_SUPPORT_GET_LOG_FILE_SIZE;
  }

  /** Write an entry to the log file with the specified format. */
  virtual void Append(const char* format, va_list ap) = 0;

  /** Flush to the OS buffers. */
  virtual void Flush() {}

  virtual InfoLogLevel GetInfoLogLevel() const {
    return log_level_.load(std::memory_order_relaxed);
  }

  virtual void SetInfoLogLevel(const InfoLogLevel log_level) {
    log_level_.store(log_level, std::memory_order_relaxed);
  }

 private:
  // No copying allowed
  Logger(const Logger&);
  void operator=(const Logger&);

  std::atomic<InfoLogLevel> log_level_;

  const char* Basename(const char* filename) {
    const char* result = strrchr(filename, '/');
    if (result == nullptr) {
      return filename;
    } else {
      return result + 1;
    }
  }
};

/**
 * "Blackhole" logger implementation - doesn't log anything.
 * Used when a Logger object is needed, but no logging is desired.
 */
class NullLogger final : public Logger {
 public:
  NullLogger() : Logger(NONE_LEVEL) {}

  void SetInfoLogLevel(const InfoLogLevel log_level) {}

 protected:
  void Append(const char* format, va_list ap) {}
};

}  // namespace rocketspeed
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
