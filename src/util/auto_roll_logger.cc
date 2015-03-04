//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/auto_roll_logger.h"
#include "src/util/mutexlock.h"

namespace rocketspeed {

// -- AutoRollLogger
Status AutoRollLogger::ResetLogger() {
  status_ = env_->NewLogger(log_fname_, &logger_);

  if (!status_.ok()) {
    return status_;
  }

  if (logger_->GetLogFileSize() ==
      (size_t)Logger::DO_NOT_SUPPORT_GET_LOG_FILE_SIZE) {
    status_ = Status::NotSupported(
        "The underlying logger doesn't support GetLogFileSize()");
  }
  if (status_.ok()) {
    cached_now = static_cast<uint64_t>(env_->NowMicros() / 1000000);
    ctime_ = cached_now;
    cached_now_access_count = 0;
  }

  return status_;
}

void AutoRollLogger::RollLogFile() {
  std::string old_fname = OldInfoLogFileName(log_dir_, log_filename_,
                                             env_->NowMicros());
  env_->RenameFile(log_fname_, old_fname);
}

void AutoRollLogger::Append(const char* format, va_list ap) {
  assert(GetStatus().ok());

  std::shared_ptr<Logger> logger;
  {
    MutexLock l(&mutex_);
    if ((kLogFileTimeToRoll > 0 && LogExpired()) ||
        (kMaxLogFileSize > 0 && logger_->GetLogFileSize() >= kMaxLogFileSize)) {
      RollLogFile();
      Status s = ResetLogger();
      if (!s.ok()) {
        // can't really log the error if creating a new LOG file failed
        return;
      }
    }

    // pin down the current logger_ instance before releasing the mutex.
    logger = logger_;
  }

  // Another thread could have put a new Logger instance into logger_ by now.
  // However, since logger is still hanging on to the previous instance
  // (reference count is not zero), we don't have to worry about it being
  // deleted while we are accessing it.
  // Note that logv itself is not mutex protected to allow maximum concurrency,
  // as thread safety should have been handled by the underlying logger.
  logger->Append(format, ap);
}

bool AutoRollLogger::LogExpired() {
  if (cached_now_access_count >= call_NowMicros_every_N_records_) {
    cached_now = static_cast<uint64_t>(env_->NowMicros() / 1000000);
    cached_now_access_count = 0;
  }

  ++cached_now_access_count;
  return cached_now >= ctime_ + kLogFileTimeToRoll;
}

std::string InfoLogFileName(
     const std::string& path, const std::string& log_dir,
     const std::string& log_filename) {
  return path + "/" + log_dir + "/" + log_filename;
}

std::string OldInfoLogFileName(const std::string& dir,
  const std::string& log_filename, uint64_t ts) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(ts));
  return dir + "/" + log_filename + ".old." + buf;
}

Status CreateLoggerFromOptions(
    Env* env,
    const std::string& log_dir,
    const std::string& log_filename,
    size_t log_file_time_to_roll,
    size_t max_log_file_size,
    InfoLogLevel log_level,
    std::shared_ptr<Logger>* logger) {

  std::string dir;
  if (log_dir.size() == 0) {
    env->GetWorkingDirectory(&dir);
  } else {
    dir = log_dir;
  }
  std::string fname = dir + "/" + log_filename;

  env->CreateDirIfMissing(dir);  // In case it does not exist

  // Currently we only support roll by time-to-roll and log size
  if (log_file_time_to_roll > 0 || max_log_file_size > 0) {
    AutoRollLogger* result = new AutoRollLogger(
        env, dir, log_filename,
        max_log_file_size,
        log_file_time_to_roll,
        log_level);
    Status s = result->GetStatus();
    if (!s.ok()) {
      delete result;
    } else {
      logger->reset(result);
    }
    return s;
  } else {
    // Open a log file in the same directory as the db
    env->RenameFile(fname, OldInfoLogFileName(dir, log_filename,
                    env->NowMicros()));
    auto s = env->NewLogger(fname, logger);
    if (logger->get() != nullptr) {
      (*logger)->SetInfoLogLevel(log_level);
    }
    return s;
  }
}

}  // namespace rocketspeed
