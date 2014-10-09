// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/logdevice/AsyncReader.h"
#include <assert.h>
#include <algorithm>
#include <mutex>
#include <string>
#include <thread>

namespace facebook { namespace logdevice {

AsyncReaderImpl::AsyncReaderImpl()
: env_(rocketspeed::Env::Default()) {
  // Start a thread that polls all the logs currently being read.
  thread_ = std::thread([this] {
    while (!done_) {
      // Wait 100ms before polling again
      // Allows other threads to lock the mutex, and stops us from busily
      // hitting the OS for file attrs.
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      // Check all the logs for data
      std::lock_guard<std::mutex> lock(mutex_);
      for (auto& tailer : logs_) {
        Log& log = tailer.second;
        std::string fname = LogFilename(tailer.first);
        uint64_t modTime;
        if (env_->GetFileModificationTime(fname, &modTime).ok()) {
          // Has the file been modified since last successful read?
          if (modTime > tailer.second.last_mod_) {
            auto newLSN = ReadFile(tailer.first, log.from_, log.until_);
            if (newLSN != LSN_INVALID) {
              log.from_ = newLSN;

              // Update the last_mod_ time.
              // We set this to the min of the actual last modified time, and
              // 1 second ago. The reason for this is because a write could
              // happen to a log within one second of reading from it, and since
              // modified time is only accurate to one second, we will think
              // there is no additional write. Using now - 1 second means we
              // process any modification in the last second, even if we have
              // already read the file during that time.
              log.last_mod_ = std::min<uint64_t>(modTime, time(0) - 1);
            }
          }
        }
      }
    }
  });
}

// Process a log file, calling the callback for each record in the LSN range.
// Returns the next LSN to begin reading from.
lsn_t AsyncReaderImpl::ReadFile(logid_t logid, lsn_t from, lsn_t until) {
  // Iterate log records.
  LogFile file(logid, false);
  bool opened = false;
  lsn_t last_lsn = 0;
  (void)last_lsn;
  while (file.Next()) {
    opened = true;
    lsn_t lsn = file.GetLSN();
    assert(lsn > last_lsn);
    last_lsn = lsn;
    if (lsn > until) {
      // Past the end of our LSN range, so exit now.
      return lsn;
    }
    if (lsn >= from) {
      // In range, so call the callback and adjust the return LSN.
      from = lsn + 1;
      DataRecord dataRecord(logid, file.GetData(), lsn, file.GetTimestamp());
      data_cb_(dataRecord);
    }
  }
  return opened ? from : LSN_INVALID;
}

void AsyncReader::setRecordCallback(std::function<void(const DataRecord&)> cb) {
  impl()->data_cb_ = cb;
}

void AsyncReader::setGapCallback(std::function<void(const GapRecord&)> cb) {
  impl()->gap_cb_ = cb;
}

int AsyncReader::startReading(logid_t log_id, lsn_t from, lsn_t until) {
  assert(impl()->data_cb_);  // must set CB before starting to read
  std::lock_guard<std::mutex> lock(impl()->mutex_);
  impl()->logs_[log_id] = AsyncReaderImpl::Log{from, until, 0};
  return 0;
}

int AsyncReader::stopReading(logid_t log_id) {
  std::lock_guard<std::mutex> lock(impl()->mutex_);
  impl()->logs_.erase(log_id);
  return 0;
}

void AsyncReader::withoutPayload() {
  assert(false);  // not implemented
}

int AsyncReader::isConnectionHealthy(logid_t) const {
  // Not relevant for the mock log. Always return healthy.
  return 1;
}

AsyncReaderImpl *AsyncReader::impl() {
  return static_cast<AsyncReaderImpl*>(this);
}

const AsyncReaderImpl *AsyncReader::impl() const {
  return static_cast<const AsyncReaderImpl*>(this);
}

}  // namespace logdevice
}  // namespace facebook
