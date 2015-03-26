// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma GCC diagnostic ignored "-Wshadow"
#include "src/logdevice/AsyncReader.h"
#include "src/logdevice/Common.h"
#include <assert.h>
#include <algorithm>
#include <mutex>
#include <string>
#include <thread>

namespace facebook { namespace logdevice {

// Version of the LogDevice DataRecord that frees the payload when done.
// The payload is copied into a member string to persist it.
struct MockDataRecord : public DataRecord {
 public:
  MockDataRecord(logid_t logid,
                 const Payload& payload,
                 lsn_t lsn,
                 std::chrono::milliseconds timestamp)
  : payload_(reinterpret_cast<const char*>(payload.data), payload.size) {
    this->payload = Payload(payload_.data(), payload_.size());
    this->logid = logid;
    this->attrs.lsn = lsn;
    this->attrs.timestamp = timestamp;
  }

 private:
  std::string payload_;
};

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
        // Check if file exists
        if (!env_->FileExists(fname)) {
          // Determine whether or not file deleted due to trimming
          auto lsn = LastSequenceNumber(tailer.first);
          if (lsn != LSN_INVALID) {
            // File has been written to at some point and deleted due to
            // trimming.
            if (log.from_ <= lsn) {
              // If the from_ is before or up to final lsn
              // and the file doesn't exist, then we need to send a gap
              GapRecord record {
                tailer.first,
                GapType::TRIM,
                log.from_,
                std::min(log.until_, lsn)
              };
              if (gap_cb_) {
                gap_cb_(record);
              }
            }
          }
        } else if (env_->GetFileModificationTime(fname, &modTime).ok()) {
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

// Returns the last sequence number written by a file
// Possibly could be moved to env_posix as a duplicate
// piece of code exists in Client.cc
lsn_t AsyncReaderImpl::LastSequenceNumber(logid_t logid) {
  // Seqno is stored in its own file.
  std::string fname = SeqnoFilename(logid);
  rocketspeed::FileLock* fileLock;

  while (!(env_->LockFile(fname, &fileLock).ok() && fileLock)) {
    // Another thread or process has the lock, so yield to let them finish.
    std::this_thread::yield();
  }

  // Lock acquired, now try to open the file.
  rocketspeed::EnvOptions opts;
  opts.use_mmap_writes = false;  // PosixRandomRWFile doesn't support this
  std::unique_ptr<rocketspeed::RandomRWFile> file;
  if (!(env_->NewRandomRWFile(fname, &file, opts).ok() && file)) {
    env_->UnlockFile(fileLock);
    return LSN_INVALID;
  }

  lsn_t lsn = LastSeqnoWritten(fname, file, env_);
  env_->UnlockFile(fileLock);

  // Return last lsn written
  return lsn - 1;
}

// Process a log file, calling the callback for each record in the LSN range.
// Returns the next LSN to begin reading from.
lsn_t AsyncReaderImpl::ReadFile(logid_t logid, lsn_t from, lsn_t until) {
  // Iterate log records.
  LogFile file(logid, false);
  bool opened = false;
  lsn_t last_lsn = 0;
  (void)last_lsn;
  auto expected_lsn = from;
  while (file.Next()) {
    opened = true;
    lsn_t current_lsn = file.GetLSN();
    assert(current_lsn > last_lsn);
    last_lsn = current_lsn;
    if (current_lsn > expected_lsn) {
      // There is a gap from expected lsn to the minimum
      // of current lsn or until.
      GapRecord record {
        logid,
        GapType::TRIM,
        expected_lsn,
        std::min(until, current_lsn - 1)
      };
      if (gap_cb_) {
        gap_cb_(record);
      }

      // We are now proccessing from current_lsn.
      expected_lsn = current_lsn;
    }
    if (current_lsn > until) {
      // Past the end of our LSN range, so exit now.
      return current_lsn;
    }
    if (current_lsn == expected_lsn) {
      // In range, so call callback and adjust the return LSN.
      expected_lsn++;
      std::unique_ptr<DataRecord> dataRecord(
        new MockDataRecord(
          logid,
          file.GetData(),
          current_lsn,
          file.GetTimestamp()));
      data_cb_(std::move(dataRecord));
    }
  }
  return opened ? expected_lsn : LSN_INVALID;
}

void AsyncReader::setRecordCallback(
    std::function<void(std::unique_ptr<DataRecord>)> cb) {
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
