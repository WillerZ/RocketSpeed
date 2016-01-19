// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma GCC diagnostic ignored "-Wshadow"

#include "src/logdevice/Common.h"
#include <algorithm>
#include <mutex>
#include <string>
#include "include/Slice.h"
#include "include/Env.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

std::string MOCK_LOG_DIR = "_mock_logdevice_logs";

__thread E err = E::OK;

// We don't use errorStrings, so don't care about the contents, but we
// need to define these for LogDevice.
EnumMap<E, ErrorCodeInfo> errorStrings;

template <>
void EnumMap<E, ErrorCodeInfo>::setValues() {
}

LogFile::LogFile(logid_t logid, bool waitForLock)
: env_(rocketspeed::Env::Default()),
  file_lock_(env_, LogFilename(logid), waitForLock),
  file_(nullptr) {
  // Try to get a lock on the log file.
  if (file_lock_.HaveLock()) {
    // Lock acquired, now try to open the file.
    rocketspeed::EnvOptions opts;
    std::string fname = LogFilename(logid);
    if (env_->NewSequentialFile(fname, &file_, opts).ok() && file_) {
      // File open!
    } else {
      // Failed to open file, setting to null.
      file_.reset(nullptr);
    }
  }
  header_.datasize = 0;
}

bool LogFile::Next() {
  if (!file_) {
    return false;
  }
  if (!have_data_ && header_.datasize != 0) {
    // Didn't read data for last record, skip over now.
    if (!file_->Skip(header_.datasize).ok()) {
      return false;
    }
    offset_ += header_.datasize;
  }

  // Some scratch space to read the RecordHeader into.
  alignas(RecordHeader) char scratch[sizeof(RecordHeader)];
  rocketspeed::Slice headerData;
  if (!file_->Read(sizeof(RecordHeader), &headerData, scratch).ok()) {
    return false;
  }
  if (headerData.size() != sizeof(RecordHeader)) {
    return false;
  }
  offset_ += sizeof(RecordHeader);

  // Copy from scratch to actual header.
  header_ = *reinterpret_cast<const RecordHeader*>(headerData.data());

  // Flag that we haven't read the data.
  have_data_ = false;
  return true;
}

lsn_t LogFile::GetLSN() const {
  return header_.lsn;
}

std::chrono::milliseconds LogFile::GetTimestamp() const {
  return std::chrono::milliseconds(header_.timestamp);
}

Payload LogFile::GetData() {
  // This function lazily reads the data. If we haven't read it yet
  // then we first read it, then return it.
  if (!have_data_) {
    // Resize internal buffer to fit the data.
    data_.resize(header_.datasize);
    rocketspeed::Slice payloadData;

    // Try to read a verify size read.
    if (!file_->Read(header_.datasize, &payloadData, data_.data()).ok()) {
      return Payload(nullptr, 0);
    }
    if (payloadData.size() != header_.datasize) {
      return Payload(nullptr, 0);
    }
    offset_ += header_.datasize;

    // Copy from slice to internal buffer.
    std::copy_n(payloadData.data(), payloadData.size(), data_.data());
    have_data_ = true;
  }
  return Payload(data_.data(), data_.size());
}

lsn_t LastSeqnoWritten(
  const std::string& fname,
  const std::unique_ptr<rocketspeed::RandomRWFile>& file,
  rocketspeed::Env* env) {
  // Get the file size to determine final lsn.
  lsn_t lsn = LSN_OLDEST;
  uint8_t buff[sizeof(lsn_t)];
  uint64_t size;
  if (env->GetFileSize(fname, &size).ok() && size == sizeof(lsn_t)) {
    // Get last seqno.
    rocketspeed::Slice sl(reinterpret_cast<char*>(buff), sizeof(buff));
    if (file->Read(0, sizeof(buff), &sl, reinterpret_cast<char*>(buff)).ok()) {
      lsn = 0;
      for (int i = 0; i < static_cast<int>(sizeof(buff)); ++i) {
        lsn <<= 8;
        lsn |= buff[i];
      }
    }
  }
  return lsn;
}

}  // namespace logdevice
}  // namespace facebook
