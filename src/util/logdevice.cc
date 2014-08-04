// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/logdevice.h"
#include <algorithm>

namespace rocketspeed {

/**
 * Converts a LogDevice Status to a RocketSpeed::Status.
 * The mapping isn't one-to-one, so some information is lost.
 */
static Status LogDeviceErrorToStatus(facebook::logdevice::Status error) {
  switch (facebook::logdevice::err) {
    case facebook::logdevice::E::OK:
      return Status::OK();

    case facebook::logdevice::E::ACCESS:
      return Status::Unauthorized("LogDevice ACCESS");

    case facebook::logdevice::E::CONNFAILED:
      return Status::IOError("LogDevice CONNFAILED");

    case facebook::logdevice::E::FAILED:
      return Status::IOError("LogDevice FAILED");

    case facebook::logdevice::E::FILE_OPEN:
      return Status::IOError("LogDevice FILE_OPEN");

    case facebook::logdevice::E::FILE_READ:
      return Status::IOError("LogDevice FILE_READ");

    case facebook::logdevice::E::INVALID_CONFIG:
      return Status::IOError("LogDevice INVALID_CONFIG");

    case facebook::logdevice::E::PREEMPTED:
      return Status::IOError("LogDevice PREEMPTED");

    case facebook::logdevice::E::SYSLIMIT:
      return Status::IOError("LogDevice SYSLIMIT");

    case facebook::logdevice::E::INTERNAL:
      return Status::InternalError("LogDevice INTERNAL");

    case facebook::logdevice::E::NOBUFS:
      return Status::InternalError("LogDevice NOBUFS");

    case facebook::logdevice::E::INVALID_PARAM:
      return Status::InvalidArgument("LogDevice INVALID_PARAM");

    case facebook::logdevice::E::TOOBIG:
      return Status::InvalidArgument("LogDevice TOOBIG");

    case facebook::logdevice::E::TOOMANY:
      return Status::InvalidArgument("LogDevice TOOMANY");

    case facebook::logdevice::E::NOSEQUENCER:
      return Status::NotInitialized();

    case facebook::logdevice::E::TIMEDOUT:
      return Status::TimedOut();

    default:
      return Status::InternalError("LogDevice UNKNOWN");
  }
}

Status LogDeviceStorage::Create(
  std::string cluster_name,
  std::string config_url,
  std::string credentials,
  std::chrono::milliseconds timeout,
  std::unique_ptr<facebook::logdevice::ClientSettings>&& settings,
  Env* env,
  LogDeviceStorage** storage) {
  // Basic validation of parameters before sending to LogDevice.
  if (cluster_name.empty()) {
    return Status::InvalidArgument("cluster_name must not be empty.");
  }
  if (config_url.empty()) {
    return Status::InvalidArgument("config_url must not be empty.");
  }
  if (storage == nullptr) {
    return Status::InvalidArgument("Must provide the storage pointer.");
  }

  // Attempt to create internal LogDevice Client.
  // Returns null on error.
  auto client = facebook::logdevice::Client::create(
    cluster_name,
    config_url,
    credentials,
    timeout,
    std::move(settings));

  if (!client) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }

  // Successful, write out to the output parameter and return OK.
  *storage = new LogDeviceStorage(client, env);
  return Status::OK();
}

LogDeviceStorage::LogDeviceStorage(
  std::shared_ptr<facebook::logdevice::Client> client,
  Env* env)
: client_(client)
, env_(env) {
}

Status LogDeviceStorage::Append(LogID id,
                                const Slice& data) {
  // Check data isn't over the LogDevice maximum payload size.
  const size_t maxSize = facebook::logdevice::Payload::maxSize();
  if (data.size() >= maxSize) {
    return Status::InvalidArgument("Payload is over LogDevice limit of 1MB.");
  }

  // Synchronously append the data.
  facebook::logdevice::Payload payload(
    reinterpret_cast<const void*>(data.data()),
    data.size());
  const auto lsn = client_->appendSync(facebook::logdevice::logid_t(id),
                                       payload);

  // Check for errors
  if (lsn == facebook::logdevice::LSN_INVALID) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }
  return Status::OK();
}

Status LogDeviceStorage::Trim(LogID id,
                              std::chrono::microseconds age) {
  // Compute the time in milliseconds using current time subtract age.
  auto now = std::chrono::microseconds(env_->NowMicros());
  auto threshold = now - age;
  auto thresholdMs = std::chrono::duration_cast<std::chrono::milliseconds>(
    threshold);

  // Find the LSN for that time.
  facebook::logdevice::Status findTimeStatus;
  auto lsn = client_->findTimeSync(facebook::logdevice::logid_t(id),
                                   thresholdMs,
                                   &findTimeStatus);
  if (lsn == facebook::logdevice::LSN_INVALID) {
    return LogDeviceErrorToStatus(findTimeStatus);
  }
  // NOTE: findTimeStatus may be E::PARTIAL at this point, which means the LSN
  // may be incorrect. However, LogDevice guarantees that it will not be *later*
  // than the correct LSN, so it is good enough for RocketSpeed as a
  // conservative result.

  // Now do the trim.
  int result = client_->trim(facebook::logdevice::logid_t(id), lsn);
  if (result != 0) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }
  return Status::OK();
}

Status LogDeviceStorage::CreateReaders(unsigned int maxLogsPerReader,
                                       unsigned int parallelism,
                                       std::vector<LogReader*>* readers) {
  // Validate
  if (!readers) {
    return Status::InvalidArgument("readers must not be null.");
  }

  // Construct all the readers.
  readers->reserve(parallelism);
  while (parallelism--) {
    auto reader = new LogDeviceReader(this,
                                      maxLogsPerReader,
                                      client_->createAsyncReader());
    readers->push_back(reader);
  }

  return Status::OK();
}

LogDeviceReader::LogDeviceReader(
  LogDeviceStorage* storage,
  unsigned int maxLogs,
  std::unique_ptr<facebook::logdevice::AsyncReader>&& reader)
: storage_(*storage)
, maxLogs_(maxLogs)
, reader_(std::move(reader)) {
  // Setup LogDevice AsyncReader callbacks
  reader_->setRecordCallback(
    [this] (const facebook::logdevice::DataRecord& data) {
      // Make copy of payload buffer as it won't be around after this callback.
      std::unique_ptr<char[]> buffer;
      buffer.reset(new char[data.payload.size]);
      memcpy(reinterpret_cast<void*>(buffer.get()),
             data.payload.data,
             data.payload.size);

      // Convert DataRecord to our LogRecord format.
      LogRecord record;
      record.logID = static_cast<LogID>(static_cast<int64_t>(data.logid));
      record.payload = Slice(buffer.get(), data.payload.size);
      record.sequenceNumber = data.attrs.lsn;
      record.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::milliseconds(data.attrs.timestamp));

      // Add to pending_ queue.
      // TODO(pja): use lock-free queue.
      std::lock_guard<std::mutex> lock(mutex_);
      buffers_.push_back(std::move(buffer));
      pending_.push_back(record);

      // Let our selector know about the new data.
      if (selector_) {
        selector_->Notify();
      }
    });

  reader_->setGapCallback(
    [this] (const facebook::logdevice::GapRecord& gap) {
      // TODO(pja): handle gaps
    });
}

LogDeviceReader::~LogDeviceReader() {
  if (selector_) {
    assert(selector_->Deregister(this).ok());
  }
}

Status LogDeviceReader::Open(LogID id,
                             SequenceNumber startPoint,
                             SequenceNumber endPoint) {
  // Handle beginning/end magic values.
  if (startPoint == SequencePoint::kBeginningOfTimeSeqno) {
    startPoint = facebook::logdevice::LSN_OLDEST;
  }
  if (endPoint == SequencePoint::kEndOfTimeSeqno) {
    endPoint = facebook::logdevice::LSN_MAX;
  }

  // Start reading from the log.
  if (reader_->startReading(facebook::logdevice::logid_t(id),
                            startPoint,
                            endPoint)) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }
  return Status::OK();
}

Status LogDeviceReader::Close(LogID id) {
  // Simple forward to LogDevice
  if (reader_->stopReading(facebook::logdevice::logid_t(id))) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }

  // Ensure that any pending records for this log are removed.
  std::lock_guard<std::mutex> lock(mutex_);
  auto pendingIt = pending_.begin();
  auto buffersIt = buffers_.begin();
  while (pendingIt != pending_.end()) {
    if (pendingIt->logID == id) {
      pendingIt = pending_.erase(pendingIt);
      buffersIt = buffers_.erase(buffersIt);
    } else {
      ++pendingIt;
      ++buffersIt;
    }
  }
  return Status::OK();
}

Status LogDeviceReader::Read(std::vector<LogRecord>* records,
                             size_t maxRecords) {
  if (!records) {
    return Status::InvalidArgument("Must provide records pointer.");
  }
  records->clear();

  std::lock_guard<std::mutex> lock(mutex_);

  // buffers_ may still contain some payloads from a previous Read call, so we
  // should clear those now. However, buffers_ may also contain some pending
  // payloads for this Read so we need to keep those intact.
  assert(pending_.size() <= buffers_.size());  // More records than payloads?
  size_t excess = buffers_.size() - pending_.size();
  buffers_.erase(buffers_.begin(), buffers_.begin() + excess);

  // And copy them out to *records, then remove from pending queue.
  size_t numRecords = std::min(maxRecords, pending_.size());
  std::move(pending_.begin(),
            pending_.begin() + numRecords,
            std::back_inserter(*records));
  pending_.erase(pending_.begin(), pending_.begin() + numRecords);
  return Status::OK();
}

bool LogDeviceReader::HasData() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return !pending_.empty();
}

void LogDeviceReader::SetSelector(LogDeviceSelector* selector) {
  std::lock_guard<std::mutex> lock(mutex_);
  assert(selector == nullptr ||
         selector_ == nullptr);  // Already assigned to a selector!
  selector_ = selector;
}

LogDeviceSelector::LogDeviceSelector() {
}

LogDeviceSelector::~LogDeviceSelector() {
  for (auto reader : readers_) {
    static_cast<LogDeviceReader*>(reader)->SetSelector(nullptr);
  }
}

Status LogDeviceSelector::Create(LogDeviceSelector** selector) {
  if (!selector) {
    return Status::InvalidArgument("Must provide selector pointer.");
  }
  *selector = new LogDeviceSelector();
  return Status::OK();
}

Status LogDeviceSelector::Register(LogReader* reader) {
  if (!reader) {
    return Status::InvalidArgument("Cannot provide null reader.");
  }

  if (std::find(readers_.begin(), readers_.end(), reader) != readers_.end()) {
    // We have already registered this reader.
    return Status::InvalidArgument("This reader is already registered.");
  }

  readers_.push_back(static_cast<LogDeviceReader*>(reader));
  static_cast<LogDeviceReader*>(reader)->SetSelector(this);
  return Status::OK();
}

Status LogDeviceSelector::Deregister(LogReader* reader) {
  if (!reader) {
    return Status::InvalidArgument("Cannot provide null reader.");
  }

  auto it = std::find(readers_.begin(), readers_.end(), reader);
  if (it == readers_.end()) {
    // This reader isn't registered
    return Status::InvalidArgument("Reader is not registered.");
  }

  readers_.erase(it);
  static_cast<LogDeviceReader*>(reader)->SetSelector(nullptr);
  return Status::OK();
}

Status LogDeviceSelector::Select(std::vector<LogReader*>* selected,
                                 std::chrono::microseconds timeout) {
  // Validate
  if (!selected) {
    return Status::InvalidArgument("Must provide selected pointer.");
  }

  // Wait for a reader to say we have data.
  {
    std::unique_lock<std::mutex> lock(mutex_);
    monitor_.wait_for(lock, timeout);
  }

  // Three things could have happened:
  //  1) a reader has said we have data
  //  2) timeout occurred
  //  3) "spurious wakeup"
  // In all cases, we'll just check for data on all readers.
  std::copy_if(readers_.begin(),
               readers_.end(),
               std::back_inserter(*selected),
               [](const LogDeviceReader* reader) {
                 return reader->HasData();
               });

  if (selected->empty()) {
    // If selected is empty then we must have timed out, or had a spurious
    // wakeup (which we'll just treat as time out).
    return Status::TimedOut();
  }
  return Status::OK();
}

void LogDeviceSelector::Notify() {
  monitor_.notify_one();
}

}  // namespace rocketspeed
