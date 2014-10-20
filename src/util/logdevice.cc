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
  switch (error) {
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
      return Status::InternalError("LogDevice UNKNOWN " +
          std::to_string(static_cast<uint16_t>(error)));
  }
}

static LogID CastLogID(facebook::logdevice::logid_t logid) {
  return static_cast<LogID>(static_cast<int64_t>(logid));
}

Status LogDeviceStorage::Create(
  std::string cluster_name,
  std::string config_url,
  std::string credentials,
  std::chrono::milliseconds timeout,
  std::unique_ptr<facebook::logdevice::ClientSettings>&& settings,
  Env* env,
  LogDeviceStorage** storage) {
#ifdef USE_LOGDEVICE
  // Basic validation of parameters before sending to LogDevice.
  if (cluster_name.empty()) {
    return Status::InvalidArgument("cluster_name must not be empty.");
  }
  if (config_url.empty()) {
    return Status::InvalidArgument("config_url must not be empty.");
  }
#endif
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

Status LogDeviceStorage::Create(
  std::shared_ptr<facebook::logdevice::Client> client,
  Env* env,
  LogDeviceStorage** storage) {
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

Status LogDeviceStorage::AppendAsync(LogID id,
                                     const Slice& data,
                                     AppendCallback callback) {
  // Check data isn't over the LogDevice maximum payload size.
  const size_t maxSize = facebook::logdevice::Payload::maxSize();
  if (data.size() >= maxSize) {
    return Status::InvalidArgument("Payload is over LogDevice limit of 1MB.");
  }

  // Create a logdevice compatible callback.
  // TODO(pja) 1 : This might allocate once converted to an std::function
  auto logdevice_callback =
    [callback] (facebook::logdevice::Status st,
                const facebook::logdevice::DataRecord& r) {
    callback(LogDeviceErrorToStatus(st));
  };

  // Asynchronously append the data.
  facebook::logdevice::Payload payload(
    reinterpret_cast<const void*>(data.data()),
    data.size());
  int result = client_->append(facebook::logdevice::logid_t(id),
                               payload,
                               logdevice_callback);

  // Check for errors
  if (result != 0) {
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
  // findTimeSync returns the *next* LSN after that time, so we trim up to
  // (lsn - 1), otherwise this will trim the next message to be issued also.
  int result = client_->trim(facebook::logdevice::logid_t(id), lsn - 1);
  if (result != 0) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }
  return Status::OK();
}

Status LogDeviceStorage::FindTimeAsync(
    LogID id,
    std::chrono::milliseconds timestamp,
    std::function<void(Status, SequenceNumber)> callback) {
  // Construct callback adapted for the logdevice interface.
  auto adapted_callback = [callback] (facebook::logdevice::Status err,
                                      facebook::logdevice::lsn_t lsn) {
    callback(LogDeviceErrorToStatus(err), SequenceNumber(lsn));
  };

  // Call into LogDevice.
  int result = client_->findTime(facebook::logdevice::logid_t(id),
                                 timestamp,
                                 adapted_callback);
  if (result) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  } else {
    return Status::OK();
  }
}

Status
LogDeviceStorage::CreateAsyncReaders(
  unsigned int parallelism,
  std::function<void(const LogRecord&)> record_cb,
  std::function<void(const GapRecord&)> gap_cb,
  std::vector<AsyncLogReader*>* readers) {
  // Validate
  if (!readers) {
    return Status::InvalidArgument("readers must not be null.");
  }

  // Construct all the readers.
  readers->reserve(parallelism);
  while (parallelism--) {
    auto reader = new AsyncLogDeviceReader(this,
                                           record_cb,
                                           gap_cb,
                                           client_->createAsyncReader());
    readers->push_back(reader);
  }

  return Status::OK();
}

AsyncLogDeviceReader::AsyncLogDeviceReader(
  LogDeviceStorage* storage,
  std::function<void(const LogRecord&)> record_cb,
  std::function<void(const GapRecord&)> gap_cb,
  std::unique_ptr<facebook::logdevice::AsyncReader>&& reader)
: storage_(*storage)
, reader_(std::move(reader)) {
  // Setup LogDevice AsyncReader callbacks
  reader_->setRecordCallback(
    [this, record_cb] (const facebook::logdevice::DataRecord& data) {
      // Convert DataRecord to our LogRecord format.
      const LogRecord record(
              CastLogID(data.logid),
              Slice((const char*)data.payload.data, data.payload.size),
              data.attrs.lsn,
              std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::milliseconds(data.attrs.timestamp)));

      record_cb(record);
    });

  reader_->setGapCallback(
    [this, gap_cb] (const facebook::logdevice::GapRecord& gap) {
      // Convert GapRecord to our GapRecord format.
      GapRecord record { GapType::kBenign,
                         CastLogID(gap.logid),
                         gap.lo,
                         gap.hi };
      switch (gap.type) {
        case facebook::logdevice::GapType::UNKNOWN:
        case facebook::logdevice::GapType::BRIDGE:
        case facebook::logdevice::GapType::HOLE:
          record.type = GapType::kBenign;
          break;

        case facebook::logdevice::GapType::DATALOSS:
          record.type = GapType::kDataLoss;
          break;

        case facebook::logdevice::GapType::TRIM:
          record.type = GapType::kRetention;
          break;
      }

      gap_cb(record);
    });
}

AsyncLogDeviceReader::~AsyncLogDeviceReader() {
}

Status AsyncLogDeviceReader::Open(LogID id,
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

Status AsyncLogDeviceReader::Close(LogID id) {
  // Simple forward to LogDevice
  if (reader_->stopReading(facebook::logdevice::logid_t(id))) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }
  return Status::OK();
}

}  // namespace rocketspeed
