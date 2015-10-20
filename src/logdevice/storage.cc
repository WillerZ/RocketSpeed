// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/logdevice/storage.h"
#include "include/Logger.h"
#include <algorithm>
#include <thread>
#include <unordered_set>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow"
#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#ifdef USE_LOGDEVICE
#include "logdevice/common/DataRecordOwnsPayload.h"
#endif  // USE_LOGDEVICE
#pragma GCC diagnostic pop

namespace rocketspeed {

/**
 * Async Log Reader interface backed by LogDevice.
 */
class AsyncLogDeviceReader : public AsyncLogReader {
 public:
  AsyncLogDeviceReader(LogDeviceStorage* storage,
                    std::function<bool(LogRecord&)> record_cb,
                    std::function<bool(const GapRecord&)> gap_cb,
                    std::unique_ptr<facebook::logdevice::AsyncReader>&& reader);

  ~AsyncLogDeviceReader() final;

  Status Open(LogID id,
              SequenceNumber startPoint,
              SequenceNumber endPoint) final;

  Status Close(LogID id) final;

 private:
  std::shared_ptr<facebook::logdevice::AsyncReader> reader_;
  std::unordered_set<LogID> open_logs_;
};

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

    case facebook::logdevice::E::PARTIAL:
      return Status::IOError("LogDevice PARTIAL");

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
  return static_cast<LogID>(static_cast<uint64_t>(logid));
}

Status LogDeviceStorage::Create(
  std::string cluster_name,
  std::string config_url,
  std::string credentials,
  std::chrono::milliseconds timeout,
  int num_workers,
  size_t max_payload_size,
  std::string ssl_boundary,
  std::string my_location,
  Env* env,
  std::shared_ptr<Logger> info_log,
  LogDeviceStorage** storage) {
#ifdef USE_LOGDEVICE
  // Basic validation of parameters before sending to LogDevice.
  if (cluster_name.empty()) {
    return Status::InvalidArgument("cluster_name must not be empty.");
  }
  if (config_url.empty()) {
    return Status::InvalidArgument("config_url must not be empty.");
  }
#endif  // USE_LOGDEVICE
  if (storage == nullptr) {
    return Status::InvalidArgument("Must provide the storage pointer.");
  }

  // Create client settings.
  std::unique_ptr<facebook::logdevice::ClientSettings> settings(
    facebook::logdevice::ClientSettings::create());
  settings->set("num-workers", num_workers);
  settings->set("max-payload-size", max_payload_size);
  settings->set("ssl-boundary", ssl_boundary.c_str());
  settings->set("my-location", my_location.c_str());

  // These parameters control the exponential back-off times used for retries
  // when we initiate backpressure on the LogDevice client.
  settings->set("client-initial-redelivery-delay", "10ms");
  settings->set("client-max-redelivery-delay", "1000ms");

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
  *storage = new LogDeviceStorage(client, env, std::move(info_log));
  return Status::OK();
}

Status LogDeviceStorage::Create(
  std::shared_ptr<facebook::logdevice::Client> client,
  Env* env,
  std::shared_ptr<Logger> info_log,
  LogDeviceStorage** storage) {
  *storage = new LogDeviceStorage(client, env, std::move(info_log));
  return Status::OK();
}

LogDeviceStorage::LogDeviceStorage(
  std::shared_ptr<facebook::logdevice::Client> client,
  Env* env,
  std::shared_ptr<Logger> info_log)
: client_(client)
, info_log_(std::move(info_log)) {
}

LogDeviceStorage::~LogDeviceStorage() {
  // Ensure that we hold the only reference.
  // Graceful shutdown relies on this.
  // The LogDevice AsyncReaderImpl holds a reference to the LogDevice client,
  // keeping it alive, and keeping its loops running. We need to wait until
  // all of those references are released.
  //
  // To make things more difficult, the AsyncReader cannot be deleted until
  // all logs have asynchonously closed, so in AsyncLogDeviceReader::Close we
  // keep a reference to the AsyncReader to keep it alive, but this also keeps
  // the Client alive. We cannot let this reference free the Client because the
  // Client must be freed from a single thread, so we have no choice but to
  // wait here until all references are released.
  //
  // We wait for a maximum of 10 seconds before giving up.

  const std::chrono::milliseconds wait(10);
  const std::chrono::milliseconds timeout(10000);
  std::chrono::milliseconds tick(0);
  while (!client_.unique()) {
    /* sleep override */
    std::this_thread::sleep_for(wait);
    tick += wait;
    if (tick > timeout) {
      LOG_ERROR(info_log_, "Failed to release LogDevice client after %dms. "
        "Pushing forward with shutdown to avoid blocking the thread, although "
        "bad things may happen after this point.",
        static_cast<int>(timeout.count()));
      assert(false);
      break;
    }
  }
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
    callback(LogDeviceErrorToStatus(st), r.attrs.lsn);
  };

  // Asynchronously append the data.
  facebook::logdevice::Payload payload(
    reinterpret_cast<const void*>(data.data()),
    data.size());
  int result = client_->append(facebook::logdevice::logid_t(id),
                               payload,
                               std::move(logdevice_callback));

  // Check for errors
  if (result != 0) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }
  return Status::OK();
}

Status LogDeviceStorage::Trim(LogID id,
                              SequenceNumber seqno) {
  int result = client_->trimSync(facebook::logdevice::logid_t(id), seqno);
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
    if (err == facebook::logdevice::E::PARTIAL) {
      // Change error to E::OK.
      // E::PARTIAL means that not all storage nodes responded, but we still
      // have enough information for a conservative sequence number estimate.
      // findTime is always an approximation anyway, since it uses wall time.
      err = facebook::logdevice::E::OK;
    }
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
  std::function<bool(LogRecord&)> record_cb,
  std::function<bool(const GapRecord&)> gap_cb,
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
  std::function<bool(LogRecord&)> record_cb,
  std::function<bool(const GapRecord&)> gap_cb,
  std::unique_ptr<facebook::logdevice::AsyncReader>&& reader)
: reader_(std::move(reader)) {
  // Setup LogDevice AsyncReader callbacks
  reader_->setRecordCallback([this, record_cb](
      std::unique_ptr<facebook::logdevice::DataRecord>& data) {
    // Create an empty DataRecord, in case the callback returns false and the
    // LogRecord's context is stolen.
    std::unique_ptr<facebook::logdevice::DataRecord> empty_record;
#ifdef USE_LOGDEVICE
    // The clowniness with lazy construction of DataRecordOwnsPayload aims at
    // removing allocation from hot path.
    using facebook::logdevice::DataRecordOwnsPayload;
    // This clowniness is necessary, because LogDevice static_casts record to
    // DataRecordOwnsPayload internally for whatever reason.
    auto actual_data = static_cast<const DataRecordOwnsPayload*>(data.get());
    const auto ld_logid = data->logid;
    const auto ld_lsn = data->attrs.lsn;
    const auto ld_timestamp = data->attrs.timestamp;
    const auto ld_flags = actual_data->flags_;
#endif  // USE_LOGDEVICE
    // Backoff is not implemented in mock LogDevice, therefore we can leave the
    // record empty.

    // Convert DataRecord to our LogRecord format.
    LogRecord record;
    record.log_id = CastLogID(data->logid);
    record.payload = Slice((const char*)data->payload.data, data->payload.size);
    record.seqno = data->attrs.lsn;
    record.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::milliseconds(data->attrs.timestamp));
    record.context = EraseType(std::move(data));

    bool success = record_cb(record);
    if (!success) {
      // Record callback is free to move out the record.
      if (record.context) {
        // Put back the original record, since LogDevice will be expecting it
        // there.
        void* context = record.context.release();
        data.reset(static_cast<facebook::logdevice::DataRecord*>(context));
      } else {
#ifdef USE_LOGDEVICE
        // Put back an empty record instead.
        data.reset(new DataRecordOwnsPayload(ld_logid,
                                             facebook::logdevice::Payload(),
                                             ld_lsn,
                                             ld_timestamp,
                                             ld_flags));
#endif  // USE_LOGDEVICE
      }
    }
    return success;
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

        case facebook::logdevice::GapType::MAX:
          assert(false);
          std::abort();
          break;
      }

      return gap_cb(record);
    });

  // LogDevice trims record non-deterministically across storage nodes, so
  // the logs are heavily fragmented with gaps in the grey area where some
  // nodes have trimmed a record, and others haven't. This tells the reader to
  // eagerly use the highest trim point of any node.
  reader_->skipPartiallyTrimmedSections();
}

AsyncLogDeviceReader::~AsyncLogDeviceReader() {
  // Create copy of open_logs_, since it is mutated inside Close.
  std::vector<LogID> open(open_logs_.begin(), open_logs_.end());
  for (LogID log_id : open) {
    // This close may fail, but that's ok since LogDevice will clean up
    // any open logs on destruction. The only issue is that this is done in
    // a blocking way, so we hope it closes successfully for performance.
    Close(log_id);
  }
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
  open_logs_.emplace(id);
  return Status::OK();
}

Status AsyncLogDeviceReader::Close(LogID id) {
  // Simple forward to LogDevice
  // Close lambda captures the copy of AsyncReader to ensure that the reader
  // is still around until the log is fully closed.
  std::shared_ptr<facebook::logdevice::AsyncReader> copy = reader_;
  if (reader_->stopReading(facebook::logdevice::logid_t(id), [copy] () {})) {
    return LogDeviceErrorToStatus(facebook::logdevice::err);
  }
  open_logs_.erase(id);
  return Status::OK();
}

}  // namespace rocketspeed
