//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/controltower/log_tailer.h"
#include "src/util/storage.h"
#include <vector>
#include <inttypes.h>

namespace rocketspeed {

// Version of MessageData that holds a std::unique_ptr<LogRecord> to
// persist the LogDevice buffer.
struct LogRecordMessageData : public MessageData {
 public:
  explicit LogRecordMessageData(std::unique_ptr<LogRecord> record,
                                Status* status)
  : MessageData(MessageType::mDeliver)
  , record_(std::move(record)) {
    // Deserialize
    Slice payload = record_->payload;
    *status = DeSerializeStorage(&payload);
    SetSequenceNumbers(record_->seqno - 1, record_->seqno);
  }

 private:
  std::unique_ptr<LogRecord> record_;
};

LogTailer::LogTailer(std::shared_ptr<LogStorage> storage,
               std::shared_ptr<Logger> info_log) :
  storage_(storage),
  info_log_(info_log) {
}

LogTailer::~LogTailer() {
}

Status LogTailer::Initialize(OnRecordCallback on_record,
                             OnGapCallback on_gap,
                             size_t num_readers) {
  if (reader_.size() != 0) {
    return Status::OK();  // already initialized, nothing more to do
  }

  if (!on_record) {
    return Status::InvalidArgument("on_record must not be null");
  }

  if (!on_gap) {
    return Status::InvalidArgument("on_gap must not be null");
  }

  for (unsigned int reader_id = 0; reader_id < num_readers; ++reader_id) {
    AsyncLogReader* reader = nullptr;
    Status st = CreateReader(reader_id, on_record, on_gap, &reader);
    if (!st.ok()) {
      return st;
    }
    reader_.emplace_back(reader);
  }
  assert(reader_.size() == num_readers);

  // initialize the number of open logs per reader to zero.
  num_open_logs_per_reader_.resize(num_readers, 0);
  return Status::OK();
}

Status LogTailer::CreateReader(size_t reader_id,
                               OnRecordCallback on_record,
                               OnGapCallback on_gap,
                               AsyncLogReader** out) {
  // Define a lambda for callback
  auto record_cb = [this, reader_id, on_record] (
      std::unique_ptr<LogRecord> record) {
    LogID log_id = record->log_id;

    // Convert storage record into RocketSpeed message.
    Status st;
    std::unique_ptr<MessageData> msg(
      new LogRecordMessageData(std::move(record), &st));
    if (!st.ok()) {
      LOG_WARN(info_log_,
        "Failed to deserialize message (%s).",
        st.ToString().c_str());
        info_log_->Flush();
    } else {
      LOG_INFO(info_log_,
        "LogTailer received data (%.16s)@%" PRIu64
        " for Topic(%s) in Log(%" PRIu64 ").",
        msg->GetPayload().ToString().c_str(),
        msg->GetSequenceNumber(),
        msg->GetTopicName().ToString().c_str(),
        log_id);
    }
    assert(st.ok());

    // Invoke message callback.
    on_record(std::move(msg), log_id, reader_id);
  };

  auto gap_cb = [this, reader_id, on_gap] (const GapRecord& record) {
    // Log the gap.
    switch (record.type) {
      case GapType::kDataLoss:
        LOG_WARN(info_log_,
            "Data Loss in Log(%" PRIu64 ") from %" PRIu64 " -%" PRIu64 ".",
            record.log_id, record.from, record.to);
        break;

      case GapType::kRetention:
        LOG_INFO(info_log_,
            "Retention gap in Log(%" PRIu64 ") from %" PRIu64 "-%" PRIu64 ".",
            record.log_id, record.from, record.to);
        break;

      case GapType::kBenign:
        LOG_INFO(info_log_,
            "Benign gap in Log(%" PRIu64 ") from %" PRIu64 "-%" PRIu64 ".",
            record.log_id, record.from, record.to);
        break;
    }

    on_gap(record.log_id, record.type, record.from, record.to, reader_id);
  };

  // Create log reader.
  std::vector<AsyncLogReader*> readers;
  Status st = storage_->CreateAsyncReaders(1, record_cb, gap_cb, &readers);
  if (st.ok()) {
    assert(readers.size() == 1);
    *out = readers[0];
  }
  return st;
}

// Create a new instance of the LogStorage
Status
LogTailer::CreateNewInstance(Env* env,
                          std::shared_ptr<LogStorage> storage,
                          std::shared_ptr<Logger> info_log,
                          LogTailer** tailer) {
  *tailer = new LogTailer(storage, info_log);
  return Status::OK();
}

Status LogTailer::StartReading(LogID logid,
                            SequenceNumber start,
                            size_t reader_id,
                            bool first_open) {
  if (reader_.size() == 0) {
    return Status::NotInitialized();
  }
  AsyncLogReader* r = reader_[reader_id].get();
  Status st = r->Open(logid, start);
  if (st.ok()) {
    LOG_INFO(info_log_,
             "AsyncReader %zu start reading Log(%" PRIu64 ")@%" PRIu64 ".",
             reader_id,
             logid,
             start);
    if (first_open) {
      num_open_logs_per_reader_[reader_id]++;
    }
  } else {
    LOG_WARN(info_log_,
             "AsyncReader %zu failed to start reading Log(%" PRIu64
             ")@%" PRIu64 "(%s).",
             reader_id,
             logid,
             start,
             st.ToString().c_str());
  }
  return st;
}

// Stop reading from this log
Status
LogTailer::StopReading(LogID logid, size_t reader_id) {
  if (reader_.size() == 0) {
    return Status::NotInitialized();
  }
  AsyncLogReader* r = reader_[reader_id].get();
  Status st = r->Close(logid);
  if (st.ok()) {
    LOG_INFO(info_log_,
        "AsyncReader %zu stopped reading Log(%" PRIu64 ").",
        reader_id, logid);
    num_open_logs_per_reader_[reader_id]--;
  } else {
    LOG_WARN(info_log_,
        "AsyncReader %zu failed to stop reading Log(%" PRIu64 ") (%s).",
        reader_id, logid, st.ToString().c_str());
  }
  return st;
}

// find latest seqno then invoke callback
Status
LogTailer::FindLatestSeqno(
  LogID logid,
  std::function<void(Status, SequenceNumber)> callback) {
  // LogDevice treats std::chrono::milliseconds::max() specially, avoiding
  // a binary search and just returning the next LSN.
  return storage_->FindTimeAsync(logid,
                                 std::chrono::milliseconds::max(),
                                 callback);
}

// This traverses the reader array without any locks but that is
// ok because it is used only by unit tests when there is no
// tailer activity.
int
LogTailer::NumberOpenLogs() const {
  int count = 0;
  for (size_t i = 0; i < num_open_logs_per_reader_.size(); i++) {
    count += num_open_logs_per_reader_[i];
  }
  return count;
}

}  // namespace rocketspeed