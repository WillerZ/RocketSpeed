//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/controltower/log_tailer.h"
#include "src/messages/event_loop.h"
#include "src/messages/messages.h"
#include "src/messages/queues.h"
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/storage.h"
#include <vector>
#include <inttypes.h>

namespace rocketspeed {

// Version of MessageData that holds a std::unique_ptr<LogRecord> to
// persist the LogDevice buffer.
struct LogRecordMessageData : public MessageData {
 public:
  explicit LogRecordMessageData(LogRecord record,
                                Status* status)
  : MessageData(MessageType::mDeliver)
  , record_(std::move(record)) {
    // Deserialize
    Slice payload = record_.payload;
    *status = DeSerializeStorage(&payload);
    SetSequenceNumbers(record_.seqno - 1, record_.seqno);
  }

  LogRecord MoveRecord() {
    return std::move(record_);
  }

 private:
  LogRecord record_;
};

LogTailer::LogTailer(std::shared_ptr<LogStorage> storage,
                     std::shared_ptr<Logger> info_log,
                     EventLoop* event_loop,
                     ControlTowerOptions::LogTailer options) :
  storage_(storage),
  info_log_(info_log),
  options_(std::move(options)),
  event_loop_(event_loop),
  restart_events_(options_.min_reader_restart_duration,
                  options_.max_reader_restart_duration) {

  storage_to_room_queues_.reset(
    new ThreadLocalQueues<std::function<void(Flow*)>>(
      [this] () {
        return InstallQueue<std::function<void(Flow*)>>(
          event_loop_,
          info_log_,
          event_loop_->GetQueueStats(),
          options_.storage_to_room_queue_size,
          [] (Flow* flow, std::function<void(Flow*)> fn) {
            fn(flow);
          });
      }));
}

LogTailer::~LogTailer() {
  RS_ASSERT(!storage_);  // Must call Stop() before deleting.
}

Status LogTailer::Initialize(OnRecordCallback on_record,
                             OnGapCallback on_gap,
                             size_t num_readers) {
  if (readers_.size() != 0) {
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
    Status st = CreateReader(reader_id, &reader);
    if (!st.ok()) {
      return st;
    }
    readers_.emplace_back(std::unique_ptr<AsyncLogReader>(reader),
                          on_record,
                          on_gap);
  }
  RS_ASSERT(readers_.size() == num_readers);

  return Status::OK();
}

void LogTailer::Stop() {
  readers_.clear();
  storage_.reset();
}

void LogTailer::RecordCallback(Flow* flow,
                               std::unique_ptr<MessageData>& msg,
                               LogID log_id,
                               size_t reader_id) {
  SequenceNumber seqno = msg->GetSequenceNumber();
  RS_ASSERT(reader_id < readers_.size());
  Reader& reader = readers_[reader_id];
  auto it = reader.log_state.find(log_id);
  if (it == reader.log_state.end()) {
    // Log not open, ignore.
    // This can happen due to asynchrony after closing log.
    LOG_DEBUG(info_log_,
      "Reader(%zu) received record on unopened Log(%" PRIu64 ")",
      reader_id, log_id);
    stats_.log_records_out_of_order->Add(1);
    return;
  }
  if (it->second.next_seqno != seqno) {
    // Log not at this sequence number, ignore.
    // This can happen due to asynchrony after closing log.
    LOG_DEBUG(info_log_,
      "Reader(%zu) received record out of order on Log(%" PRIu64 ")."
      " Expected:%" PRIu64 " Received:%" PRIu64,
      reader_id,
      log_id,
      it->second.next_seqno,
      seqno);
    stats_.log_records_out_of_order->Add(1);
    return;
  }
  // Now expecting next seqno.
  it->second.next_seqno = seqno + 1;

  reader.on_record(flow, msg, log_id, reader_id);
}

void LogTailer::GapCallback(Flow* flow,
                            LogID log_id,
                            GapType gap_type,
                            SequenceNumber from,
                            SequenceNumber to,
                            size_t reader_id) {
  // Log the gap.
  switch (gap_type) {
    case GapType::kDataLoss:
      LOG_WARN(info_log_,
          "Data Loss in Log(%" PRIu64 ") from %" PRIu64 " -%" PRIu64 ".",
          log_id, from, to);
      break;

    case GapType::kRetention:
      LOG_WARN(info_log_,
          "Retention gap in Log(%" PRIu64 ") from %" PRIu64 "-%" PRIu64 ".",
          log_id, from, to);
      break;

    case GapType::kBenign:
      LOG_INFO(info_log_,
          "Benign gap in Log(%" PRIu64 ") from %" PRIu64 "-%" PRIu64 ".",
          log_id, from, to);
      break;
  }

  RS_ASSERT(reader_id < readers_.size());
  Reader& reader = readers_[reader_id];
  auto it = reader.log_state.find(log_id);
  if (it == reader.log_state.end()) {
    // Log not open, ignore.
    // This can happen due to asynchrony after closing log.
    LOG_DEBUG(info_log_,
      "Reader(%zu) received gap on unopened Log(%" PRIu64 ")",
      reader_id, log_id);
    stats_.gap_records_out_of_order->Add(1);
    return;
  }
  if (it->second.next_seqno != from) {
    // Log not at this sequence number, ignore.
    // This can happen due to asynchrony after closing log.
    LOG_DEBUG(info_log_,
      "Reader(%zu) received gap out of order on Log(%" PRIu64 ")."
      " Expected:%" PRIu64 " Received:%" PRIu64,
      reader_id,
      log_id,
      it->second.next_seqno,
      from);
    stats_.gap_records_out_of_order->Add(1);
    return;
  }
  // Now expecting next seqno.
  it->second.next_seqno = to + 1;

  reader.on_gap(flow, log_id, gap_type, from, to, reader_id);
}

Status LogTailer::CreateReader(size_t reader_id, AsyncLogReader** out) {
  // Define a lambda for callback
  auto record_cb = [this, reader_id]
      (LogRecord& record) mutable {
    // Convert storage record into RocketSpeed message.
    LogID log_id = record.log_id;
    SequenceNumber seqno = record.seqno;
    Status st;
    std::unique_ptr<MessageData> msg(
      new LogRecordMessageData(std::move(record), &st));

    bool success;
    if (!st.ok()) {
      LOG_ERROR(info_log_,
        "Failed to deserialize message in Log(%" PRIu64 ")@%" PRIu64 ": %s",
        log_id,
        seqno,
        st.ToString().c_str());

      // Forward to LogTailer thread.
      success = TryForward(
        [this, log_id, seqno, reader_id] (Flow* flow) {
          // Treat corrupt data as data loss.
          GapCallback(
            flow, log_id, GapType::kDataLoss, seqno, seqno, reader_id);
        });
    } else {
      LOG_DEBUG(info_log_,
        "LogTailer received data (%.16s)@%" PRIu64
        " for Topic(%s,%s) in Log(%" PRIu64 ").",
        msg->GetPayload().ToString().c_str(),
        seqno,
        msg->GetNamespaceId().ToString().c_str(),
        msg->GetTopicName().ToString().c_str(),
        log_id);

      // Forward to LogTailer thread.
      auto msg_raw = msg.release();
      success = TryForward(
        [this, msg_raw, log_id, reader_id] (Flow* flow) mutable {
          std::unique_ptr<MessageData> msg_owned(msg_raw);
          RecordCallback(flow, msg_owned, log_id, reader_id);
        });
      if (!success) {
        msg.reset(msg_raw);
      }
    }

    if (!success) {
      // Need to put the record back so that the storage layer can retry.
      RS_ASSERT(msg);
      record = static_cast<LogRecordMessageData*>(msg.get())->MoveRecord();
    }
    return success;
  };

  auto gap_cb = [this, reader_id]
      (const GapRecord& record) {
    // Extract parameters.
    const LogID log_id = record.log_id;
    const SequenceNumber from = record.from;
    const SequenceNumber to = record.to;
    const GapType type = record.type;

    // Forward to LogTailer thread.
    return TryForward(
      [this, log_id, from, to, type, reader_id] (Flow* flow) {
        GapCallback(flow, log_id, type, from, to, reader_id);
      });
  };

  // Create log reader.
  std::vector<AsyncLogReader*> readers;
  RS_ASSERT(storage_);
  Status st = storage_->CreateAsyncReaders(1, record_cb, gap_cb, &readers);
  if (st.ok()) {
    RS_ASSERT(readers.size() == 1);
    *out = readers[0];
  }
  return st;
}

// Create a new instance of the LogStorage
Status
LogTailer::CreateNewInstance(Env* env,
                             std::shared_ptr<LogStorage> storage,
                             std::shared_ptr<Logger> info_log,
                             EventLoop* event_loop,
                             ControlTowerOptions::LogTailer options,
                             LogTailer** tailer) {
  *tailer = new LogTailer(storage, info_log, event_loop, std::move(options));
  return Status::OK();
}

Status LogTailer::StartReading(LogID logid,
                            SequenceNumber start,
                            size_t reader_id) {
  if (readers_.size() == 0) {
    return Status::NotInitialized();
  }
  RS_ASSERT(reader_id < readers_.size());
  Reader& reader = readers_[reader_id];
  Status st = reader.log_reader->Open(logid, start);
  if (st.ok()) {
    LOG_INFO(info_log_,
             "AsyncReader %zu start reading Log(%" PRIu64 ")@%" PRIu64 ".",
             reader_id,
             logid,
             start);
    auto it = reader.log_state.find(logid);
    if (it == reader.log_state.end()) {
      it = reader.log_state.emplace(logid, Reader::LogState(start)).first;
      stats_.readers_started->Add(1);
    } else {
      it->second.next_seqno = start;
      stats_.readers_restarted->Add(1);
      restart_events_.RemoveEvent(it->second.restart_event_handle);
    }
    it->second.restart_event_handle =
      restart_events_.AddEvent(reader_id, logid);
  } else {
    LOG_ERROR(info_log_,
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
  if (readers_.size() == 0) {
    return Status::NotInitialized();
  }
  Reader& reader = readers_[reader_id];
  Status st;
  auto it = reader.log_state.find(logid);
  if (it != reader.log_state.end()) {
    restart_events_.RemoveEvent(it->second.restart_event_handle);
    reader.log_state.erase(it);
    stats_.readers_stopped->Add(1);
    st = reader.log_reader->Close(logid);
    if (st.ok()) {
      LOG_INFO(info_log_,
          "AsyncReader %zu stopped reading Log(%" PRIu64 ").",
          reader_id, logid);
    } else {
      LOG_ERROR(info_log_,
          "AsyncReader %zu failed to stop reading Log(%" PRIu64 ") (%s).",
          reader_id, logid, st.ToString().c_str());
    }
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
  RS_ASSERT(storage_);
  return storage_->FindTimeAsync(logid,
                                 std::chrono::milliseconds::max(),
                                 std::move(callback));
}

int
LogTailer::NumberOpenLogs() const {
  int count = 0;
  for (const Reader& reader : readers_) {
    count += static_cast<int>(reader.log_state.size());
  }
  return count;
}

Statistics LogTailer::GetStatistics() const {
  stats_.open_logs->Set(NumberOpenLogs());
  Statistics stats = stats_.all;
  return stats;
}

bool LogTailer::TryForward(std::function<void(Flow*)> command) {
  bool force_failure = false;
  if (options_.FAULT_send_log_record_failure_rate != 0.0) {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    if (dist(ThreadLocalPRNG()) < options_.FAULT_send_log_record_failure_rate) {
      force_failure = true;
      LOG_DEBUG(info_log_, "Forcing TryForward to fail.");
    }
  }
  return !force_failure &&
    storage_to_room_queues_->GetThreadLocal()->TryWrite(command);
}

void LogTailer::Tick() {
  // Process due events on LogReaders.
  auto now = std::chrono::steady_clock::now();
  while (!restart_events_.empty() &&
         restart_events_.begin()->restart_time < now) {
    auto& ev = *restart_events_.begin();
    auto reader_id = ev.reader_id;
    auto log_id = ev.log_id;

    // Find the sequence number currently reading at.
    RS_ASSERT(reader_id < readers_.size());
    Reader& reader = readers_[reader_id];
    auto it = reader.log_state.find(log_id);
    RS_ASSERT(it != reader.log_state.end());
    auto seqno = it->second.next_seqno;

    // Restart reading at this sequence number.
    // Note: this will remove and re-add the event to restart_events_.
    StartReading(log_id, seqno, reader_id);
    stats_.forced_restarts->Add(1);
  }
}

LogTailer::RestartEvents::Handle
LogTailer::RestartEvents::AddEvent(size_t reader_id, LogID log_id) {
  // Generate random restart time.
  auto& prng = ThreadLocalPRNG();
  std::uniform_int_distribution<std::chrono::milliseconds::rep>
    distribution(min_restart_duration_.count(),
                 max_restart_duration_.count());
  auto restart_time = std::chrono::steady_clock::now() +
    std::chrono::milliseconds(distribution(prng));

  // Add to ordered set of events.
  auto result = emplace(restart_time, reader_id, log_id);
  RS_ASSERT(result.second);
  return result.first;
}

void LogTailer::RestartEvents::RemoveEvent(Handle handle) {
  erase(handle);
}

}  // namespace rocketspeed
