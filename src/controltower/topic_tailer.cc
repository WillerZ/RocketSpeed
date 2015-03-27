//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/controltower/topic_tailer.h"

#include <unordered_map>
#include <vector>
#include <inttypes.h>

#include "src/controltower/log_tailer.h"
#include "src/util/storage.h"
#include "src/util/topic_uuid.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * Encapsulates state needed for one reader of a log.
 */
class LogReader {
 public:
  /**
   * Create a LogReader.
   *
   * @param tailer LogTailer to read from.
   * @param reader_id LogTailer reader ID.
   */
  explicit LogReader(LogTailer* tailer,
                     size_t reader_id)
  : tailer_(tailer)
  , reader_id_(reader_id) {
    assert(tailer);
  }

  /**
   * Updates internal state on a delivered record.
   *
   * @param log_id Log ID of record.
   * @param seqno Sequence number of record.
   * @param topic UUID of record topic.
   * @param prev_seqno Output location for previous sequence number processed
   *                   for the topic. If this is the first record processed on
   *                   this topic then prev_seqno is set to the starting
   *                   seqno for the log.
   * @return ok() if successful, otherwise error.
   */
  Status ProcessRecord(LogID log_id,
                       SequenceNumber seqno,
                       const TopicUUID& topic,
                       SequenceNumber* prev_seqno);

  /**
   * Updates internal state on a gap.
   *
   * @param log_id Log ID of gap.
   * @param from First sequence number of gap.
   * @param to Last sequence number of gap.
   * @param type Type of gap.
   * @return ok() if successful, otherwise error.
   */
  Status ProcessGap(LogID log_id,
                    SequenceNumber from,
                    SequenceNumber to,
                    GapType type);

  /**
   * Initialize reader state for log.
   *
   * @param log_id ID of log to initialize.
   * @param seqno Starting seqno to read from.
   * @return ok() if successful, otherwise error.
   */
  Status StartReading(LogID log_id, SequenceNumber seqno);

  /**
   * Free up reader state for a log.
   *
   * @param log_id ID of log to free.
   * @return ok() if successful, otherwise error.
   */
  Status StopReading(LogID log_id);

  /**
   * Returns the log reader ID.
   */
  size_t GetReaderId() const {
    return reader_id_;
  }

 private:
  struct LogState {
    // Sequence number we started from for log.
    SequenceNumber start_seqno;

    // Sequence number the reader started reading from.
    std::unordered_map<TopicUUID, SequenceNumber> prev_seqno;

    // Last read sequence number on this log.
    SequenceNumber last_read;

    // Number of active subscribers.
    size_t num_subscribers = 0;
  };

  ThreadCheck thread_check_;
  LogTailer* tailer_;
  size_t reader_id_;
  std::unordered_map<LogID, LogState> log_state_;
};

Status LogReader::ProcessRecord(LogID log_id,
                                SequenceNumber seqno,
                                const TopicUUID& topic,
                                SequenceNumber* prev_seqno) {
  thread_check_.Check();

  // Get state for this log.
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    if (seqno != log_state.last_read + 1) {
      return Status::NotFound();
    }
    log_state.last_read = seqno;

    // Check if we've process records on this topic before.
    auto prev_seqno_it = log_state.prev_seqno.find(topic);
    if (prev_seqno_it == log_state.prev_seqno.end()) {
      // First time seeing this topic.
      // Set prev_seqno to starting seqno for log.
      *prev_seqno = log_state.start_seqno;
      log_state.prev_seqno.emplace(topic, seqno);
      return Status::OK();
    } else {
      // We've seen this topic before.
      // Use and update previous seqno.
      *prev_seqno = prev_seqno_it->second;
      prev_seqno_it->second = seqno;
      return Status::OK();
    }
  } else {
    // This log isn't open.
    return Status::NotFound();
  }
}

Status LogReader::ProcessGap(LogID log_id,
                             SequenceNumber from,
                             SequenceNumber to,
                             GapType type) {
  thread_check_.Check();

  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    if (from != log_state.last_read + 1) {
      // Ignore out-of-order records.
      return Status::NotFound();
    }
    log_state.last_read = to;
    return Status::OK();
  } else {
    return Status::NotFound();
  }
}

Status LogReader::StartReading(LogID log_id, SequenceNumber seqno) {
  thread_check_.Check();

  Status st;
  auto it = log_state_.find(log_id);
  if (it == log_state_.end()) {
    // First time opening.
    // Start the log tailer.
    const bool first_open = true;
    st = tailer_->StartReading(log_id, seqno, reader_id_, first_open);
    if (st.ok()) {
      // Insert new log state.
      LogState log_state;
      log_state.start_seqno = seqno;
      log_state.last_read = seqno - 1;
      log_state.num_subscribers = 1;
      log_state_.emplace(log_id, std::move(log_state));
    }
  } else {
    // Already reading, check if we need to reseek.
    LogState& log_state = it->second;
    if (log_state.last_read >= seqno) {
      // Need to rewind to seqno.
      const bool first_open = false;
      st = tailer_->StartReading(log_id, seqno, reader_id_, first_open);
      if (st.ok()) {
        log_state.num_subscribers++;
        log_state.last_read = seqno - 1;
        log_state.start_seqno = seqno;
        log_state.prev_seqno.clear();
      }
      return st;
    } else {
      log_state.num_subscribers++;
    }
  }
  return st;
}

Status LogReader::StopReading(LogID log_id) {
  thread_check_.Check();

  Status st;
  auto it = log_state_.find(log_id);
  if (it == log_state_.end()) {
    assert(false);
    return Status::InternalError("Not reading this log");
  } else {
    LogState& log_state = it->second;
    if (log_state.num_subscribers == 1) {
      // Last subscriber for this log, so stop reading.
      st = tailer_->StopReading(log_id, reader_id_);
      if (st.ok()) {
        log_state.num_subscribers--;
        assert(log_state.num_subscribers == 0);
        log_state_.erase(it);
      }
    } else {
      // More subscribers, just decrement the counter and continue.
      log_state.num_subscribers--;
    }
  }
  return st;
}

TopicTailer::TopicTailer(
    BaseEnv* env,
    LogTailer* log_tailer,
    std::shared_ptr<LogRouter> log_router,
    std::shared_ptr<Logger> info_log,
    std::function<void(std::unique_ptr<Message>, LogID)> on_message) :
  env_(env),
  log_tailer_(log_tailer),
  log_router_(std::move(log_router)),
  info_log_(std::move(info_log)),
  on_message_(std::move(on_message)),
  worker_loop_(65536),
  worker_thread_(0) {
}

TopicTailer::~TopicTailer() {
  if (worker_thread_) {
    worker_loop_.Stop();
    env_->WaitForJoin(worker_thread_);
  }
}

Status TopicTailer::SendLogRecord(
    std::unique_ptr<MessageData> msg,
    LogID log_id,
    size_t reader_id) {
  // Validate.
  assert(reader_id == log_reader_->GetReaderId());

  // Send to worker loop.
  MessageData* data_raw = msg.release();
  bool sent = worker_loop_.Send([this, data_raw, log_id] () {
    // Process message from the log tailer.
    std::unique_ptr<MessageData> data(data_raw);
    TopicUUID uuid(data->GetNamespaceId(), data->GetTopicName());
    SequenceNumber seqno = data->GetSequenceNumber();
    SequenceNumber prev_seqno;
    Status st = log_reader_->ProcessRecord(log_id,
                                           seqno,
                                           uuid,
                                           &prev_seqno);

    if (st.ok()) {
      data->SetSequenceNumbers(prev_seqno, seqno);
      on_message_(std::unique_ptr<Message>(data.release()), log_id);
    } else {
      // We don't have log open, so drop.
    }
  });

  Status st;
  if (!sent) {
    delete data_raw;
    st = Status::NoBuffer();
  }
  return st;
}

Status TopicTailer::SendGapRecord(
    LogID log_id,
    GapType type,
    SequenceNumber from,
    SequenceNumber to,
    size_t reader_id) {
  // Validate.
  assert(reader_id == log_reader_->GetReaderId());

  // Send to worker loop.
  bool sent = worker_loop_.Send([this, log_id, type, from, to] () {
    Status st = log_reader_->ProcessGap(log_id, from, to, type);
    if (st.ok()) {
      // Create message to send downstream.
      // We don't know the tenant ID at this point, or the host ID. These will
      // have to be set later.
      std::unique_ptr<Message> msg(
        new MessageGap(Tenant::InvalidTenant, ClientID(), type, from, to));
      on_message_(std::move(msg), log_id);
    }
  });

  return sent ? Status::OK() : Status::NoBuffer();
}

Status TopicTailer::Initialize(size_t reader_id) {
  // Initialize log_reader_.
  log_reader_.reset(new LogReader(log_tailer_, reader_id));
  worker_thread_ = env_->StartThread(
    [this] () {
      worker_loop_.Run([this] (TopicTailerCommand command) {
        // TopicTailerCommand is just a function.
        command();
      });;
    },
    "tailer-" + std::to_string(reader_id));
  return Status::OK();
}

// Create a new instance of the LogStorage
Status
TopicTailer::CreateNewInstance(
    BaseEnv* env,
    LogTailer* log_tailer,
    std::shared_ptr<LogRouter> log_router,
    std::shared_ptr<Logger> info_log,
    std::function<void(std::unique_ptr<Message>, LogID)> on_message,
    TopicTailer** tailer) {
  *tailer = new TopicTailer(env,
                            log_tailer,
                            std::move(log_router),
                            std::move(info_log),
                            std::move(on_message));
  return Status::OK();
}

Status TopicTailer::StartReading(const TopicUUID& topic,
                                 SequenceNumber start,
                                 HostNumber hostnum) {
  thread_check_.Check();

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  bool sent = worker_loop_.Send([this, logid, start] () {
    log_reader_->StartReading(logid, start);
  });
  return sent ? Status::OK() : Status::NoBuffer();
}

// Stop reading from this log
Status
TopicTailer::StopReading(const TopicUUID& topic, HostNumber hostnum) {
  thread_check_.Check();

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  bool sent = worker_loop_.Send([this, logid] () {
    log_reader_->StopReading(logid);
  });
  return sent ? Status::OK() : Status::NoBuffer();
}

Status
TopicTailer::FindLatestSeqno(
    const TopicUUID& topic,
    std::function<void(Status, SequenceNumber)> callback) {
  thread_check_.Check();

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  // Forward request to log tailer.
  return log_tailer_->FindLatestSeqno(logid, std::move(callback));
}

}  // namespace rocketspeed
