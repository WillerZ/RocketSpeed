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
   * @param info_log Logger.
   * @param tailer LogTailer to read from.
   * @param reader_id LogTailer reader ID.
   */
  explicit LogReader(std::shared_ptr<Logger> info_log,
                     LogTailer* tailer,
                     size_t reader_id)
  : info_log_(info_log)
  , tailer_(tailer)
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
   * Checks that a gap is valid for processing.
   *
   * @param log_id Log ID of gap.
   * @param from Starting sequence number of gap.
   * @return ok() if valid.
   */
  Status ValidateGap(LogID log_id, SequenceNumber from);

  /**
   * Updates internal state on a gap, and provides gap messages for each
   * affected topic.
   *
   * Pre-condition: ValidateGap(log_id, from).ok()
   *
   * @param log_id Log ID of gap.
   * @param topic Topic of gap.
   * @param from First sequence number of gap.
   * @param to Last sequence number of gap.
   * @param type Type of gap.
   */
  void ProcessGap(LogID log_id,
                  const TopicUUID& topic,
                  GapType type,
                  SequenceNumber from,
                  SequenceNumber to,
                  SequenceNumber* prev_seqno);

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
   * Flushes the log state for a log.
   *
   * @param log_id Log state to flush.
   * @param seqno Sequence number to flush before.
   */
  void FlushHistory(LogID log_id, SequenceNumber seqno);

  /**
   * Processes benign gap by advancing log reader state beyond gap.
   *
   * @param log_id Log to advance.
   * @param from Start sequence number of gap.
   * @param to End sequence number of gap.
   */
  void ProcessBenignGap(LogID log_id,
                        SequenceNumber from,
                        SequenceNumber to);

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
  std::shared_ptr<Logger> info_log_;
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
      LOG_WARN(info_log_,
        "Record received out of order. Expected:%" PRIu64 " Received:%" PRIu64,
        log_state.last_read + 1,
        seqno);
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
    LOG_WARN(info_log_,
      "Record received for unopened Log(%" PRIu64 ")",
      log_id);
    return Status::NotFound();
  }
}

Status LogReader::ValidateGap(LogID log_id, SequenceNumber from) {
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    if (from != log_state.last_read + 1) {
      LOG_INFO(info_log_,
        "Gap received out of order. Expected:%" PRIu64 " Received:%" PRIu64,
        log_state.last_read + 1,
        from);
      return Status::NotFound();
    }
  } else {
    LOG_INFO(info_log_,
      "Gap received on unopened Log(%" PRIu64 ")",
      log_id);
    return Status::NotFound();
  }
  return Status::OK();
}

void LogReader::ProcessGap(
    LogID log_id,
    const TopicUUID& topic,
    GapType type,
    SequenceNumber from,
    SequenceNumber to,
    SequenceNumber* prev_seqno) {
  thread_check_.Check();

  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    if (from != log_state.last_read + 1) {
      assert(false);  // should have been validated before calling this.
    }

    // Find previous seqno for topic.
    auto seq_it = log_state.prev_seqno.find(topic);
    if (seq_it == log_state.prev_seqno.end()) {
      *prev_seqno = log_state.start_seqno;
      log_state.prev_seqno.emplace(topic, to);
    } else {
      *prev_seqno = seq_it->second;
      seq_it->second = to;
    }
  } else {
    assert(false);  // should have been validated before calling this.
  }
}

void LogReader::FlushHistory(LogID log_id, SequenceNumber seqno) {
  thread_check_.Check();
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    log_state.start_seqno = seqno;
    log_state.last_read = seqno - 1;
    log_state.prev_seqno.clear();
  }
}

void LogReader::ProcessBenignGap(LogID log_id,
                                 SequenceNumber from,
                                 SequenceNumber to) {
  thread_check_.Check();
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    log_state.last_read = to;
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
    std::function<void(std::unique_ptr<Message>,
                       std::vector<HostNumber>)> on_message) :
  env_(env),
  log_tailer_(log_tailer),
  log_router_(std::move(log_router)),
  info_log_(std::move(info_log)),
  on_message_(std::move(on_message)),
  worker_loop_(1 << 20),
  worker_thread_(0) {
}

TopicTailer::~TopicTailer() {
  assert(worker_thread_ == 0);  // must call Stop() before deleting.
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
    SequenceNumber next_seqno = data->GetSequenceNumber();
    SequenceNumber prev_seqno;
    Status st = log_reader_->ProcessRecord(log_id,
                                           next_seqno,
                                           uuid,
                                           &prev_seqno);

    if (st.ok()) {
      // Find subscribed hosts.
      std::vector<HostNumber> hosts;
      topic_map_[log_id].VisitSubscribers(
        uuid, prev_seqno, next_seqno,
        [this, &hosts, next_seqno, &uuid] (TopicSubscription* sub) {
          hosts.emplace_back(sub->GetHostNum());
          sub->SetSequenceNumber(next_seqno + 1);
          LOG_INFO(info_log_,
            "Hostnum(%d) advanced to %s@%" PRIu64,
            int(sub->GetHostNum()),
            uuid.ToString().c_str(),
            next_seqno);
        });

      if (hosts.empty()) {
        LOG_INFO(info_log_,
          "No hosts found for message on %s@%" PRIu64 "-%" PRIu64,
          uuid.ToString().c_str(),
          prev_seqno,
          next_seqno);
      } else {
        // Send message downstream.
        data->SetSequenceNumbers(prev_seqno, next_seqno);
        on_message_(std::unique_ptr<Message>(data.release()), std::move(hosts));
      }
    } else {
      // We don't have log open, so drop.
      LOG_WARN(info_log_,
        "Failed to process message (%.16s) on Log(%" PRIu64 ")@%" PRIu64
        " (%s)",
        data->GetPayload().ToString().c_str(),
        log_id,
        next_seqno,
        st.ToString().c_str());
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
    // Check for out-of-order gap messages, or gaps received on log that
    // we're not reading on.
    Status st = log_reader_->ValidateGap(log_id, from);
    if (!st.ok()) {
      return;
    }

    // Send per-topic gap messages for subscribed topics.
    topic_map_[log_id].VisitTopics(
      [&] (const TopicUUID& topic) {
        // Get the last known seqno for topic.
        SequenceNumber prev_seqno;
        log_reader_->ProcessGap(log_id, topic, type, from, to, &prev_seqno);

        // Find subscribed hosts.
        std::vector<HostNumber> hosts;
        topic_map_[log_id].VisitSubscribers(
          topic, prev_seqno, to,
          [this, &hosts, to, &topic] (TopicSubscription* sub) {
            hosts.emplace_back(sub->GetHostNum());
            sub->SetSequenceNumber(to + 1);
            LOG_INFO(info_log_,
              "Hostnum(%d) advanced to %s@%" PRIu64,
              int(sub->GetHostNum()),
              topic.ToString().c_str(),
              to);
          });

        // Send message.
        Slice namespace_id;
        Slice topic_name;
        topic.GetTopicID(&namespace_id, &topic_name);
        std::unique_ptr<Message> msg(
          new MessageGap(Tenant::GuestTenant,
                         namespace_id,
                         topic_name,
                         type,
                         prev_seqno,
                         to));
        on_message_(std::move(msg), std::move(hosts));
      });

    if (type == GapType::kBenign) {
      // For benign gaps, we haven't lost any information, but we need to
      // advance the state of the log reader so that it expects the next
      // records.
      log_reader_->ProcessBenignGap(log_id, from, to);
    } else {
      // For malignant gaps (retention or data loss), we've lost information
      // about the history of topics in the log, so we need to flush the
      // log reader history to avoid it claiming to know something about topics
      // that it doesn't.
      log_reader_->FlushHistory(log_id, to + 1);
    }
  });

  return sent ? Status::OK() : Status::NoBuffer();
}

Status TopicTailer::Initialize(size_t reader_id) {
  // Initialize log_reader_.
  log_reader_.reset(new LogReader(info_log_, log_tailer_, reader_id));
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

void TopicTailer::Stop() {
  if (worker_thread_) {
    worker_loop_.Stop();
    env_->WaitForJoin(worker_thread_);
    worker_thread_ = 0;
  }
}

// Create a new instance of the TopicTailer
Status
TopicTailer::CreateNewInstance(
    BaseEnv* env,
    LogTailer* log_tailer,
    std::shared_ptr<LogRouter> log_router,
    std::shared_ptr<Logger> info_log,
    std::function<void(std::unique_ptr<Message>,
                       std::vector<HostNumber>)> on_message,
    TopicTailer** tailer) {
  *tailer = new TopicTailer(env,
                            log_tailer,
                            std::move(log_router),
                            std::move(info_log),
                            std::move(on_message));
  return Status::OK();
}

Status TopicTailer::AddSubscriber(const TopicUUID& topic,
                                  SequenceNumber start,
                                  HostNumber hostnum) {
  thread_check_.Check();

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  bool sent = worker_loop_.Send([this, logid, topic, start, hostnum] () {
    bool was_added = topic_map_[logid].AddSubscriber(topic, start, hostnum);
    LOG_INFO(info_log_,
      "Hostnum(%d) subscribed for %s@%" PRIu64 " (%s)",
      int(hostnum),
      topic.ToString().c_str(),
      start,
      was_added ? "new" : "update");

    if (!was_added) {
      // Was update, so remove old subscription first.
      log_reader_->StopReading(logid);
    }
    log_reader_->StartReading(logid, start);
  });

  return sent ? Status::OK() : Status::NoBuffer();
}

// Stop reading from this log
Status
TopicTailer::RemoveSubscriber(const TopicUUID& topic, HostNumber hostnum) {
  thread_check_.Check();

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  bool sent = worker_loop_.Send([this, logid, topic, hostnum] () {
    bool was_removed = topic_map_[logid].RemoveSubscriber(topic, hostnum);
    if (was_removed) {
      LOG_INFO(info_log_,
        "Hostnum(%d) unsubscribed for %s",
        int(hostnum),
        topic.ToString().c_str());

      log_reader_->StopReading(logid);
    }
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
