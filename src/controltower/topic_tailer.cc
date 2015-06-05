//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/controltower/topic_tailer.h"

#include <limits>
#include <unordered_map>
#include <vector>
#include <inttypes.h>

#include "src/controltower/log_tailer.h"
#include "src/util/storage.h"
#include "src/util/topic_uuid.h"
#include "src/util/common/linked_map.h"
#include "src/util/common/thread_check.h"
#include "src/messages/msg_loop.h"

namespace rocketspeed {

/**
 * Constants for the cost of accepting a subscription.
 * See: LogReader::SubscriptionCost.
 */
enum : uint64_t {
  /**
   * The cost of rewinding is infinite (we don't want to rewind unless we must).
   */
  kSubscriptionCostRewind = std::numeric_limits<uint64_t>::max(),

  /**
   * Heuristic for the cost of starting a subscription. If we have a reader
   * at 100, a spare reader with no logs open, and a new subscription at 101,
   * it would be better for the reader at 100 to take on the subscription than
   * to start a new reader. The break-even point where a new reader is
   * preferable is when the old reader is kSubscriptionCostStart behind.
   */
  kSubscriptionCostStart = 1000
};

/**
 * Encapsulates state needed for one reader of a log.
 */
class LogReader {
 public:
  /**
   * Create a LogReader.
   *
   * @param info_log Logger.
   * @param tailer LogTailer to read from (or nullptr for virtual readers).
   * @param reader_id LogTailer reader ID.
   * @param max_subscription_lag Maximum number of sequence numbers a
   *                             subscription can lag behind before sending gap.
   */
  explicit LogReader(std::shared_ptr<Logger> info_log,
                     LogTailer* tailer,
                     size_t reader_id,
                     int64_t max_subscription_lag)
  : info_log_(info_log)
  , tailer_(tailer)
  , reader_id_(reader_id)
  , max_subscription_lag_(max_subscription_lag) {
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
   * Should be called whenever a new subscription arrives for a topic, which
   * will be handled by this reader.
   *
   * @param topic Topic to start reading.
   * @param log_id ID of log to initialize.
   * @param seqno Starting seqno to read from.
   * @return ok() if successful, otherwise error.
   */
  Status StartReading(const TopicUUID& topic,
                      LogID log_id,
                      SequenceNumber seqno);

  /**
   * Should be called when there are *no more* readers on a topic, entirely.
   * Will cause the log reader to forget about previous sequence numbers for
   * the topic, and if this was the last topic subscribed then will close the
   * log entirely.
   *
   * @param topic Topic to stop reading.
   * @param log_id ID of log to free.
   * @return ok() if successful, otherwise error.
   */
  Status StopReading(const TopicUUID& topic, LogID log_id);

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
   * Bump lagging subscriptions that are older than
   * (next_seqno - max_subscription_lag). on_bump will be called for all topics
   * that have been bumped, with the last known sequence number on the topic.
   *
   * @param log_id Log to trim.
   * @param next_seqno Tail sequence number of the log to trim.
   * @param on_bump To be invoked for bumped topics.
   */
  void BumpLaggingSubscriptions(
    LogID log_id,
    SequenceNumber next_seqno,
    std::function<void(const TopicUUID&, SequenceNumber)> on_bump);

  /**
   * Returns the cost of accepting a new subscription (lower better).
   */
  uint64_t SubscriptionCost(const TopicUUID& topic,
                            LogID log_id,
                            SequenceNumber seqno) const;

  /**
   * Tests if this LogReader can be merged into another for a particular log,
   * i.e. reader can subsume all of this reader's subscriptions.
   */
  bool CanMergeInto(LogReader* reader, LogID log_id) const;

  /**
   * Merges subscriptions state into another LogReader for a particular log.
   * This reader will stop reading on log_id, and its state removed.
   */
  void MergeInto(LogReader* reader, LogID log_id);

  /**
   * Take the log subscriptions from another reader and start reading.
   */
  void StealLogSubscriptions(LogReader* reader, LogID log_id);

  /**
   * Returns the log reader ID.
   */
  size_t GetReaderId() const {
    return reader_id_;
  }

  /**
   * A virtual reader maintains a start_seqno and topic state, without having
   * an actual log reader active.
   */
  bool IsVirtual() const {
    return tailer_ == nullptr;
  }

  /**
   * Check if log is open.
   */
  bool IsLogOpen(LogID log_id) const {
    return log_state_.find(log_id) != log_state_.end();
  }

  /**
   * Get human-readable information about a log.
   */
  std::string GetLogInfo(LogID log_id) const;

  /**
   * Get human-readable information about all logs.
   */
  std::string GetAllLogsInfo() const;


 private:
  struct TopicState {
    SequenceNumber next_seqno;
  };

  struct LogState {
    // Sequence number we started from for log.
    SequenceNumber start_seqno;

    // State of subscriptions on each topic.
    LinkedMap<TopicUUID, TopicState> topics;

    // Last read sequence number on this log.
    SequenceNumber last_read;

    // This is a lower-bound estimate on the last sequence number for this log.
    // A tail_seqno 0 should be interpreted as no estimate.
    // tail_seqno will be initially set after a call to FindLatestSeqno,
    // and will increase on receipt of later records.
    // Stopping reading will reset the tail_seqno to 0.
    // This value can become inaccurate if a reader is receiving records
    // slower than they are produced.
    SequenceNumber tail_seqno = 0;
  };

  ThreadCheck thread_check_;
  std::shared_ptr<Logger> info_log_;
  LogTailer* tailer_;
  size_t reader_id_;
  std::unordered_map<LogID, LogState> log_state_;
  int64_t max_subscription_lag_;
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
        "Reader(%zu) received record out of order on %s Log(%" PRIu64 ")."
        " Expected:%" PRIu64 " Received:%" PRIu64,
        reader_id_,
        topic.ToString().c_str(),
        log_id,
        log_state.last_read + 1,
        seqno);
      return Status::NotFound();
    }
    log_state.last_read = seqno;

    // Check if we've process records on this topic before.
    auto it = log_state.topics.find(topic);
    if (it != log_state.topics.end()) {
      // Advance reader for this topic.
      *prev_seqno = it->second.next_seqno;
      it->second.next_seqno = seqno + 1;
      log_state.topics.move_to_back(it);
    } else {
      *prev_seqno = 0;  // no topic
    }
    return Status::OK();
  } else {
    // This log isn't open.
    LOG_WARN(info_log_,
      "Reader(%zu) received record for %s on unopened Log(%" PRIu64 ")",
      reader_id_,
      topic.ToString().c_str(), log_id);
    return Status::NotFound();
  }
}

Status LogReader::ValidateGap(LogID log_id, SequenceNumber from) {
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    if (from != log_state.last_read + 1) {
      LOG_DEBUG(info_log_,
        "Reader(%zu) received gap out of order. Expected:%" PRIu64
        " Received:%" PRIu64,
        reader_id_,
        log_state.last_read + 1,
        from);
      return Status::NotFound();
    }
  } else {
    LOG_DEBUG(info_log_,
      "Reader(%zu) received gap on unopened Log(%" PRIu64 ")",
      reader_id_,
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
    auto it = log_state.topics.find(topic);
    if (it != log_state.topics.end()) {
      *prev_seqno = it->second.next_seqno;
      assert(*prev_seqno != 0);
      it->second.next_seqno = to + 1;
      log_state.topics.move_to_back(it);
    } else {
      *prev_seqno = 0;
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

void LogReader::BumpLaggingSubscriptions(
    LogID log_id,
    SequenceNumber seqno,
    std::function<void(const TopicUUID&, SequenceNumber)> on_bump) {
  thread_check_.Check();
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    while (!log_state.topics.empty()) {
      // Get topic with oldest known sequence number.
      auto it = log_state.topics.begin();
      const SequenceNumber tseqno = it->second.next_seqno;

      // Is it older than the trim point?
      if (tseqno + max_subscription_lag_ < seqno) {
        // Eligible for bump.
        const TopicUUID& topic = it->first;
        LOG_DEBUG(info_log_,
          "Bumping %s from %" PRIu64 " to %" PRIu64 " on Log(%" PRIu64 ")",
          topic.ToString().c_str(),
          tseqno,
          seqno,
          log_id);
        on_bump(topic, tseqno);
        log_state.topics.move_to_back(it);
        it->second.next_seqno = seqno + 1;
      } else {
        break;
      }
    }
  }
}

Status LogReader::StartReading(const TopicUUID& topic,
                               LogID log_id,
                               SequenceNumber seqno) {
  thread_check_.Check();

  Status st;
  auto log_it = log_state_.find(log_id);
  const bool first_open = (log_it == log_state_.end());
  if (first_open) {
    // First time opening this log.
    LogState log_state;
    log_state.start_seqno = seqno;
    log_state.last_read = seqno - 1;
    log_it = log_state_.emplace(log_id, std::move(log_state)).first;
  }

  LogState& log_state = log_it->second;

  bool reseek = false;
  auto it = log_state.topics.find(topic);
  if (it == log_state.topics.end()) {
    TopicState topic_state;
    topic_state.next_seqno = seqno;
    it = log_state.topics.emplace_front(topic, topic_state).first;
    reseek = true;
  } else {
    reseek = (seqno < it->second.next_seqno);
    it->second.next_seqno = std::min(it->second.next_seqno, seqno);
    log_state.topics.move_to_front(it);
  }

  if (!first_open && reseek) {
    // No need to reseek if we are yet to reach that sequence number.
    reseek = (seqno <= log_state.last_read);
  }

  if (seqno < log_state.start_seqno) {
    assert(reseek);
  }

  if (reseek) {
    if (first_open) {
      LOG_INFO(info_log_,
        "%sReader(%zu) now reading Log(%" PRIu64 ") from %" PRIu64 " for %s",
        IsVirtual() ? "Virtual" : "",
        reader_id_, log_id, seqno, topic.ToString().c_str());
    } else {
      LOG_INFO(info_log_,
        "%sReader(%zu) rewinding Log(%" PRIu64 ") from %" PRIu64 " to %" PRIu64
        " for %s",
        IsVirtual() ? "Virtual" : "",
        reader_id_,
        log_id,
        log_state.last_read + 1,
        seqno,
        topic.ToString().c_str());
    }

    if (!IsVirtual()) {
      st = tailer_->StartReading(log_id, seqno, reader_id_, first_open);
      if (!st.ok()) {
        LOG_ERROR(info_log_,
          "Reader(%zu) failed to start reading Log(%" PRIu64 ")@%" PRIu64": %s",
          reader_id_,
          log_id,
          seqno,
          st.ToString().c_str());
        return st;
      }
    }
    log_state.start_seqno = std::min(log_state.start_seqno, seqno);
    log_state.last_read = seqno - 1;
  }
  return st;
}

Status LogReader::StopReading(const TopicUUID& topic, LogID log_id) {
  thread_check_.Check();

  Status st;
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
    auto it = log_state.topics.find(topic);
    if (it != log_state.topics.end()) {
      LOG_INFO(info_log_,
        "No more subscribers on %s for Log(%" PRIu64 ") %sReader(%zu)",
        topic.ToString().c_str(),
        log_id,
        IsVirtual() ? "Virtual" : "",
        reader_id_);
      log_state.topics.erase(it);

      if (log_state.topics.empty()) {
        // Last subscriber for this log, so stop reading.
        if (!IsVirtual()) {
          st = tailer_->StopReading(log_id, reader_id_);
        }
        if (st.ok()) {
          LOG_INFO(info_log_,
            "No more subscribers on Log(%" PRIu64 ") %sReader(%zu)",
            log_id,
            IsVirtual() ? "Virtual" : "",
            reader_id_);
          assert(log_state.topics.empty());
          log_state_.erase(log_it);
        } else {
          LOG_ERROR(info_log_,
            "Reader(%zu) failed to stop reading Log(%" PRIu64 "): %s",
            reader_id_,
            log_id,
            st.ToString().c_str());
        }
      }
    }
  }
  return st;
}

uint64_t LogReader::SubscriptionCost(const TopicUUID& topic,
                                     LogID log_id,
                                     SequenceNumber seqno) const {
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    const LogState& log_state = log_it->second;
    if (log_state.last_read < seqno) {
      // We haven't reached this seqno yet, so the costs is the distance
      // until we reach the new sequence number.
      return seqno - log_state.last_read;
    }

    // We have already passed the subscription seqno, but we might have
    // kept track of it for a different subscriber.
    auto it = log_state.topics.find(topic);
    if (it == log_state.topics.end()) {
      // Unknown topic, so rewind necessary.
      return kSubscriptionCostRewind;
    } else {
      if (seqno < it->second.next_seqno) {
        // We've already passed this seqno, even for this topic, so rewind.
        return kSubscriptionCostRewind;
      } else {
        // Zero cost to taking on this subscription.
        return 0;
      }
    }
  } else {
    // We aren't reading this log, so we can start reading immediately.
    // However, to start reading we need to communicate with the log storage,
    // which has a cost. It's cheaper for a reader at 100 to accept a
    // subscription at 101 than it is for an idle reader to open that log.
    return kSubscriptionCostStart;
  }
}

bool LogReader::CanMergeInto(LogReader* reader, LogID log_id) const {
  thread_check_.Check();
  assert(reader);

  // Cannot merge to/from a virtual reader
  assert(!IsVirtual());
  assert(!reader->IsVirtual());

  // Find LogState in this reader.
  auto log_it1 = log_state_.find(log_id);
  if (log_it1 == log_state_.end()) {
    // We're not reading this log, nothing to merge.
    return false;
  }

  // Find LogState in destination reader.
  auto log_it2 = reader->log_state_.find(log_id);
  if (log_it2 == reader->log_state_.end()) {
    // Reader isn't reading this log, so cannot subsume subscriptions.
    return false;
  }

  // Can merge when they are at the same sequence number.
  const LogState& src = log_it1->second;
  const LogState& dest = log_it2->second;
  return dest.last_read == src.last_read;
}

void LogReader::MergeInto(LogReader* reader, LogID log_id) {
  thread_check_.Check();
  assert(reader);
  assert(CanMergeInto(reader, log_id));

  // Extract LogStates for this log.
  auto log_it1 = log_state_.find(log_id);
  auto log_it2 = reader->log_state_.find(log_id);
  assert(log_it1 != log_state_.end());
  assert(log_it2 != log_state_.end());

  // Verify last_read.
  LogState& src = log_it1->second;
  LogState& dest = log_it2->second;
  assert(dest.last_read == src.last_read);

  LOG_INFO(info_log_,
    "Merging Reader(%zu) into Reader(%zu) on Log(%" PRIu64 ")@%" PRIu64,
    reader_id_,
    reader->reader_id_,
    log_id,
    src.last_read);

  // Now just merge the topic state by taking the min of next_seqno for each.
  for (auto& src_topic_entry : src.topics) {
    const TopicUUID& topic = src_topic_entry.first;
    TopicState& src_topic = src_topic_entry.second;
    auto it = dest.topics.find(topic);
    if (it != dest.topics.end()) {
      // Merge TopicStates by taking the min seqno.
      TopicState& dest_topic = it->second;
      dest_topic.next_seqno = std::min(dest_topic.next_seqno,
                                       src_topic.next_seqno);
    } else {
      // Merge by inserting.
      TopicState topic_state;
      topic_state.next_seqno = src_topic.next_seqno;
      // TODO(pja) : these shouldn't emplace_back
      dest.topics.emplace_back(topic, topic_state);
    }
  }

  // Now clear our state and stop reading the log.
  log_state_.erase(log_it1);
  Status st = tailer_->StopReading(log_id, reader_id_);
  if (st.ok()) {
    LOG_INFO(info_log_, "Reader(%zu) stopped on Log(%" PRIu64 ") due to merge",
      reader_id_,
      log_id);
  } else {
    LOG_ERROR(info_log_, "Failed to stop Reader(%zu) on Log(%" PRIu64 "): %s",
      reader_id_,
      log_id,
      st.ToString().c_str());
  }
}

void LogReader::StealLogSubscriptions(LogReader* reader, LogID log_id) {
  // Must be stealing from a virtual log.
  assert(reader->IsVirtual());
  assert(reader->IsLogOpen(log_id));
  assert(!IsVirtual());
  assert(!IsLogOpen(log_id));

  LogState& log_state = reader->log_state_.find(log_id)->second;

  const bool first_open = true;
  Status st = tailer_->StartReading(log_id,
                                    log_state.start_seqno,
                                    reader_id_,
                                    first_open);
  if (st.ok()) {
    log_state_.emplace(log_id, std::move(log_state));
  } else {
    LOG_ERROR(info_log_,
      "Reader(%zu) failed to start reading Log(%" PRIu64 ")@%" PRIu64 ": %s",
      reader_id_,
      log_id,
      log_state.start_seqno,
      st.ToString().c_str());
  }
}

std::string LogReader::GetLogInfo(LogID log_id) const {
  thread_check_.Check();
  char buffer[1024];
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    const LogState& log_state = log_it->second;
    snprintf(
      buffer, sizeof(buffer),
      "Log(%" PRIu64 ").reader[%zu].start_seqno: %" PRIu64 "\n"
      "Log(%" PRIu64 ").reader[%zu].last_read: %" PRIu64 "\n"
      "Log(%" PRIu64 ").reader[%zu].num_topics_subscribed: %zu\n",
      log_id, reader_id_, log_state.start_seqno,
      log_id, reader_id_, log_state.last_read,
      log_id, reader_id_, log_state.topics.size());
  } else {
    snprintf(buffer, sizeof(buffer),
      "Log(%" PRIu64 ").reader[%zu] not currently reading\n",
      log_id, reader_id_);
  }
  return std::string(buffer);
}

std::string LogReader::GetAllLogsInfo() const {
  thread_check_.Check();
  std::string result;
  for (const auto& log_entry : log_state_) {
    result += GetLogInfo(log_entry.first);
  }
  return result;
}

TopicTailer::TopicTailer(
    BaseEnv* env,
    MsgLoop* msg_loop,
    int worker_id,
    LogTailer* log_tailer,
    std::shared_ptr<LogRouter> log_router,
    std::shared_ptr<Logger> info_log,
    std::function<void(std::unique_ptr<Message>,
                       std::vector<HostNumber>)> on_message) :
  env_(env),
  msg_loop_(msg_loop),
  worker_id_(worker_id),
  log_tailer_(log_tailer),
  log_router_(std::move(log_router)),
  info_log_(std::move(info_log)),
  on_message_(std::move(on_message)) {
}

TopicTailer::~TopicTailer() {
}

Status TopicTailer::SendLogRecord(
    std::unique_ptr<MessageData> msg,
    LogID log_id,
    size_t reader_id) {
  // Send to worker loop.
  MessageData* data_raw = msg.release();
  bool sent = Forward([this, data_raw, log_id, reader_id] () {
    // Validate.
    LogReader* reader = FindLogReader(reader_id);
    assert(reader != nullptr);

    // Process message from the log tailer.
    stats_.log_records_received->Add(1);
    std::unique_ptr<MessageData> data(data_raw);
    TopicUUID uuid(data->GetNamespaceId(), data->GetTopicName());
    SequenceNumber next_seqno = data->GetSequenceNumber();
    SequenceNumber prev_seqno = 0;
    Status st = reader->ProcessRecord(log_id,
                                      next_seqno,
                                      uuid,
                                      &prev_seqno);

    auto ts_it = tail_seqno_cached_.find(log_id);
    bool is_tail = false;
    if (ts_it != tail_seqno_cached_.end() && ts_it->second <= next_seqno) {
      // If we had an estimate on the tail sequence number and it was lower
      // than this record, then update the estimate.
      is_tail = true;
      ts_it->second = next_seqno + 1;
    }

    if (prev_seqno != 0 && st.ok()) {
      // Find subscribed hosts.
      std::vector<HostNumber> hosts;
      topic_map_[log_id].VisitSubscribers(
        uuid, prev_seqno, next_seqno,
        [&] (TopicSubscription* sub) {
          hosts.emplace_back(sub->GetHostNum());
          sub->SetSequenceNumber(next_seqno + 1);
          LOG_DEBUG(info_log_,
            "Hostnum(%d) advanced to %s@%" PRIu64 " on Log(%" PRIu64 ")"
            " Reader(%zu)",
            int(sub->GetHostNum()),
            uuid.ToString().c_str(),
            next_seqno + 1,
            log_id,
            reader_id);
        });

      std::vector<HostNumber> tail_hosts;
      if (is_tail) {
        // This is a message at the tail.
        // Find all hosts subscribed at 0.
        topic_map_[log_id].VisitSubscribers(
          uuid, 0, 0,
          [&] (TopicSubscription* sub) {
            tail_hosts.emplace_back(sub->GetHostNum());
            sub->SetSequenceNumber(next_seqno + 1);
            LOG_DEBUG(info_log_,
              "Hostnum(%d) advanced to %s@%" PRIu64 " on Log(%" PRIu64 ")"
              " Reader(%zu)",
              int(sub->GetHostNum()),
              uuid.ToString().c_str(),
              next_seqno + 1,
              log_id,
              reader_id);
          });

        // Hosts subscribed at the tail need the message previous sequence
        // number to be 0, so we need to send a different message to these
        // hosts.
        if (!tail_hosts.empty()) {
          std::unique_ptr<Message> tail_data;
          data->SetSequenceNumbers(0, next_seqno);
          if (hosts.empty()) {
            // No hosts subscribed at non-0, so just use the data message.
            tail_data = std::move(data);
          } else {
            // We need to send 'data' to the non-0 subscribing hosts.
            tail_data = Message::Copy(*data);
          }
          // Send message downstream.
          stats_.new_tail_records_sent->Add(1);
          on_message_(std::move(tail_data), std::move(tail_hosts));
        }
      }

      if (!hosts.empty()) {
        // Send message downstream.
        assert(data);
        data->SetSequenceNumbers(prev_seqno, next_seqno);
        stats_.log_records_with_subscriptions->Add(1);
        on_message_(std::unique_ptr<Message>(data.release()), std::move(hosts));
      }

      if (data) {
        stats_.log_records_without_subscriptions->Add(1);
        LOG_DEBUG(info_log_,
          "Reader(%zu) found no hosts for %smessage on %s@%" PRIu64 "-%" PRIu64,
          reader_id,
          is_tail ? "tail " : "",
          uuid.ToString().c_str(),
          prev_seqno,
          next_seqno);
      }

      // Bump subscriptions that are many subscriptions behind.
      // If there is a topic that hasn't been seen for a while in this log then
      // we send a gap from its expected sequence number to the current seqno.
      // For example, if we are at sequence number 200 and topic T was last seen
      // at sequence number 100, then we send a gap from 100-200 to subscribers
      // on T.
      reader->BumpLaggingSubscriptions(
        log_id,            // Log to bump
        next_seqno,        // Current seqno
        [&] (const TopicUUID& topic, SequenceNumber bump_seqno) {
          // This will be called for each bumped topic.
          // bump_seqno is the last known seqno for the topic.

          // Find subscribed hosts between bump_seqno and next_seqno.
          std::vector<HostNumber> bumped_hosts;
          topic_map_[log_id].VisitSubscribers(
            topic, bump_seqno, next_seqno,
            [&] (TopicSubscription* sub) {
              // Add host to list.
              bumped_hosts.emplace_back(sub->GetHostNum());

              // Advance subscription.
              sub->SetSequenceNumber(next_seqno + 1);
              LOG_DEBUG(info_log_,
                "Hostnum(%d) bumped to %s@%" PRIu64 " on Log(%" PRIu64 ")"
                " Reader(%zu)",
                int(sub->GetHostNum()),
                topic.ToString().c_str(),
                next_seqno + 1,
                log_id,
                reader_id);
            });

          if (!bumped_hosts.empty()) {
            // Send gap message.
            Slice namespace_id;
            Slice topic_name;
            topic.GetTopicID(&namespace_id, &topic_name);
            std::unique_ptr<Message> trim_msg(
              new MessageGap(Tenant::GuestTenant,
                             namespace_id.ToString(),
                             topic_name.ToString(),
                             GapType::kBenign,
                             bump_seqno,
                             next_seqno));
            stats_.bumped_subscriptions->Add(bumped_hosts.size());
            on_message_(std::move(trim_msg), std::move(bumped_hosts));
          }
        });
    } else {
      // Log not open or at wrong seqno, so drop.
      stats_.log_records_out_of_order->Add(1);
      LOG_WARN(info_log_,
        "Reader(%zu) failed to process message (%.16s)"
        " on Log(%" PRIu64 ")@%" PRIu64
        " (%s)",
        reader_id,
        data->GetPayload().ToString().c_str(),
        log_id,
        next_seqno,
        st.ToString().c_str());
    }

    AttemptReaderMerges(reader, log_id);
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
  // Send to worker loop.
  bool sent = Forward([this, log_id, type, from, to, reader_id] () {
    // Validate.
    LogReader* reader = FindLogReader(reader_id);
    assert(reader != nullptr);

    // Check for out-of-order gap messages, or gaps received on log that
    // we're not reading on.
    stats_.gap_records_received->Add(1);
    Status st = reader->ValidateGap(log_id, from);
    if (!st.ok()) {
      stats_.gap_records_out_of_order->Add(1);
      return;
    }

    // Send per-topic gap messages for subscribed topics.
    topic_map_[log_id].VisitTopics(
      [&] (const TopicUUID& topic) {
        // Get the last known seqno for topic.
        SequenceNumber prev_seqno;
        reader->ProcessGap(log_id, topic, type, from, to, &prev_seqno);

        auto ts_it = tail_seqno_cached_.find(log_id);
        if (ts_it != tail_seqno_cached_.end() && ts_it->second <= to) {
          // If we had an estimate on the tail sequence number and it was lower
          // than this record, then update the estimate.
          ts_it->second = to + 1;
        }

        // Find subscribed hosts.
        std::vector<HostNumber> hosts;
        topic_map_[log_id].VisitSubscribers(
          topic, prev_seqno, to,
          [&] (TopicSubscription* sub) {
            hosts.emplace_back(sub->GetHostNum());
            sub->SetSequenceNumber(to + 1);
            LOG_DEBUG(info_log_,
              "Hostnum(%d) advanced to %s@%" PRIu64 " on Log(%" PRIu64 ")"
              " Reader(%zu)",
              int(sub->GetHostNum()),
              topic.ToString().c_str(),
              to,
              log_id,
              reader_id);
          });

        // Send message.
        if (!hosts.empty()){
          Slice namespace_id;
          Slice topic_name;
          topic.GetTopicID(&namespace_id, &topic_name);
          std::unique_ptr<Message> msg(
            new MessageGap(Tenant::GuestTenant,
                           namespace_id.ToString(),
                           topic_name.ToString(),
                           type,
                           prev_seqno,
                           to));
          stats_.gap_records_with_subscriptions->Add(1);
          on_message_(std::move(msg), std::move(hosts));
        } else {
          stats_.gap_records_without_subscriptions->Add(1);
        }
      });

    if (type == GapType::kBenign) {
      // For benign gaps, we haven't lost any information, but we need to
      // advance the state of the log reader so that it expects the next
      // records.
      stats_.benign_gaps_received->Add(1);
      reader->ProcessBenignGap(log_id, from, to);
    } else {
      // For malignant gaps (retention or data loss), we've lost information
      // about the history of topics in the log, so we need to flush the
      // log reader history to avoid it claiming to know something about topics
      // that it doesn't.
      stats_.malignant_gaps_received->Add(1);
      reader->FlushHistory(log_id, to + 1);
    }

    AttemptReaderMerges(reader, log_id);
  });

  return sent ? Status::OK() : Status::NoBuffer();
}

SequenceNumber TopicTailer::GetTailSeqnoEstimate(LogID log_id) const {
  thread_check_.Check();
  auto ts_it = tail_seqno_cached_.find(log_id);
  return ts_it == tail_seqno_cached_.end() ? 0 : ts_it->second;
}

Status TopicTailer::Initialize(const std::vector<size_t>& reader_ids,
                               int64_t max_subscription_lag) {
  // Initialize log_readers_.
  for (size_t reader_id : reader_ids) {
    log_readers_.emplace_back(
      new LogReader(info_log_,
                    log_tailer_,
                    reader_id,
                    max_subscription_lag));
  }
  pending_reader_.reset(
    new LogReader(info_log_,
                  nullptr,  // null LogTailer <=> virtual reader
                  0,
                  max_subscription_lag));
  return Status::OK();
}

// Create a new instance of the TopicTailer
Status
TopicTailer::CreateNewInstance(
    BaseEnv* env,
    MsgLoop* msg_loop,
    int worker_id,
    LogTailer* log_tailer,
    std::shared_ptr<LogRouter> log_router,
    std::shared_ptr<Logger> info_log,
    std::function<void(std::unique_ptr<Message>,
                       std::vector<HostNumber>)> on_message,
    TopicTailer** tailer) {
  *tailer = new TopicTailer(env,
                            msg_loop,
                            worker_id,
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
  stats_.add_subscriber_requests->Add(1);

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  // Handle to 0 sequence number special case.
  // Zero means to start reading from the latest records, so we first need
  // to asynchronously consult the LogTailer for the latest seqno, and then
  // process the subscription.
  if (start == 0) {
    stats_.add_subscriber_requests_at_0->Add(1);
    // Create a callback to enqueue a subscribe command.
    // TODO(pja) 1: When this is passed to FindLatestSeqno, it will allocate
    // when converted to an std::function - could use an alloc pool for this.
    auto callback = [this, topic, hostnum, logid] (Status status,
                                                   SequenceNumber seqno) {
      if (!status.ok()) {
        LOG_WARN(info_log_,
          "Failed to find latest sequence number in %s (%s)",
          topic.ToString().c_str(),
          status.ToString().c_str());
        return;
      }

      bool sent = Forward([this, topic, hostnum, logid, seqno] () {
        AddTailSubscriber(topic, hostnum, logid, seqno);

        LOG_INFO(info_log_,
          "Suggesting tail for Log(%" PRIu64 ")@%" PRIu64,
          logid,
          seqno);

        auto ts_it = tail_seqno_cached_.find(logid);
        if (ts_it == tail_seqno_cached_.end()) {
          tail_seqno_cached_.emplace(logid, seqno);
        } else {
          ts_it->second = std::max(ts_it->second, seqno);
        }
      });

      if (!sent) {
        LOG_WARN(info_log_,
          "Failed to send %s@0 sub for HostNum(%d) to TopicTailer worker loop",
          topic.ToString().c_str(),
          hostnum);
      }
    };

    // Check if we already have a good estimate of the tail seqno first.
    bool sent = Forward([this, topic, hostnum, logid, callback] () {
      SequenceNumber tail_seqno = GetTailSeqnoEstimate(logid);
      if (tail_seqno != 0) {
        // Invoke FindLatestSeqno callback immediately.
        stats_.add_subscriber_requests_at_0_fast->Add(1);
        callback(Status::OK(), tail_seqno);
      } else {
        // Otherwise do full FindLatestSeqno request.
        stats_.add_subscriber_requests_at_0_slow->Add(1);
        Status seqno_status = log_tailer_->FindLatestSeqno(logid, callback);
        if (!seqno_status.ok()) {
          LOG_WARN(info_log_,
            "Failed to find latest seqno (%s) for %s",
            seqno_status.ToString().c_str(),
            topic.ToString().c_str());
        } else {
          LOG_INFO(info_log_,
            "Sent FindLatestSeqno request for Hostnum(%d) for %s",
            hostnum,
            topic.ToString().c_str());
        }
      }
    });
    return sent ? Status::OK() : Status::NoBuffer();
  }

  bool sent = Forward([this, logid, topic, start, hostnum] () {
    AddSubscriberInternal(topic, hostnum, logid, start);
  });

  return sent ? Status::OK() : Status::NoBuffer();
}

// Stop reading from this log
Status
TopicTailer::RemoveSubscriber(const TopicUUID& topic, HostNumber hostnum) {
  thread_check_.Check();
  stats_.remove_subscriber_requests->Add(1);

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  bool sent = Forward([this, logid, topic, hostnum] () {
    LOG_DEBUG(info_log_,
      "Hostnum(%d) unsubscribed for %s",
      int(hostnum),
      topic.ToString().c_str());
    RemoveSubscriberInternal(topic, hostnum, logid);
  });

  return sent ? Status::OK() : Status::NoBuffer();
}

bool TopicTailer::Forward(std::function<void()> command) {
  std::unique_ptr<Command> cmd(new ExecuteCommand(std::move(command)));
  Status st = msg_loop_->SendCommand(std::move(cmd), worker_id_);
  return st.ok();
}

std::string TopicTailer::GetLogInfo(LogID log_id) const {
  thread_check_.Check();
  char buffer[256];
  snprintf(buffer, sizeof(buffer),
    "Log(%" PRIu64 ").tail_seqno_cached: %" PRIu64 "\n",
    log_id, GetTailSeqnoEstimate(log_id));
  std::string result = buffer;
  for (auto& reader : log_readers_) {
    result += reader->GetLogInfo(log_id);
  }
  return result;
}

std::string TopicTailer::GetAllLogsInfo() const {
  thread_check_.Check();
  std::string result;
  for (const auto& entry : tail_seqno_cached_) {
    char buffer[256];
    snprintf(buffer, sizeof(buffer),
      "Log(%" PRIu64 ").tail_seqno_cached: %" PRIu64 "\n",
      entry.first, entry.second);
    result += buffer;
  }
  for (auto& reader : log_readers_) {
    result += reader->GetAllLogsInfo();
  }
  return result;
}

void TopicTailer::AddTailSubscriber(const TopicUUID& topic,
                                    HostNumber hostnum,
                                    LogID logid,
                                    SequenceNumber seqno) {
  // Send message to inform subscriber of latest seqno.
  LOG_DEBUG(info_log_,
    "Sending gap message on %s@0-%" PRIu64 " Log(%" PRIu64 ")",
    topic.ToString().c_str(),
    seqno - 1,
    logid);
  Slice namespace_id;
  Slice topic_name;
  topic.GetTopicID(&namespace_id, &topic_name);
  std::unique_ptr<Message> msg(
    new MessageGap(Tenant::GuestTenant,
                   namespace_id.ToString(),
                   topic_name.ToString(),
                   GapType::kBenign,
                   0,
                   seqno - 1));
  on_message_(std::move(msg), { hostnum });

  AddSubscriberInternal(topic, hostnum, logid, seqno);
}

void TopicTailer::AddSubscriberInternal(const TopicUUID& topic,
                                        HostNumber hostnum,
                                        LogID logid,
                                        SequenceNumber seqno) {
  thread_check_.Check();

  // Add the new subscription.
  bool was_added = topic_map_[logid].AddSubscriber(topic, seqno, hostnum);
  if (was_added) {
    stats_.updated_subscriptions->Add(1);
  }

  // Using 'seqno - 1' to ensure that we start reading at a sequence
  // number that exists. FindLatestSeqno returns the *next* seqno to be
  // written to the log.
  SequenceNumber start =
    log_tailer_->CanSubscribePastEnd() ? seqno : seqno - 1;
  LogReader* reader = ReaderForNewSubscription(hostnum, topic, logid, start);
  assert(reader);
  reader->StartReading(topic, logid, start);

  LOG_DEBUG(info_log_,
    "Hostnum(%d) subscribed for %s@%" PRIu64 " (%s) on %sReader(%zu)",
    int(hostnum),
    topic.ToString().c_str(),
    seqno,
    was_added ? "new" : "update",
    reader->IsVirtual() ? "Virtual" : "",
    reader->GetReaderId());
}

void TopicTailer::RemoveSubscriberInternal(const TopicUUID& topic,
                                           HostNumber hostnum,
                                           LogID logid) {
  thread_check_.Check();

  bool all_removed = topic_map_[logid].RemoveSubscriber(topic, hostnum);
  if (all_removed) {
    // No more subscribers left on this topic. Inform readers.
    bool log_closed = true;
    for (auto& reader : log_readers_) {
      reader->StopReading(topic, logid);
      log_closed = log_closed && !reader->IsLogOpen(logid);
    }
    if (log_closed) {
      // Tail seqno cache is no longer being updated, so clear.
      tail_seqno_cached_.erase(logid);
    }
  }
}

LogReader* TopicTailer::FindLogReader(size_t reader_id) {
  // If we get a large number of readers then a better
  // data structure may be necessary.
  for (std::unique_ptr<LogReader>& reader : log_readers_) {
    if (reader->GetReaderId() == reader_id) {
      return reader.get();
    }
  }
  return nullptr;
}


LogReader* TopicTailer::ReaderForNewSubscription(HostNumber hostnum,
                                                 const TopicUUID& topic,
                                                 LogID logid,
                                                 SequenceNumber seqno) {
  // Find the best reader for this subscription.
  // We never rewind a reader until it is merged with another.
  // If a subscription is before the current position of all readers then
  // the subscription is added to pending_reader_. Once a reader merges with
  // another, the merged reader takes over the subscriptions of pending reader.
  // This algorithm only works with > 1 reader, so with one reader we just
  // rewind always. A better algorithm would be to use timers (TODO).
  if (log_readers_.size() == 1) {
    return log_readers_[0].get();
  }
  LogReader* best_reader = pending_reader_.get();
  uint64_t best_cost = kSubscriptionCostRewind;
  for (auto& reader : log_readers_) {
    // Find cost of accepting this new subscription.
    uint64_t reader_cost = reader->SubscriptionCost(topic, logid, seqno);
    if (reader_cost < best_cost) {
      // This is a better reader.
      best_reader = reader.get();
      best_cost = reader_cost;
    }
  }
  return best_reader;
}

void TopicTailer::AttemptReaderMerges(LogReader* src, LogID log_id) {
  // Attempt to merge src reader into all other readers on log_id.
  for (auto& dest : log_readers_) {
    if (src != dest.get() && src->CanMergeInto(dest.get(), log_id)) {
      // Perform merge.
      src->MergeInto(dest.get(), log_id);

      // Now check if there are pending subscriptions on the virtual reader.
      if (pending_reader_->IsLogOpen(log_id)) {
        // We'll subsume the subscriptions from the virtual reader.
        src->StealLogSubscriptions(pending_reader_.get(), log_id);
      }
      break;
    }
  }
}


}  // namespace rocketspeed
