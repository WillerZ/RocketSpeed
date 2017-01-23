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
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/common/thread_check.h"
#include "src/messages/msg_loop.h"
#include "src/messages/observable_map.h"
#include "src/messages/queues.h"

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
   */
  void ProcessRecord(LogID log_id,
                     SequenceNumber seqno,
                     const TopicUUID& topic,
                     SequenceNumber* prev_seqno);

  /**
   * Updates internal state on a gap, and provides gap messages for each
   * affected topic.
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
   * Stops reading a log without throwing away any state.
   * This operation is idempotent.
   *
   * @param log_id ID of the log to pause reading.
   */
  void PauseReading(LogID log_id);

  /**
   * Restarts a reader at its current position.
   * The purpose of this call is to try and save a reader from a bad state by
   * re-issuing the StartReading command. It also gives the storage client a
   * chance to re-balance the reader thread affinity, which can cluster due to
   * our usage pattern.
   *
   * @param log_id ID of the log to restart reading on.
   */
  void RestartReading(LogID log_id);

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
  template <typename OnBump>
  void BumpLaggingSubscriptions(
    LogID log_id,
    SequenceNumber next_seqno,
    const OnBump& on_bump);

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
   * A virtual reader maintains topic subscription state, without having
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
   * Returns next expected sequence number for a log, or 0 if log not open.
   */
  SequenceNumber GetNextSequenceNumber(LogID log_id) const {
    auto it = log_state_.find(log_id);
    if (it != log_state_.end()) {
      return it->second.last_read + 1;
    } else {
      return 0;
    }
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
    // Finds minimum of next_seqno in topics.
    SequenceNumber ComputeStartSeqno() const;

    // State of subscriptions on each topic.
    LinkedMap<TopicUUID, TopicState> topics;

    // Last read sequence number on this log.
    SequenceNumber last_read;

    // If the reader is currently paused or not.
    bool is_reading = false;
  };

  // Starts a log reader at seqno (if not already started).
  void StartLogReader(LogID log_id, LogState& log_state, SequenceNumber seqno);

  // Stops a log reader.
  void StopLogReader(LogID log_id, LogState& log_state);

  // Restarts a log reader, even if already reading.
  void RestartLogReader(LogID log_id, LogState& log_state);

  ThreadCheck thread_check_;
  std::shared_ptr<Logger> info_log_;
  LogTailer* tailer_;
  size_t reader_id_;
  std::unordered_map<LogID, LogState> log_state_;
  int64_t max_subscription_lag_;
};

void LogReader::StartLogReader(LogID log_id,
                               LogState& log_state,
                               SequenceNumber seqno) {
  if (log_state.last_read != seqno - 1 || !log_state.is_reading) {
    log_state.last_read = seqno - 1;
    log_state.is_reading = true;
    if (!IsVirtual()) {
      tailer_->StartReading(log_id, seqno, reader_id_);
    }
  }
}

void LogReader::StopLogReader(LogID log_id, LogState& log_state) {
  if (log_state.is_reading) {
    log_state.is_reading = false;
    if (!IsVirtual()) {
      tailer_->StopReading(log_id, reader_id_);
    }
  }
}

void LogReader::RestartLogReader(LogID log_id,
                                 LogState& log_state) {
  log_state.is_reading = true;
  if (!IsVirtual()) {
    tailer_->StartReading(log_id, log_state.last_read + 1, reader_id_);
  }
}

void LogReader::ProcessRecord(LogID log_id,
                              SequenceNumber seqno,
                              const TopicUUID& topic,
                              SequenceNumber* prev_seqno) {
  thread_check_.Check();

  // Get state for this log.
  auto log_it = log_state_.find(log_id);
  RS_ASSERT(log_it != log_state_.end());

  LogState& log_state = log_it->second;

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

    RS_ASSERT(from == log_state.last_read + 1);

    // Find previous seqno for topic.
    auto it = log_state.topics.find(topic);
    if (it != log_state.topics.end()) {
      *prev_seqno = it->second.next_seqno;
      RS_ASSERT(*prev_seqno != 0);
      it->second.next_seqno = to + 1;
      log_state.topics.move_to_back(it);
    } else {
      *prev_seqno = 0;
    }
  } else {
    RS_ASSERT(false);  // should have been validated before calling this.
  }
}

void LogReader::FlushHistory(LogID log_id, SequenceNumber seqno) {
  thread_check_.Check();
  auto log_it = log_state_.find(log_id);
  if (log_it != log_state_.end()) {
    LogState& log_state = log_it->second;
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

template <typename OnBump>
void LogReader::BumpLaggingSubscriptions(
    LogID log_id,
    SequenceNumber seqno,
    const OnBump& on_bump) {
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

    // Start the LogTailer.
    StartLogReader(log_id, log_state, seqno);
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
        StopLogReader(log_id, log_state);
        RS_ASSERT(log_state.topics.empty());
        log_state_.erase(log_it);
      }
    }
  }
  return st;
}

void LogReader::PauseReading(LogID log_id) {
  thread_check_.Check();
  RS_ASSERT(!IsVirtual());

  auto log_it = log_state_.find(log_id);
  RS_ASSERT(log_it != log_state_.end());
  StopLogReader(log_id, log_it->second);
}

void LogReader::RestartReading(LogID log_id) {
  thread_check_.Check();
  RS_ASSERT(!IsVirtual());

  auto log_it = log_state_.find(log_id);
  RS_ASSERT(log_it != log_state_.end());
  RestartLogReader(log_id, log_it->second);
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
  RS_ASSERT(reader);

  // Cannot merge to/from a virtual reader
  RS_ASSERT(!IsVirtual());
  RS_ASSERT(!reader->IsVirtual());

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
  RS_ASSERT(reader);
  RS_ASSERT(CanMergeInto(reader, log_id));

  // Extract LogStates for this log.
  auto log_it1 = log_state_.find(log_id);
  auto log_it2 = reader->log_state_.find(log_id);
  RS_ASSERT(log_it1 != log_state_.end());
  RS_ASSERT(log_it2 != log_state_.end());

  // Verify last_read.
  LogState& src = log_it1->second;
  LogState& dest = log_it2->second;
  RS_ASSERT(dest.last_read == src.last_read);

  LOG_INFO(info_log_,
    "Merging Reader(%zu) into Reader(%zu) on Log(%" PRIu64 ")@%" PRIu64,
    reader_id_,
    reader->reader_id_,
    log_id,
    src.last_read + 1);

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
  RS_ASSERT(reader->IsVirtual());
  RS_ASSERT(reader->IsLogOpen(log_id));
  RS_ASSERT(!IsVirtual());
  RS_ASSERT(!IsLogOpen(log_id));

  auto log_it = reader->log_state_.find(log_id);
  RS_ASSERT(log_it != reader->log_state_.end());
  LogState& log_state = log_it->second;

  auto start_seqno = log_state.ComputeStartSeqno();
  Status st = tailer_->StartReading(log_id, start_seqno, reader_id_);
  if (st.ok()) {
    RS_ASSERT(!log_state.topics.empty());
    log_state_.emplace(log_id, std::move(log_state));
    reader->log_state_.erase(log_it);
  } else {
    LOG_ERROR(info_log_,
      "Reader(%zu) failed to start reading Log(%" PRIu64 ")@%" PRIu64 ": %s",
      reader_id_,
      log_id,
      start_seqno,
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
      "Log(%" PRIu64 ").reader[%zu].last_read: %" PRIu64 "\n"
      "Log(%" PRIu64 ").reader[%zu].num_topics_subscribed: %zu\n",
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

SequenceNumber LogReader::LogState::ComputeStartSeqno() const {
  SequenceNumber min_seqno = std::numeric_limits<SequenceNumber>::max();
  RS_ASSERT(!topics.empty());
  for (const auto& entry : topics) {
    min_seqno = std::min(min_seqno, entry.second.next_seqno);
  }
  return min_seqno;
}

TopicTailer::TopicTailer(
    BaseEnv* env,
    MsgLoop* msg_loop,
    int worker_id,
    LogTailer* log_tailer,
    std::shared_ptr<LogRouter> log_router,
    std::shared_ptr<Logger> info_log,
    size_t cache_size_per_room,
    bool cache_data_from_system_namespaces,
    size_t cache_block_size,
    int bloom_bits_per_msg,
    std::function<void(Flow*,
                       const Message&,
                       std::vector<CopilotSub>)> on_message,
    std::function<int(const CopilotSub&)> copilot_worker,
    ControlTowerOptions::TopicTailer options) :
  env_(env),
  msg_loop_(msg_loop),
  worker_id_(worker_id),
  log_tailer_(log_tailer),
  log_router_(std::move(log_router)),
  info_log_(std::move(info_log)),
  on_message_(std::move(on_message)),
  data_cache_(cache_size_per_room, cache_data_from_system_namespaces,
              bloom_bits_per_msg, cache_block_size),
  prng_(ThreadLocalPRNG()),
  options_(options),
  event_loop_(msg_loop_->GetEventLoop(worker_id_)),
  copilot_worker_(std::move(copilot_worker)) {

  latest_seqno_queues_.reset(
    new ThreadLocalQueues<FindLatestSeqnoResponse>(
      [this] () {
        return InstallSPSCQueue<FindLatestSeqnoResponse>(
          event_loop_,
          info_log_,
          options_.max_find_time_requests, // queue size = max inflight requests
          [this] (Flow* flow, FindLatestSeqnoResponse response) {
            ProcessFindLatestSeqnoResponse(flow, std::move(response));
          });
      }));

  reentry_cache_readers_ =
    std::make_shared<ObservableMap<LogReaderId, std::nullptr_t>>();

  // Setup processor for re-entry cache readers.
  // These are readers that started reading a log, read some from log storage,
  // found data in the cache, but hit backpressure while in the cache.
  // This processor will retry reading from the cache later.
  InstallSource<std::pair<LogReaderId, std::nullptr_t>>(
    event_loop_,
    reentry_cache_readers_.get(),
    [this] (Flow* flow, std::pair<LogReaderId, std::nullptr_t> item) {
      SendCacheRecord(flow, item.first.log_id, item.first.reader);
    });
}

TopicTailer::~TopicTailer() {
}

void TopicTailer::SendFindLatestSeqnoRequest(LogID logid) {
  // Sanity check that we aren't sending more than required.
  RS_ASSERT(InFlightFindLatestSeqnoRequests() <=
    options_.max_find_time_requests);

  // Create a callback to enqueue a subscribe command.
  auto callback = [this, logid] (Status status, SequenceNumber seqno) {
    // Send response back to room.
    FindLatestSeqnoResponse response { status, logid, seqno };
    bool sent = latest_seqno_queues_->GetThreadLocal()->Write(response);
    RS_ASSERT(sent);  // gating logic should prevent queue overflow
    if (!sent) {
      LOG_ERROR(info_log_,
        "Failed to send FindLatestSeqnoResponse for Log(%" PRIu64 ")",
        logid);
    }
  };

  Status seqno_status = log_tailer_->FindLatestSeqno(logid, callback);
  if (!seqno_status.ok()) {
    LOG_ERROR(info_log_,
      "Failed to find latest seqno (%s) for Log(%" PRIu64 ")",
      seqno_status.ToString().c_str(),
      logid);
    // TODO: gating + retries
  } else {
    LOG_INFO(info_log_,
      "Sent FindLatestSeqno request on Log(%" PRIu64 ")",
      logid);
  }
}

void TopicTailer::ProcessFindLatestSeqnoResponse(Flow* flow,
                                                 FindLatestSeqnoResponse resp) {
  thread_check_.Check();

  auto status = resp.status;
  auto logid = resp.log_id;
  auto seqno = resp.seqno;

  if (!status.ok()) {
    LOG_WARN(info_log_,
      "Failed to find latest sequence number in Log(%" PRIu64 "): %s",
      logid,
      status.ToString().c_str());

    // Retry.
    SendFindLatestSeqnoRequest(logid);
    return;
  }

  // Find all copilots pending this response.
  for (auto id : pending_find_time_response_[logid]) {
    // IMPORTANT: Since this callback is asynchronous, the subscriber
    // may have unsubscribed since they issued the subscribe(0) request.
    // We need to check this otherwise we may open a reader for a
    // non-existent subscription, which will never be closed.
    TopicUUID* topic = stream_subscriptions_.Find(id.stream_id, id.sub_id);
    if (topic) {
      // Subscription exists: add subscriber.
      AddTailSubscriber(flow, *topic, id, logid, seqno);
    } else {
      LOG_DEBUG(info_log_,
        "%s unsubscribed before FindLatestSeqno response arrived.",
        id.ToString().c_str());
    }
  }
  pending_find_time_response_.erase(logid);

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

  // One less request in flight now, so send any pending requests that were
  // blocked due to having too many in flight.
  if (!pending_find_time_requests_.empty()) {
    LogID next_request_log_id = pending_find_time_requests_.front();
    pending_find_time_requests_.pop_front();
    SendFindLatestSeqnoRequest(next_request_log_id);

    // There should not have been any pending requests unless the number in
    // flight was too high. It should now be at the limit again after sending
    // another.
    RS_ASSERT(InFlightFindLatestSeqnoRequests() ==
           options_.max_find_time_requests);
  }
}

size_t TopicTailer::InFlightFindLatestSeqnoRequests() const {
  return pending_find_time_response_.size() -
         pending_find_time_requests_.size();
}


void TopicTailer::ReceiveLogRecord(std::unique_ptr<MessageData> data,
                                   LogID log_id,
                                   LogReader* reader,
                                   Flow* flow) {
  // This portion of code is invoked in the room-thread.
  thread_check_.Check();

  // Process message from the log tailer.
  stats_.log_records_received->Add(1);
  stats_.log_records_received_payload_size->Add(data->GetPayload().
                                                size());
  TopicUUID uuid(data->GetNamespaceId(), data->GetTopicName());
  SequenceNumber next_seqno = data->GetSequenceNumber();
  SequenceNumber prev_seqno = 0;
  RS_ASSERT(next_seqno == reader->GetNextSequenceNumber(log_id));
  reader->ProcessRecord(log_id, next_seqno, uuid, &prev_seqno);
  if (0) {
    LOG_DEBUG(info_log_,
              "Inserted seqno %" PRIu64 " on Log(%" PRIu64 ")"
              " Topic(%s, %s)",
              next_seqno,
              log_id,
              data->GetNamespaceId().ToString().c_str(),
              data->GetTopicName().ToString().c_str());
  }

  auto ts_it = tail_seqno_cached_.find(log_id);
  bool is_tail = false;
  if (ts_it != tail_seqno_cached_.end() && ts_it->second <= next_seqno) {
    // If we had an estimate on the tail sequence number and it was lower
    // than this record, then update the estimate.
    is_tail = true;
    ts_it->second = next_seqno + 1;
  }

  if (is_tail) {
    stats_.tail_records_received->Add(1);
  } else {
    stats_.backlog_records_received->Add(1);
  }

  if (prev_seqno != 0) {
    // Find subscribed hosts.
    TopicManager& topic_manager = topic_map_[log_id];

    std::vector<CopilotSub> recipients;
    topic_manager.VisitSubscribers(
      uuid, prev_seqno, next_seqno,
      [&] (TopicSubscription* sub) {
        const CopilotSub id = sub->GetID();
        recipients.emplace_back(id);
        sub->SetSequenceNumber(next_seqno + 1);
        LOG_DEBUG(info_log_,
          "%s advanced to %s@%" PRIu64 " on Log(%" PRIu64 ")"
          " Reader(%zu)",
          id.ToString().c_str(),
          uuid.ToString().c_str(),
          next_seqno + 1,
          log_id,
          reader->GetReaderId());
      });

    if (!recipients.empty()) {
      // Modify message and send it out.
      data->SetPreviousSequenceNumber(prev_seqno);
      stats_.log_records_with_subscriptions->Add(1);
      on_message_(flow, *data, std::move(recipients));
    } else {
      stats_.log_records_without_subscriptions->Add(1);
      LOG_DEBUG(info_log_,
        "Reader(%zu) found no hosts for %smessage on %s@%" PRIu64 "-%" PRIu64,
        reader->GetReaderId(),
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
        std::vector<CopilotSub> bumped_subscriptions;
        topic_manager.VisitSubscribers(
          topic, bump_seqno, next_seqno,
          [&] (TopicSubscription* sub) {
            const CopilotSub id = sub->GetID();
            // Add host to list.
            bumped_subscriptions.emplace_back(id);

            // Advance subscription.
            sub->SetSequenceNumber(next_seqno + 1);
            LOG_DEBUG(info_log_,
              "%s bumped to %s@%" PRIu64 " on Log(%" PRIu64 ")"
              " Reader(%zu)",
              id.ToString().c_str(),
              topic.ToString().c_str(),
              next_seqno + 1,
              log_id,
              reader->GetReaderId());
          });

        if (!bumped_subscriptions.empty()) {
          // Send gap message.
          Slice namespace_id;
          Slice topic_name;
          topic.GetTopicID(&namespace_id, &topic_name);
          MessageGap trim_msg(Tenant::GuestTenant,
                           namespace_id.ToString(),
                           topic_name.ToString(),
                           GapType::kBenign,
                           bump_seqno,
                           next_seqno);
          stats_.bumped_subscriptions->Add(bumped_subscriptions.size());
          on_message_(flow, trim_msg, std::move(bumped_subscriptions));
        }
      });
  }

  // Transfer ownership of this message to the cache.
  Slice namespace_id = data->GetNamespaceId();
  Slice topic_name = data->GetTopicName();
  data_cache_.StoreData(namespace_id, topic_name, log_id, std::move(data));
}

TopicTailer::CacheRead
TopicTailer::AdvanceReaderFromCache(Flow* flow,
                                    LogID log_id,
                                    LogReader* reader) {
  thread_check_.Check();

  // if cache is not enabled, then short-circuit
  if (data_cache_.GetCapacity() == 0) {
    return CacheRead::kNoneRead;
  }

  SequenceNumber seqno = reader->GetNextSequenceNumber(log_id);
  if (seqno == 0) {
    // Log not open.
    return CacheRead::kNoneRead;
  }

  bool backoff = false;

  // callback to process a data message from cache
  auto on_message_cache = [&] (MessageData* data, bool* delivered) {
    TopicUUID uuid(data->GetNamespaceId(), data->GetTopicName());
    SequenceNumber next_seqno = data->GetSequenceNumber();
    SequenceNumber prev_seqno = 0;
    RS_ASSERT(next_seqno >= reader->GetNextSequenceNumber(log_id));
    reader->ProcessRecord(log_id, next_seqno, uuid, &prev_seqno);

    // However, may still be no subscribers for this topic.
    if (prev_seqno != 0) {
      // Find subscribed hosts.
      std::vector<CopilotSub> recipients;
      topic_map_[log_id].VisitSubscribers(
        uuid, prev_seqno, next_seqno,
        [&] (TopicSubscription* sub) {
          const CopilotSub id = sub->GetID();
          recipients.emplace_back(id);
          sub->SetSequenceNumber(next_seqno + 1);
          LOG_DEBUG(info_log_,
            "%s advanced to %s@%" PRIu64 " on Log(%" PRIu64 ")"
            " Reader(%zu) by cache",
            id.ToString().c_str(),
            uuid.ToString().c_str(),
            next_seqno + 1,
            log_id,
            reader->GetReaderId());
        });

      if (!recipients.empty()) {
        // Modify message and send it out.
        data->SetPreviousSequenceNumber(prev_seqno);
        on_message_(flow, *data, std::move(recipients));
        stats_.records_served_from_cache->Add(1);
        *delivered = true;          // delivered this message
      }
    }
    // For flow control, we stop reading from the cache if any write failed.
    // The cache will be rechecked once the backpressure is lifted.
    RS_ASSERT(!backoff);
    backoff = flow->WriteHasFailed();
    return !backoff;
  };

  // Scan the list of subscribers for this log. If and only if there is
  // precisely one subscribed topic, remember it.
  Slice lookup_topic;
  int topic_count = 0;
  topic_map_[log_id].VisitTopics(
    [&] (const TopicUUID& topic) {
      Slice namespace_id;
      Slice topic_name;
      topic.GetTopicID(&namespace_id, &topic_name);
      topic_count++;
      if (topic_count > 1) {
        lookup_topic.clear();
        return false;    // no need to look at more topics
      }
      lookup_topic = topic_name; // store first topic name
      return true;
    });
  // If there is only one subscribed topic in this log,
  // then we use that topic name to match bloom filters.

  // Deliver as much data as possible from the cache.
  SequenceNumber old = seqno;
  seqno = data_cache_.VisitCache(log_id, seqno, lookup_topic,
                                 std::move(on_message_cache));

  if (old != seqno) {
    stats_.cache_reentries->Add(1);
    return backoff ? CacheRead::kReadBackoff : CacheRead::kReadContinue;
  } else {
    RS_ASSERT(!backoff);  // how can we hit backoff without reading anything?
    return CacheRead::kNoneRead;
  }
}

void TopicTailer::SendCacheRecord(Flow* flow,
                                  LogID log_id,
                                  LogReader* reader) {
  if (!reader->IsLogOpen(log_id)) {
    // Reader no longer open.
    return;
  }

  // Check if we are now in the cache, and deliver messages from cache
  // if available.
  switch (AdvanceReaderFromCache(flow, log_id, reader)) {
    case CacheRead::kReadContinue:
    case CacheRead::kNoneRead:
      // No backoff, so attempt merge, otherwise resume reading.
      if (!AttemptReaderMerges(reader, log_id)) {
        // Did not merge with another reader, so RestartReading at the
        // new sequence number for this log (stop + start).
        LOG_INFO(info_log_,
          "Restarting @%" PRIu64 " after reading from Log(%" PRIu64 ") cache.",
          reader->GetNextSequenceNumber(log_id),
          log_id);
        reader->RestartReading(log_id);
      }
      break;

    case CacheRead::kReadBackoff:
      // Messages were delivered from the cache, but backpressure was
      // applied before reaching the end of the cache.
      // Stop the log reader for now, and try reading cache later.
      LOG_INFO(info_log_,
        "Backing off @%" PRIu64 " after reading from Log(%" PRIu64 ") cache.",
        reader->GetNextSequenceNumber(log_id),
        log_id);
      reader->PauseReading(log_id);
      reentry_cache_readers_->Write(LogReaderId(log_id, reader), nullptr);
      break;
  }
}

void TopicTailer::SendLogRecord(
    Flow* flow,
    std::unique_ptr<MessageData>& msg,
    LogID log_id,
    size_t reader_id) {
  thread_check_.Check();

  // Find reader.
  LogReader* reader = FindLogReader(reader_id);
  RS_ASSERT(reader != nullptr);

  // Update state for this log and distribute message.
  ReceiveLogRecord(std::move(msg), log_id, reader, flow);

  // Check if we can advance from cache.
  SequenceNumber seqno = reader->GetNextSequenceNumber(log_id);
  RS_ASSERT(seqno != 0);
  if (data_cache_.HasEntry(log_id, seqno)) {
    // Pause reading log, and start reading from cache.
    reader->PauseReading(log_id);
    reentry_cache_readers_->Write(LogReaderId(log_id, reader), nullptr);
  } else {
    // See if we can merge with another reader.
    AttemptReaderMerges(reader, log_id);
  }
}

void TopicTailer::SendGapRecord(
    Flow* flow,
    LogID log_id,
    GapType type,
    SequenceNumber from,
    SequenceNumber to,
    size_t reader_id) {
  thread_check_.Check();

  LogReader* reader = FindLogReader(reader_id);
  RS_ASSERT(reader != nullptr);

  stats_.gap_records_received->Add(1);

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
      std::vector<CopilotSub> recipients;
      topic_map_[log_id].VisitSubscribers(
        topic, prev_seqno, to,
        [&] (TopicSubscription* sub) {
          recipients.emplace_back(sub->GetID());
          sub->SetSequenceNumber(to + 1);
          LOG_DEBUG(info_log_,
            "%s advanced to %s@%" PRIu64 " on Log(%" PRIu64 ")"
            " Reader(%zu)",
            sub->GetID().ToString().c_str(),
            topic.ToString().c_str(),
            to,
            log_id,
            reader_id);
        });

      // Send message.
      if (!recipients.empty()){
        Slice namespace_id;
        Slice topic_name;
        topic.GetTopicID(&namespace_id, &topic_name);
        MessageGap mgap(Tenant::GuestTenant,
                         namespace_id.ToString(),
                         topic_name.ToString(),
                         type,
                         prev_seqno,
                         to);
        stats_.gap_records_with_subscriptions->Add(1);
        on_message_(flow, mgap, std::move(recipients));
      } else {
        stats_.gap_records_without_subscriptions->Add(1);
      }
      return true;  // continue to iterate to the next topic
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
}

void TopicTailer::Tick() {
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
    size_t cache_size_per_room,
    bool cache_data_from_system_namespaces,
    size_t cache_block_size,
    int bloom_bits_per_msg,
    std::function<void(Flow*,
                       const Message&,
                       std::vector<CopilotSub>)> on_message,
    std::function<int(const CopilotSub&)> copilot_worker,
    ControlTowerOptions::TopicTailer options,
    TopicTailer** tailer) {
  *tailer = new TopicTailer(env,
                            msg_loop,
                            worker_id,
                            log_tailer,
                            std::move(log_router),
                            std::move(info_log),
                            cache_size_per_room,
                            cache_data_from_system_namespaces,
                            cache_block_size,
                            bloom_bits_per_msg,
                            std::move(on_message),
                            std::move(copilot_worker),
                            options);
  return Status::OK();
}

Status TopicTailer::AddSubscriber(const TopicUUID& topic,
                                  SequenceNumber start,
                                  CopilotSub id) {
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

    // Check if we already have a good estimate of the tail seqno first.
    SequenceNumber tail_seqno = GetTailSeqnoEstimate(logid);
    if (tail_seqno != 0) {
      // Can add subscriber immediately.
      stats_.add_subscriber_requests_at_0_fast->Add(1);

      // Using SourcelessFlow here until we have flow from the socket, which
      // would be passed into AddSubscriber.
      // TODO(pja) T8668773.
      SourcelessFlow no_flow(
          msg_loop_->GetEventLoop(worker_id_)->GetFlowControl());
      AddTailSubscriber(&no_flow, topic, id, logid, tail_seqno);
    } else {
      // Otherwise do full FindLatestSeqno request.
      stats_.add_subscriber_requests_at_0_slow->Add(1);

      // First insert into the stream subscriptions map to indicate that the
      // subscription exists (and allow unsubscriptions to work).
      stream_subscriptions_.Insert(id.stream_id, id.sub_id, topic);

      // Add to the list of copilots waiting on a FindLatestSeqno request.
      const size_t in_flight = InFlightFindLatestSeqnoRequests();
      auto& pending = pending_find_time_response_[logid];
      pending.emplace_back(id);

      if (pending.size() > 1) {
        // Already a FindLatestSeqno request in flight, so we'll just wait for
        // that to finish and use the same result.
        LOG_DEBUG(info_log_,
          "Piggy-backing in flight FindLatestSeqno request on Log(%" PRIu64 ")"
          " for %s",
          logid,
          id.ToString().c_str());
      } else if (in_flight < options_.max_find_time_requests) {
        // Not too many in flight, so send the request now.
        SendFindLatestSeqnoRequest(logid);
      } else {
        // Too many FindLatestSeqno requests in flight.
        // Mark it pending for later. These will be picked up when the
        // next response comes back.
        if (!pending_find_time_requests_.contains(logid)) {
          pending_find_time_requests_.emplace_back(logid);
        }
      }
    }
  } else {
    // Non-zero sequence number.
    AddSubscriberInternal(topic, id, logid, start);
  }
  return Status::OK();
}

// Stop reading from this log
Status TopicTailer::RemoveSubscriber(CopilotSub id) {
  thread_check_.Check();
  stats_.remove_subscriber_requests->Add(1);

  TopicUUID topic;
  if (!stream_subscriptions_.MoveOut(id.stream_id, id.sub_id, &topic)) {
    LOG_WARN(info_log_,
      "Cannot remove unknown subscription %s",
      id.ToString().c_str());
    return Status::NotFound();
  }

  // Map topic to log.
  LogID logid;
  Status st = log_router_->GetLogID(topic, &logid);
  if (!st.ok()) {
    return st;
  }

  LOG_DEBUG(info_log_,
    "%s unsubscribed for %s",
    id.ToString().c_str(),
    topic.ToString().c_str());
  RemoveSubscriberInternal(topic, id, logid);
  return Status::OK();
}

Status TopicTailer::RemoveSubscriber(StreamID stream_id) {
  thread_check_.Check();
  LOG_DEBUG(info_log_, "StreamID(%" PRIu64 ") unsubscribed for all topics", stream_id);
  RemoveSubscriberInternal(stream_id);
  return Status::OK();
}

std::string TopicTailer::ClearCache() {
  thread_check_.Check();
  LOG_INFO(info_log_, "Clearing cache for worker_id %d", worker_id_);
  data_cache_.ClearCache();
  return "";
}

std::string TopicTailer::SetCacheCapacity(size_t newcapacity) {
  thread_check_.Check();
  LOG_INFO(info_log_, "Setting new cache capacity %lu for worker_id %d",
           newcapacity, worker_id_);
  data_cache_.SetCapacity(newcapacity);
  return "";
}

std::string TopicTailer::GetCacheUsage() {
  thread_check_.Check();
  return std::to_string(data_cache_.GetUsage());
}

std::string TopicTailer::GetCacheCapacity() {
  thread_check_.Check();
  return std::to_string(data_cache_.GetCapacity());
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

void TopicTailer::AddTailSubscriber(Flow* flow,
                                    const TopicUUID& topic,
                                    CopilotSub id,
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
  MessageGap mgap(Tenant::GuestTenant,
                   namespace_id.ToString(),
                   topic_name.ToString(),
                   GapType::kBenign,
                   0,
                   seqno - 1);
  on_message_(flow, mgap, { id });

  AddSubscriberInternal(topic, id, logid, seqno);
}

bool TopicTailer::DeliverFromCache(Flow* flow,
                                   const TopicUUID& topic,
                                   CopilotSub copilot,
                                   LogID logid,
                                   SequenceNumber* seqno) {
  // if cache is not enabled, then short-circuit
  if (data_cache_.GetCapacity() == 0) {
    return true;
  }

  RS_ASSERT(seqno);
  RS_ASSERT(*seqno != 0);
  thread_check_.Check();
  SequenceNumber delivered = *seqno;
  std::vector<CopilotSub> recipient;
  recipient.emplace_back(copilot);
  bool backoff = false;

  // callback to process a data message from cache
  auto on_message_cache =
    [&] (MessageData* data_raw, bool* sent) {

    auto uuid_pair = std::make_pair(data_raw->GetNamespaceId(),
                                    data_raw->GetTopicName());
    auto msg_seqno = data_raw->GetSequenceNumber();
    RS_ASSERT(msg_seqno >= *seqno);

    LOG_DEBUG(info_log_,
        "CacheTailer received data (%.16s)@%" PRIu64
        " for Topic(%s,%s) in Log(%" PRIu64 ").",
        data_raw->GetPayload().ToString().c_str(),
        msg_seqno,
        data_raw->GetNamespaceId().ToString().c_str(),
        data_raw->GetTopicName().ToString().c_str(),
        logid);

    // If this message is for our topic, then deliver
    if (topic == uuid_pair) {
      this->stats_.records_served_from_cache->Add(1);
      if (0) {
        LOG_DEBUG(info_log_,
                  "Delivering data to %s@%" PRIu64 " on Log(%" PRIu64
                  ") from cache",
                  topic.ToString().c_str(),
                  msg_seqno,
                  logid);
      }
      // Deliver message
      data_raw->SetPreviousSequenceNumber(delivered);
      delivered = msg_seqno + 1;

      // When reading from cache, flow control is implemented by sending as
      // much as we can until the sink rejects the writes. At that point,
      // we stop reading and will retry later.
      on_message_(flow, *data_raw, recipient);
      *sent = true;                     // message delivered
      RS_ASSERT(!backoff);
      backoff = flow->WriteHasFailed();
      return !backoff;  // if false, stop reading from cache.
    }
    return true;
  };

  Slice ns, topic_name;
  topic.GetTopicID(&ns, &topic_name);

  // Deliver as much data as possible from the cache.
  SequenceNumber old = *seqno;
  *seqno = data_cache_.VisitCache(logid, *seqno, topic_name,
                                  std::move(on_message_cache));

  if (backoff) {
    // Backpressure was requested while iterating the cache, causing us to
    // exit early. Instead of delivering the gap and opening the reader, we
    // should retry delivering from the cache later.
    return false;
  }

  // If there a gap between the last message delivered from the cache
  // and the largest seqno number in cache, then deliver a gap.
  if (*seqno > delivered) {
    if (0) {
      LOG_DEBUG(info_log_,
                "Delivering gap to %s(@%" PRIu64 "-%" PRIu64
                ") on Log(%" PRIu64 ") from cache",
                topic_name.ToString().c_str(),
                delivered,
                *seqno - 1,
                logid);
    }
    MessageGap mgap(Tenant::GuestTenant,
                    ns.ToString(),
                    topic_name.ToString(),
                    GapType::kBenign,
                    delivered,
                    *seqno-1);
    // Since there is a single gap message sent per subscription, we can
    // ignore backpressure: the size is bounded by number of subscriptions.
    SourcelessFlow noflow(
        msg_loop_->GetEventLoop(worker_id_)->GetFlowControl());
    on_message_(&noflow, mgap, recipient);
  }
  if (old != *seqno) {
    LOG_DEBUG(info_log_,
      "Subscription(%s) subscription fastforward %s from %" PRIu64
      " to %" PRIu64,
      copilot.ToString().c_str(),
      topic_name.ToString().c_str(),
      old,
      *seqno);
  }
  return true;
}

void TopicTailer::ProcessPendingSubscription(Flow* flow,
                                             const TopicUUID& topic,
                                             CopilotSub id,
                                             LogID logid,
                                             SequenceNumber seqno) {
  RS_ASSERT(seqno != 0);
  thread_check_.Check();

  // Deliver the earliest part of this topic from cache if available.
  if (DeliverFromCache(flow, topic, id, logid, &seqno)) {
    // Done delivering what we can from cache, open reader for the rest.
    LogReader* reader = ReaderForNewSubscription(id, topic, logid, seqno);
    RS_ASSERT(reader);
    reader->StartReading(topic, logid, seqno);

    // Add the new subscription.
    bool was_added = topic_map_[logid].AddSubscriber(topic, seqno, id);
    if (was_added) {
      stats_.updated_subscriptions->Add(1);
    }

    LOG_DEBUG(info_log_,
      "%s subscribed for %s@%" PRIu64 " (%s) on %sReader(%zu)",
      id.ToString().c_str(),
      topic.ToString().c_str(),
      seqno,
      was_added ? "new" : "update",
      reader->IsVirtual() ? "Virtual" : "",
      reader->GetReaderId());
  } else {
    // Didn't deliver everything we could from the cache, so add to the cache
    // readers list. This will be checked later to see if we can deliver more.
    GetPendingReaderQueue(id)->Write(id, PendingSubscription(logid, seqno));

    LOG_INFO(info_log_,
      "Failed to deliver all from cache for %s on %s (will retry later)",
      id.ToString().c_str(),
      topic.ToString().c_str());
    stats_.cache_reader_backoff->Add(1);
  }
}

void TopicTailer::AddSubscriberInternal(const TopicUUID& topic,
                                        CopilotSub id,
                                        LogID logid,
                                        SequenceNumber seqno) {
  RS_ASSERT(seqno != 0);
  thread_check_.Check();

  GetPendingReaderQueue(id)->Write(id, PendingSubscription(logid, seqno));
  stream_subscriptions_.Insert(id.stream_id, id.sub_id, topic);
}

void TopicTailer::RemoveSubscriberInternal(const TopicUUID& topic,
                                           CopilotSub id,
                                           LogID logid) {
  thread_check_.Check();

  bool all_removed = topic_map_[logid].RemoveSubscriber(topic, id);
  if (all_removed) {
    // No more subscribers left on this topic. Inform readers.
    bool log_closed = true;
    for (auto& reader : log_readers_) {
      reader->StopReading(topic, logid);
      log_closed = log_closed && !reader->IsLogOpen(logid);
    }
    pending_reader_->StopReading(topic, logid);
    log_closed = log_closed && !pending_reader_->IsLogOpen(logid);

    if (log_closed) {
      // Tail seqno cache is no longer being updated, so clear.
      tail_seqno_cached_.erase(logid);
    }
  }

  GetPendingReaderQueue(id)->Remove(id);
}

void TopicTailer::RemoveSubscriberInternal(StreamID stream_id) {
  thread_check_.Check();

  // Remove all subscriptions on this stream.
  stream_subscriptions_.VisitSubscriptions(
    stream_id,
    [&] (SubscriptionID sub_id, const TopicUUID& topic) {
      LogID log_id;
      Status st = log_router_->GetLogID(topic, &log_id);
      if (st.ok()) {
        CopilotSub id(stream_id, sub_id);
        RemoveSubscriberInternal(topic, id, log_id);
      }
    });

  stream_subscriptions_.Remove(stream_id);
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


LogReader* TopicTailer::ReaderForNewSubscription(CopilotSub id,
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

bool TopicTailer::AttemptReaderMerges(LogReader* src, LogID log_id) {
  // Attempt to merge src reader into all other readers on log_id.
  for (auto& dest : log_readers_) {
    if (src != dest.get() && src->CanMergeInto(dest.get(), log_id)) {
      // Perform merge.
      src->MergeInto(dest.get(), log_id);
      stats_.reader_merges->Add(1);

      // Now check if there are pending subscriptions on the virtual reader.
      if (pending_reader_->IsLogOpen(log_id)) {
        // We'll subsume the subscriptions from the virtual reader.
        src->StealLogSubscriptions(pending_reader_.get(), log_id);
      }
      return true;
    }
  }
  return false;
}

Statistics TopicTailer::GetStatistics() const {
  stats_.cache_usage->Set(data_cache_.GetUsage()); // update cache statistics
  Statistics stats = stats_.all;
  stats.Aggregate(data_cache_.GetStatistics());
  return stats;
}

ObservableMap<CopilotSub, TopicTailer::PendingSubscription>*
TopicTailer::GetPendingReaderQueue(const CopilotSub& sub) {
  int worker_id = copilot_worker_(sub);
  if (worker_id == -1) {
    // This should never happen, all subscriptions should have a worker.
    RS_ASSERT(false);
    return 0;
  }
  if (static_cast<size_t>(worker_id) >= cache_readers_.size()) {
    cache_readers_.resize(worker_id + 1);
  }
  if (!cache_readers_[worker_id]) {
    // Construct new queue.
    cache_readers_[worker_id] =
      std::make_shared<ObservableMap<CopilotSub, PendingSubscription>>();

    // Setup processor for the source.
    InstallSource<std::pair<CopilotSub, PendingSubscription>>(
      event_loop_,
      cache_readers_[worker_id].get(),
      [this] (Flow* flow, std::pair<CopilotSub, PendingSubscription> item) {
        const auto& id = item.first;
        TopicUUID* uuid = stream_subscriptions_.Find(id.stream_id, id.sub_id);
        if (uuid) {
          const LogID logid = item.second.logid;
          const SequenceNumber seqno = item.second.seqno;

          LOG_DEBUG(info_log_,
            "Retrying delivery for %s on %s@%" PRIu64,
            id.ToString().c_str(),
            uuid->ToString().c_str(),
            seqno);

          ProcessPendingSubscription(flow, *uuid, id, logid, seqno);
        }
      });
  }
  return cache_readers_[worker_id].get();
}

}  // namespace rocketspeed
