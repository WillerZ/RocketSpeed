// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <random>
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/storage.h"
#include "src/util/subscription_map.h"
#include "src/util/topic_uuid.h"
#include "src/util/common/statistics.h"
#include "src/util/common/thread_check.h"
#include "src/controltower/options.h"
#include "src/controltower/topic.h"
#include "src/controltower/tower.h"

namespace rocketspeed {

class LogTailer;
class LogRouter;
class LogReader;
class MsgLoop;

class TopicTailer {
 friend class ControlTowerTest;
 public:
  /**
   * Create a TopicTailer.
   *
   * @param env Environment.
   * @param msg_loop MsgLoop this tailer belongs to.
   * @param worker_id Worker index tailer runs on.
   * @param log_tailer Tailer for logs. Will be manipulated by TopicTailer.
   * @param log_router For routing topics to logs.
   * @param info_log For logging.
   * @param on_message Callback for Deliver and Gap messages.
   * @param tailer Output parameter for created TopicTailer.
   * @return ok() if TopicTailer created, otherwise error.
   */
  static Status CreateNewInstance(
    BaseEnv* env,
    MsgLoop* msg_loop,
    int worker_id,
    LogTailer* log_tailer,
    std::shared_ptr<LogRouter> log_router,
    std::shared_ptr<Logger> info_log,
    std::function<void(std::unique_ptr<Message>,
                       std::vector<CopilotSub>)> on_message,
    ControlTowerOptions::TopicTailer options,
    TopicTailer** tailer);

  /**
   * Initialize the TopicTailer first before using it.
   *
   * @param reader_ids IDs of readers on LogTailer.
   * @param max_subscription_lag Maximum number of sequence numbers that a
   *                             subscription can lag behind before being sent
   *                             a gap.
   * @return ok if successful, otherwise error code.
   */
  Status Initialize(const std::vector<size_t>& reader_ids,
                    int64_t max_subscription_lag);

  /**
   * Adds a subscriber to a topic. This call is not thread-safe.
   */
  Status AddSubscriber(const TopicUUID& topic,
                       SequenceNumber start,
                       CopilotSub id);

  /**
   * Removes a single subscription. This call is not thread-safe.
   */
  Status RemoveSubscriber(CopilotSub id);

  /**
   * Removes all subscriptions for a stream.
   */
  Status RemoveSubscriber(StreamID stream_id);

  /**
   * Process a data record from a log tailer, and forward to on_message.
   *
   * @param msg Log record message.
   * @param log_id ID of log record resides in.
   * @param reader_id ID of reader delivering the record.
   * @return OK() if record was sent for processing.
   */
  Status SendLogRecord(
    std::unique_ptr<MessageData>& msg,
    LogID log_id,
    size_t reader_id);

  /**
   * Process a gap record from a log tailer, and forward to on_message.
   *
   * @param log_id ID of log containing the gap.
   * @param type The type of gap encountered.
   * @param from The first gap sequence number (inclusive).
   * @param to The last gap sequence number (inclusive).
   * @param reader_id ID of reader that encountered the gap.
   * @return OK() if record was sent for processing.
   */
  Status SendGapRecord(
    LogID log_id,
    GapType type,
    SequenceNumber from,
    SequenceNumber to,
    size_t reader_id);

  const Statistics& GetStatistics() const {
    return stats_.all;
  }

  /**
   * Get an estimate of tail seqno for a log, or 0 if unknown.
   */
  SequenceNumber GetTailSeqnoEstimate(LogID log_id) const;

  /**
   * Get human-readable information about a particular log.
   */
  std::string GetLogInfo(LogID log_id) const;

  /**
   * Get human-readable information about all logs.
   */
  std::string GetAllLogsInfo() const;

  ~TopicTailer();

 private:
  typedef std::function<void()> TopicTailerCommand;

  // private constructor
  TopicTailer(BaseEnv* env,
              MsgLoop* msg_loop,
              int worker_id,
              LogTailer* log_tailer,
              std::shared_ptr<LogRouter> log_router,
              std::shared_ptr<Logger> info_log,
              std::function<void(std::unique_ptr<Message>,
                                 std::vector<CopilotSub>)> on_message,
              ControlTowerOptions::TopicTailer options);

  template <typename Function>
  bool Forward(Function command);

  bool Forward(std::unique_ptr<Command> command);

  void AddTailSubscriber(const TopicUUID& topic,
                         CopilotSub id,
                         LogID logid,
                         SequenceNumber seqno);

  void AddSubscriberInternal(const TopicUUID& topic,
                             CopilotSub id,
                             LogID logid,
                             SequenceNumber start);

  void RemoveSubscriberInternal(const TopicUUID& topic,
                                CopilotSub id,
                                LogID logid);

  void RemoveSubscriberInternal(StreamID stream_id);

  /**
   * Finds the LogReader* with given reader_id, or nullptr if none found.
   */
  LogReader* FindLogReader(size_t reader_id);

  /**
   * Assign a new subscription (id + topic) to a LogReader.
   */
  LogReader* ReaderForNewSubscription(CopilotSub id,
                                      const TopicUUID& topic,
                                      LogID logid,
                                      SequenceNumber seqno);

  /**
   * Attempt to merge src into all other readers.
   * Merging into another reader means that the other reader will subsume all
   * subscriptions that src was serving, and src will stop reading on log_id.
   */
  void AttemptReaderMerges(LogReader* src, LogID log_id);

  ThreadCheck thread_check_;

  BaseEnv* env_;

  // Message loop that we belong to, and our worker index.
  MsgLoop* msg_loop_;
  const int worker_id_;

  // The log tailer.
  LogTailer* log_tailer_;

  // Topic to log router.
  std::shared_ptr<LogRouter> log_router_;

  // Information log
  std::shared_ptr<Logger> info_log_;

  // Each reader is capable of reading each log once.
  // We have multiple readers in case a log is subscribed to at multiple
  // positions.
  std::vector<std::unique_ptr<LogReader>> log_readers_;

  // This is a 'virtual' reader for all pending subscriptions.
  // It has the same interface as LogReader, but doesn't actually read
  // from a log. Once a read reader (from log_readers_) becomes available,
  // it will take the state of pending_reader_ and open the log at the
  // correct position.
  std::unique_ptr<LogReader> pending_reader_;

  // Callback for outgoing messages.
  std::function<void(std::unique_ptr<Message>,
                     std::vector<CopilotSub>)> on_message_;

  // Subscription information per topic
  std::unordered_map<LogID, TopicManager> topic_map_;

  // Cached tail sequence number per log.
  std::unordered_map<LogID, SequenceNumber> tail_seqno_cached_;

  std::mt19937_64& prng_;

  ControlTowerOptions::TopicTailer options_;

  // Queues used to communicate from storage threads back to the room.
  std::unique_ptr<ThreadLocalCommandQueues> storage_to_room_queues_;

  // Map of subscriptions per stream.
  SubscriptionMap<TopicUUID> stream_subscriptions_;

  struct Stats {
    Stats() {
      const std::string prefix = "tower.topic_tailer.";

      log_records_received =
        all.AddCounter(prefix + "log_records_received");
      log_records_received_payload_size =
        all.AddCounter(prefix + "log_records_received_payload_size");
      backlog_records_received =
        all.AddCounter(prefix + "backlog_records_received");
      tail_records_received =
        all.AddCounter(prefix + "tail_records_received");
      new_tail_records_sent =
        all.AddCounter(prefix + "new_tail_records_sent");
      log_records_with_subscriptions =
        all.AddCounter(prefix + "log_records_with_subscriptions");
      log_records_without_subscriptions =
        all.AddCounter(prefix + "log_records_without_subscriptions");
      log_records_out_of_order =
        all.AddCounter(prefix + "log_records_out_of_order");
      bumped_subscriptions =
        all.AddCounter(prefix + "bumped_subscriptions");
      gap_records_received =
        all.AddCounter(prefix + "gap_records_received");
      gap_records_out_of_order =
        all.AddCounter(prefix + "gap_records_out_of_order");
      gap_records_with_subscriptions =
        all.AddCounter(prefix + "gap_records_with_subscriptions");
      gap_records_without_subscriptions =
        all.AddCounter(prefix + "gap_records_without_subscriptions");
      benign_gaps_received =
        all.AddCounter(prefix + "benign_gaps_received");
      malignant_gaps_received =
        all.AddCounter(prefix + "malignant_gaps_received");
      add_subscriber_requests =
        all.AddCounter(prefix + "add_subscriber_requests");
      add_subscriber_requests_at_0 =
        all.AddCounter(prefix + "add_subscriber_requests_at_0");
      add_subscriber_requests_at_0_fast =
        all.AddCounter(prefix + "add_subscriber_requests_at_0_fast");
      add_subscriber_requests_at_0_slow =
        all.AddCounter(prefix + "add_subscriber_requests_at_0_slow");
      updated_subscriptions =
        all.AddCounter(prefix + "updated_subscriptions");
      remove_subscriber_requests =
        all.AddCounter(prefix + "remove_subscriber_requests");
    }

    Statistics all;
    Counter* log_records_received;
    Counter* log_records_received_payload_size;
    Counter* backlog_records_received;
    Counter* tail_records_received;
    Counter* new_tail_records_sent;
    Counter* log_records_with_subscriptions;
    Counter* log_records_without_subscriptions;
    Counter* log_records_out_of_order;
    Counter* bumped_subscriptions;
    Counter* gap_records_received;
    Counter* gap_records_out_of_order;
    Counter* gap_records_with_subscriptions;
    Counter* gap_records_without_subscriptions;
    Counter* benign_gaps_received;
    Counter* malignant_gaps_received;
    Counter* add_subscriber_requests;
    Counter* add_subscriber_requests_at_0;
    Counter* add_subscriber_requests_at_0_fast;
    Counter* add_subscriber_requests_at_0_slow;
    Counter* updated_subscriptions;
    Counter* remove_subscriber_requests;
  } stats_;
};

template <typename Function>
bool TopicTailer::Forward(Function command) {
  std::unique_ptr<Command> cmd(MakeExecuteCommand(std::move(command)));
  return Forward(std::move(cmd));
}

}  // namespace rocketspeed
