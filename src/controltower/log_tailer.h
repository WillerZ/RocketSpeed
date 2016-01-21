// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <set>
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "include/Env.h"
#include "src/util/common/statistics.h"
#include "src/util/storage.h"
#include "src/controltower/options.h"

namespace rocketspeed {

class EventLoop;
class Flow;
template <typename> class ThreadLocalQueues;

//
// A LogTailer reads specified logs from Storage and delivers data to the user.
// A LogTailer has a number of readers (identified by reader_id). The callback
// for each reader will only be called on one thread.
//
class LogTailer {
 public:
  /**
   * Callback for incoming messages.
   */
  typedef std::function<void(Flow*,
                             std::unique_ptr<MessageData>&,  // publish msg
                             LogID,                          // log ID
                             size_t)>                        // reader ID
    OnRecordCallback;

  /**
   * Callback for incoming gaps.
   */
  typedef std::function<void(Flow*,
                             LogID,           // log ID
                             GapType,         // type of gap
                             SequenceNumber,  // start sequence number
                             SequenceNumber,  // end sequence number
                             size_t)>         // reader ID
    OnGapCallback;

  /**
   * Create a LogTailer.
   */
  static Status CreateNewInstance(
                           Env* env,
                           std::shared_ptr<LogStorage> storage,
                           std::shared_ptr<Logger> info_log,
                           EventLoop* event_loop,
                           ControlTowerOptions::LogTailer options,
                           LogTailer** tailer);

  /**
   * Shuts down the LogTailer.
   *
   * It is safe to destroy the LogStorage afterwards, but it is undefined to
   * invoke any other actions on the LogTailer other than destruction.
   */
  void Stop();

  /**
   * Initialize the LogTailer first before using it.
   *
   * @param on_record Callback for when a data record is received.
   * @param on_gap Callback for when there is a gap in the log.
   * @param num_readers Number of reader IDs to allocate.
   * @return ok if successful, otherwise error code.
   */
  Status Initialize(OnRecordCallback on_record,
                    OnGapCallback on_gap,
                    size_t num_readers);

  /**
   * Opens the specified log at specified position or reseeks to the position if
   * the log was opened. This call is not thread-safe.
   */
  Status StartReading(LogID logid,
                      SequenceNumber start,
                      size_t reader_id);

  // No more records from this log anymore
  // This call is not thread-safe.
  Status StopReading(LogID logid, size_t reader_id);

  // Asynchronously finds the latest seqno then
  // invokes the callback on an unspecified thread.
  Status FindLatestSeqno(
    LogID logid,
    std::function<void(Status, SequenceNumber)> callback);

  // Can we subscribe past the end of the log?
  bool CanSubscribePastEnd() const {
    return storage_->CanSubscribePastEnd();
  }

  Statistics GetStatistics() const;

  void Tick();

  ~LogTailer();

 private:
  /**
   * LogReaders are restarted periodically. This structure represents a
   * the restart event for a particular reader and log. It is ordered by
   * time.
   */
  struct RestartEvent {
    RestartEvent(std::chrono::steady_clock::time_point _restart_time,
                 size_t _reader_id,
                 LogID _log_id)
    : restart_time(_restart_time)
    , reader_id(_reader_id)
    , log_id(_log_id) {
    }

    std::chrono::steady_clock::time_point restart_time;
    size_t reader_id;
    LogID log_id;

    bool operator<(const RestartEvent& rhs) const {
      return std::tie(restart_time, reader_id, log_id) <
        std::tie(rhs.restart_time, rhs.reader_id, rhs.log_id);
    }
  };

  /**
   * An ordered set of RestartEvents.
   * Is a thin wrapper around std::set<RestartEvent>, providing convenient
   * interface for adding new events with random expiry time.
   */
  class RestartEvents : public std::set<RestartEvent> {
   public:
    /** Opaque handle used for removing events. */
    using Handle = iterator;

    RestartEvents(std::chrono::milliseconds min_restart_duration,
                  std::chrono::milliseconds max_restart_duration)
    : min_restart_duration_(min_restart_duration)
    , max_restart_duration_(max_restart_duration) {
    }

    /**
     * Adds a new event with a random restart time in the future and returns
     * the new handle.
     */
    Handle AddEvent(size_t reader_id, LogID log_id);

    /**
     * Removes an existing event by its handle.
     */
    void RemoveEvent(Handle handle);

   private:
    const std::chrono::milliseconds min_restart_duration_;
    const std::chrono::milliseconds max_restart_duration_;
  };

  // private constructor
  LogTailer(std::shared_ptr<LogStorage> storage,
            std::shared_ptr<Logger> info_log,
            EventLoop* event_loop,
            ControlTowerOptions::LogTailer options);

  // Creates a log reader.
  Status CreateReader(size_t reader_id, AsyncLogReader** out);

  // The total number of open logs.
  int NumberOpenLogs() const;

  void RecordCallback(Flow* flow,
                      std::unique_ptr<MessageData>& msg,
                      LogID log_id,
                      size_t reader_id);

  void GapCallback(Flow* flow,
                   LogID log_id,
                   GapType gap_type,
                   SequenceNumber from,
                   SequenceNumber to,
                   size_t reader_id);

  /**
   * Forwards a command from a storage thread to a LogTailer thread.
   * The command will only be sent when returning true. On a return of false,
   * the caller should attempt to resend the command later.
   */
  bool TryForward(std::function<void(Flow*)> command);

  // The Storage device
  std::shared_ptr<LogStorage> storage_;

  // One reader per ControlRoom
  struct Reader {
    explicit Reader(std::unique_ptr<AsyncLogReader> _log_reader,
                    OnRecordCallback _on_record,
                    OnGapCallback _on_gap)
    : log_reader(std::move(_log_reader))
    , on_record(std::move(_on_record))
    , on_gap(std::move(_on_gap)) {}

    struct LogState {
      explicit LogState(SequenceNumber _next_seqno)
      : next_seqno(_next_seqno) {}

      SequenceNumber next_seqno;
      RestartEvents::Handle restart_event_handle;
    };

    std::unique_ptr<AsyncLogReader> log_reader;
    std::unordered_map<LogID, LogState> log_state;
    OnRecordCallback on_record;
    OnGapCallback on_gap;
  };
  std::vector<Reader> readers_;

  // Information log
  std::shared_ptr<Logger> info_log_;

  ControlTowerOptions::LogTailer options_;

  EventLoop* event_loop_;

  // Queues for storage threads delivering records or gaps back to rooms.
  std::unique_ptr<ThreadLocalQueues<std::function<void(Flow*)>>>
    storage_to_room_queues_;

  // Contains a set of (LogID, reader_id) pairs that will be restarted at
  // a certain point in time. Readers are restarted occasionally to allow
  // the storage layer to rebalance threads, and provides some extra resilience
  // against unexpected log reader failures.
  // The set is ordered by time.
  RestartEvents restart_events_;

  struct Stats {
    Stats() {
      const std::string prefix = "tower.log_tailer.";

      open_logs =
        all.AddCounter(prefix + "open_logs");
      readers_started =
        all.AddCounter(prefix + "readers_started");
      readers_restarted =
        all.AddCounter(prefix + "readers_restarted");
      readers_stopped =
        all.AddCounter(prefix + "readers_stopped");
      log_records_out_of_order =
        all.AddCounter(prefix + "log_records_out_of_order");
      gap_records_out_of_order =
        all.AddCounter(prefix + "gap_records_out_of_order");
      forced_restarts =
        all.AddCounter(prefix + "forced_restarts");
    }

    Statistics all;
    Counter* open_logs;
    Counter* readers_started;
    Counter* readers_restarted;
    Counter* readers_stopped;
    Counter* log_records_out_of_order;
    Counter* gap_records_out_of_order;
    Counter* forced_restarts;
  } stats_;
};

}  // namespace rocketspeed
