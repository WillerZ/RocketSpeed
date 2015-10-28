// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/util/common/statistics.h"
#include "src/util/storage.h"

namespace rocketspeed {

//
// A LogTailer reads specified logs from Storage and delivers data to the user.
// A LogTailer has a number of readers (identified by reader_id). The callback
// for each reader will only be called on one thread.
//
class LogTailer {
 public:
  /**
   * Callback for incoming messages. If the message was processed successfully,
   * the callback should return true. If unsuccessful, false should be returned,
   * and the MessageData should be unmoved.
   */
  typedef std::function<bool(std::unique_ptr<MessageData>&,  // publish msg
                             LogID,                          // log ID
                             size_t)>                        // reader ID
    OnRecordCallback;

  /**
   * Callback for incoming gaps. If the gap was processed successfully,
   * the callback should return true. If unsuccessful, false should be returned.
   */
  typedef std::function<bool(LogID,           // log ID
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
                      size_t reader_id,
                      bool first_open);

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

  ~LogTailer();

 private:
  // private constructor
  LogTailer(std::shared_ptr<LogStorage> storage,
            std::shared_ptr<Logger> info_log);

  // Creates a log reader.
  Status CreateReader(size_t reader_id,
                      OnRecordCallback on_record,
                      OnGapCallback on_gap,
                      AsyncLogReader** out);

  // The total number of open logs.
  int NumberOpenLogs() const;

  // The Storage device
  std::shared_ptr<LogStorage> storage_;

  // One reader per ControlRoom
  std::vector<std::unique_ptr<AsyncLogReader>> reader_;

  // Information log
  std::shared_ptr<Logger> info_log_;

  // Count of number of open logs per reader (unit tests only)
  mutable std::vector<int> num_open_logs_per_reader_;

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
    }

    Statistics all;
    Counter* open_logs;
    Counter* readers_started;
    Counter* readers_restarted;
    Counter* readers_stopped;
  } stats_;
};

}  // namespace rocketspeed
