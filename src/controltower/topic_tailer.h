// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/messages/messages.h"
#include "src/util/hostmap.h"
#include "src/util/storage.h"
#include "src/util/topic_uuid.h"
#include "src/util/worker_loop.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class LogTailer;
class LogRouter;
class LogReader;

class TopicTailer {
 friend class ControlTowerTest;
 public:
  // create a LogTailer
  static Status CreateNewInstance(
   BaseEnv* env,
   LogTailer* log_tailer,
   std::shared_ptr<LogRouter> log_router,
   std::shared_ptr<Logger> info_log,
   std::function<void(std::unique_ptr<Message>, LogID)> on_message,
   TopicTailer** tailer);

  /**
   * Initialize the TopicTailer first before using it.
   *
   * @param reader_id ID of reader on LogTailer.
   * @return ok if successful, otherwise error code.
   */
  Status Initialize(size_t reader_id);

  /**
   * Opens the specified log at specified position or reseeks to the position if
   * the log was opened. This call is not thread-safe.
   */
  Status StartReading(const TopicUUID& topic,
                      SequenceNumber start,
                      HostNumber hostnum);

  // No more records from this log anymore
  // This call is not thread-safe.
  Status StopReading(const TopicUUID& topic,
                     HostNumber hostnum);

  // Asynchronously finds the latest seqno then
  // invokes the callback.
  Status FindLatestSeqno(
    const TopicUUID& topic,
    std::function<void(Status, SequenceNumber)> callback);

  /**
   * Process a data record from a log tailer, and forward to on_message.
   *
   * @param msg Log record message.
   * @param log_id ID of log record resides in.
   * @param reader_id ID of reader delivering the record.
   * @return OK() if record was sent for processing.
   */
  Status SendLogRecord(
    std::unique_ptr<MessageData> msg,
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

  ~TopicTailer();

 private:
  typedef std::function<void()> TopicTailerCommand;

  // private constructor
  TopicTailer(BaseEnv* env,
              LogTailer* log_tailer,
              std::shared_ptr<LogRouter> log_router,
              std::shared_ptr<Logger> info_log,
              std::function<void(std::unique_ptr<Message>, LogID)> on_message);

  ThreadCheck thread_check_;

  BaseEnv* env_;

  // The log tailer.
  LogTailer* log_tailer_;

  // Topic to log router.
  std::shared_ptr<LogRouter> log_router_;

  // Information log
  std::shared_ptr<Logger> info_log_;

  std::unique_ptr<LogReader> log_reader_;

  // Callback for outgoing messages.
  std::function<void(std::unique_ptr<Message>, LogID)> on_message_;

  WorkerLoop<TopicTailerCommand> worker_loop_;
  BaseEnv::ThreadId worker_thread_;
};

}  // namespace rocketspeed
