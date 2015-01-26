// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <vector>
#include "./Status.h"
#include "./Types.h"
#include "src/port/Env.h"
#include "src/util/storage.h"
#include "src/controltower/room.h"

namespace rocketspeed {

//
// A Tailer reads specified logs from Storage and delivers data to the
// specified ControlRooms.
// Each ControlRoom has its own Reader so that a ControlRoom does not have
// to sychronize access to a Reader, thereby avoiding any kind of locking.
//
class Tailer {
 friend class ControlTowerTest;
 public:
  // create a Tailer
  static Status CreateNewInstance(
                           Env* env,
                           const std::vector<unique_ptr<ControlRoom>>& rooms,
                           std::shared_ptr<LogStorage> storage,
                           const URL& storage_url,
                           std::shared_ptr<Logger> info_log,
                           int num_workers,
                           Tailer** tailer);

  // Initialize the Tailer first before using it
  Status Initialize();

  /**
   * Opens the specified log at specified position or reseeks to the position if
   * the log was opened. This call is not thread-safe.
   */
  Status StartReading(LogID logid,
                      SequenceNumber start,
                      unsigned int roomid,
                      bool first_open) const;

  // No more records from this log anymore
  // This call is not thread-safe.
  Status StopReading(LogID logid, unsigned int roomid) const;

  // Asynchronously finds the latest seqno then
  // invokes the callback.
  Status FindLatestSeqno(LogID logid,
                    std::function<void(Status, SequenceNumber)> callback) const;

  virtual ~Tailer();

 private:
  // private constructor
  Tailer(const std::vector<unique_ptr<ControlRoom>>& rooms,
         std::shared_ptr<LogStorage> storage,
         std::shared_ptr<Logger> info_log);

  // A pointer to all the rooms
  const std::vector<unique_ptr<ControlRoom>>& rooms_;

  // The Storage device
  const std::shared_ptr<LogStorage> storage_;

  // One reader per ControlRoom
  std::vector<unique_ptr<AsyncLogReader>> reader_;

  // Information log
  std::shared_ptr<Logger> info_log_;

  // Count of number of open logs per reader (unit tests only)
  mutable std::vector<int> num_open_logs_per_reader_;

  // The total number of open logs (no locks) (unit test only)
  int NumberOpenLogs() const;
};

}  // namespace rocketspeed
