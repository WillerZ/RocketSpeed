// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <vector>
#include "./Env.h"
#include "./Status.h"
#include "./Types.h"
#include "src/util/storage.h"
#include "src/util/logdevice.h"
#include "src/controltower/room.h"

namespace rocketspeed {

//
// A Tailer reads specified logs from Storage and delivers data to the
// specified ControlRooms.
// Each ControlRoom has its own Reader so that a ControlRoom does not have
// to sychronize access to a Reader, thereby avoiding any kind of locking.
//
class Tailer {
 public:
  // create a Tailer
  static Status CreateNewInstance(
                           Env* env,
                           const std::vector<unique_ptr<ControlRoom>>& rooms,
                           const URL& storage_url,
                           Tailer** tailer);

  // Opens the specified log at specified position
  // This call is not thread-safe.
  Status StartReading(LogID logid, SequenceNumber start,
                      unsigned int roomid) const;

  // No more records from this log anymore
  // This call is not thread-safe.
  Status StopReading(LogID logid, unsigned int roomid) const;

  virtual ~Tailer();

 private:
  // private constructor
  Tailer(const std::vector<unique_ptr<ControlRoom>>& rooms,
         const URL& storage_url,
         LogStorage* storage);

  // A pointer to all the rooms
  const std::vector<unique_ptr<ControlRoom>>& rooms_;

  // definition of the storage
  const URL storage_url_;

  // The Storage device
  const unique_ptr<LogStorage> storage_;

  // One reader per ControlRoom
  std::vector<unique_ptr<AsyncLogReader>> reader_;

  // initialize the Tailer first before using it
  Status Initialize();
};

}  // namespace rocketspeed
