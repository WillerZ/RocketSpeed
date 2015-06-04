// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <utility>
#include "src/port/Env.h"
#include "logdevice/include/AsyncReader.h"
#include "src/logdevice/Common.h"

namespace facebook { namespace logdevice {

class AsyncReaderImpl : public AsyncReader {
 public:
  AsyncReaderImpl();

  ~AsyncReaderImpl() {
    done_ = true;
    thread_.join();
  }

 private:
  friend class AsyncReader;

  // LSN range to read from a particular log, and the last modified time,
  // used to wait for reads.
  struct Log {
    lsn_t from_;
    lsn_t until_;
    uint64_t last_mod_;
  };

  // Reads a log file and calls the data callback for all records in the
  // supplied range. Returns the last LSN read.
  lsn_t ReadFile(logid_t logid, lsn_t from, lsn_t until);

  // Gets the last sequence number written to a file
  lsn_t LastSequenceNumber(logid_t logid);

  // Callbacks
  std::function<bool(std::unique_ptr<DataRecord>&)> data_cb_;
  std::function<bool(const GapRecord&)> gap_cb_;

  // Mutex for locking the logs_ map.
  std::mutex mutex_;

  // Map containing logs currently read from. Maps a log ID to a Log,
  // which stores the current LSN range a last modified time.
  std::map<logid_t, Log> logs_;

  // File reading daemon thread.
  std::thread thread_;

  rocketspeed::Env* env_;

  // Flag used to tell the reading thread that we should stop.
  std::atomic<bool> done_{false};
};


}  // namespace logdevice
}  // namespace facebook
