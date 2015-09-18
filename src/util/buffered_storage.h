// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "src/util/storage.h"

namespace rocketspeed {

class Env;
class Logger;
class MsgLoop;

class BufferedLogStorageWorker;

/**
 * BufferedLogStorage wraps another LogStorage implementation, providing
 * buffered writes to increase throughput.
 */
class BufferedLogStorage : public LogStorage {
 public:
  static Status Create(Env* env,
                       std::shared_ptr<Logger> info_log,
                       std::shared_ptr<LogStorage> wrapped_storage,
                       MsgLoop* msg_loop,
                       size_t max_batch_entries,
                       size_t max_batch_bytes,
                       std::chrono::microseconds max_batch_latency,
                       LogStorage** storage);

  ~BufferedLogStorage() final;

  Status AppendAsync(LogID id,
                     const Slice& data,
                     AppendCallback callback) final;

  Status FindTimeAsync(LogID id,
                       std::chrono::milliseconds timestamp,
                       std::function<void(Status, SequenceNumber)> callback);

  Status CreateAsyncReaders(
    unsigned int parallelism,
    std::function<bool(LogRecord&)> record_cb,
    std::function<bool(const GapRecord&)> gap_cb,
    std::vector<AsyncLogReader*>* readers);

  bool CanSubscribePastEnd() const {
    return true;
  }

 private:
  BufferedLogStorage(Env* env,
                     std::shared_ptr<Logger> info_log,
                     std::shared_ptr<LogStorage> wrapped_storage,
                     MsgLoop* msg_loop,
                     size_t max_batch_entries,
                     size_t max_batch_bytes,
                     std::chrono::microseconds max_batch_latency);

  Env* env_;
  std::shared_ptr<Logger> info_log_;
  std::shared_ptr<LogStorage> storage_;
  MsgLoop* msg_loop_;
  size_t max_batch_entries_;
  size_t max_batch_bytes_;
  std::chrono::microseconds max_batch_latency_;
  size_t batch_bits_;
  std::vector<std::unique_ptr<BufferedLogStorageWorker>> workers_;
};

}  // namespace rocketspeed
