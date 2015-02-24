// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "src/port/Env.h"
#include "src/util/storage.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow"
#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/Client.h"
#pragma GCC diagnostic pop

namespace rocketspeed {

/**
 * LogDevice log record entry.
 */
struct LogDeviceRecord : public LogRecord {
 public:
  explicit
  LogDeviceRecord(std::unique_ptr<facebook::logdevice::DataRecord> record);

 private:
  std::unique_ptr<facebook::logdevice::DataRecord> record_;
};

/**
 * Log storage interface backed by LogDevice.
 */
class LogDeviceStorage : public LogStorage {
 public:
  /**
   * Constructs a LogDeviceStorage.
   *
   * @param cluster_name   name of the LogDevice cluster to connect to
   * @param config_url     a URL that identifies at a LogDevice configuration
   *                       resource (such as a file) describing the LogDevice
   *                       cluster this client will talk to. The only supported
   *                       formats are currently
   *                       file:<path-to-configuration-file> and
   *                       configerator:<configerator-path>. Examples:
   *                         "file:logdevice.test.conf"
   *                         "configerator:logdevice/logdevice.test.conf"
   * @param credentials    credentials specification. This may include
   *                       credentials to present to the LogDevice cluster
   *                       along with authentication and encryption specifiers.
   *                       Format TBD. Currently ignored.
   * @param timeout        construction timeout. This value also serves as the
   *                       default timeout for methods on the created object
   * @param num_workers    number of client workers.
   * @param storage        output parameter to store the constructed
   *                       LogDeviceStorage object.
   * @return on success returns OK(), otherwise errorcode.
   */
  static Status Create(
    std::string cluster_name,
    std::string config_url,
    std::string credentials,
    std::chrono::milliseconds timeout,
    int num_workers,
    Env* env,
    LogDeviceStorage** storage);

  /**
   * Constructs a LogDeviceStorage using a previously created Client object.
   *
   * @param client Previously created client object.
   * @param storage output parameter to store the constructed LogDeviceStorage
   *        object.
   * @param env Env object for platform specific operations.
   * @return on success returns OK(), otherwise errorcode.
   */
  static Status Create(
    std::shared_ptr<facebook::logdevice::Client> client,
    Env* env,
    LogDeviceStorage** storage);

  ~LogDeviceStorage() final {}

  Status AppendAsync(LogID id,
                     const Slice& data,
                     AppendCallback callback) final;

  Status Trim(LogID id,
              std::chrono::microseconds age) final;

  Status FindTimeAsync(LogID id,
                       std::chrono::milliseconds timestamp,
                       std::function<void(Status, SequenceNumber)> callback);

  Status CreateAsyncReaders(unsigned int parallelism,
                      std::function<void(std::unique_ptr<LogRecord>)> record_cb,
                      std::function<void(const GapRecord&)> gap_cb,
                      std::vector<AsyncLogReader*>* readers);

 private:
  LogDeviceStorage(std::shared_ptr<facebook::logdevice::Client> client,
                   Env* env);

  std::shared_ptr<facebook::logdevice::Client> client_;
  Env* env_;
};

/**
 * Async Log Reader interface backed by LogDevice.
 */
class AsyncLogDeviceReader : public AsyncLogReader {
 public:
  AsyncLogDeviceReader(LogDeviceStorage* storage,
                    std::function<void(std::unique_ptr<LogRecord>)> record_cb,
                    std::function<void(const GapRecord&)> gap_cb,
                    std::unique_ptr<facebook::logdevice::AsyncReader>&& reader);

  ~AsyncLogDeviceReader() final;

  Status Open(LogID id,
              SequenceNumber startPoint,
              SequenceNumber endPoint) final;

  Status Close(LogID id) final;

 private:
  std::unique_ptr<facebook::logdevice::AsyncReader> reader_;
};

}  // namespace rocketspeed
