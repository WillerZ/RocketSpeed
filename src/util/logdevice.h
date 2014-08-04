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
#include <vector>
#include "include/Env.h"
#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/Client.h"
#include "src/util/storage.h"

namespace rocketspeed {

// Forward declarations
class LogDeviceReader;
class LogDeviceSelector;

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
   * @param settings       client settings instance to take ownership of,
   *                       or nullptr for default settings
   * @param storage        output parameter to store the constructed
   *                       LogDeviceStorage object.
   * @return on success returns OK(), otherwise errorcode.
   */
  static Status Create(
    std::string cluster_name,
    std::string config_url,
    std::string credentials,
    std::chrono::milliseconds timeout,
    std::unique_ptr<facebook::logdevice::ClientSettings>&& settings,
    Env* env,
    LogDeviceStorage** storage);

  ~LogDeviceStorage() final {}

  Status Append(LogID id,
                const Slice& data) final;

  Status Trim(LogID id,
              std::chrono::microseconds age) final;

  Status CreateReaders(unsigned int maxLogsPerReader,
                       unsigned int parallelism,
                       std::vector<LogReader*>* readers) final;

 private:
  LogDeviceStorage(std::shared_ptr<facebook::logdevice::Client> client,
                   Env* env);

  std::shared_ptr<facebook::logdevice::Client> client_;
  Env* env_;
};

/**
 * Log Reader interface backed by LogDevice.
 */
class LogDeviceReader : public LogReader {
 public:
  LogDeviceReader(LogDeviceStorage* storage,
                  unsigned int maxLogs,
                  std::unique_ptr<facebook::logdevice::AsyncReader>&& reader);

  ~LogDeviceReader() final;

  Status Open(LogID id,
              SequenceNumber startPoint,
              SequenceNumber endPoint) final;

  Status Close(LogID id) final;

  Status Read(std::vector<LogRecord>* records,
              size_t maxRecords) final;

  bool HasData() const;

 private:
  friend class LogDeviceSelector;

  void SetSelector(LogDeviceSelector* selector);

  LogDeviceStorage& storage_;
  const unsigned int maxLogs_ = 0;
  std::unique_ptr<facebook::logdevice::AsyncReader> reader_;
  mutable std::mutex mutex_;
  std::vector<std::unique_ptr<char[]>> buffers_;
  std::vector<LogRecord> pending_;
  LogDeviceSelector* selector_ = nullptr;
};

/**
 * LogSelector interface backed by LogDevice.
 */
class LogDeviceSelector : public LogSelector {
 public:
  LogDeviceSelector();

  static Status Create(LogDeviceSelector** selector);

  ~LogDeviceSelector() final;

  Status Register(LogReader* reader) final;

  Status Deregister(LogReader* reader) final;

  Status Select(std::vector<LogReader*>* selected,
                std::chrono::microseconds timeout) final;

  void Notify();

 private:
  std::vector<LogDeviceReader*> readers_;
  std::mutex mutex_;
  std::condition_variable monitor_;
};

}  // namespace rocketspeed
