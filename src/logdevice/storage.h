// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "src/util/storage.h"

namespace facebook {
  namespace logdevice {
    class Client;
  }
}

namespace rocketspeed {

class Env;
class Logger;

/**
 * Log storage interface backed by LogDevice.
 */
class LogDeviceStorage : public LogStorage {
 public:
  /**
   * Constructs a LogDeviceStorage.
   *
   * @param cluster_name      name of the LogDevice cluster to connect to
   * @param config_url        a URL that identifies at a LogDevice configuration
   *                          resource (such as a file) describing the LogDevice
   *                          cluster this client will talk to.
   *                          The only supported formats are currently:
   *                            file:<path-to-configuration-file> and
   *                            configerator:<configerator-path>. Examples:
   *                              "file:logdevice.test.conf"
   *                              "configerator:logdevice/logdevice.test.conf"
   * @param credentials       credentials specification. This may include
   *                          credentials to present to the LogDevice cluster
   *                          along with authentication and encryption
   *                          specifiers. Format TBD. Currently ignored.
   * @param timeout           construction timeout. This value also serves as
   *                          default timeout for methods on the created object
   * @param num_workers       number of client workers.
   * @param max_payload_size  max payload size in bytes.
   * @param ssl_boundary      boundary to use SSL, must be one of:
   *                          none, node, rack, row, cluster, dc, or region.
   * @param my_location       location for determining SSL boundaries:
   *                          {region}.{dc}.{cluster}.{row}.{rack}
   * @param env               environment.
   * @param info_log          for logging.
   * @param storage           output parameter to store the constructed
   *                          LogDeviceStorage object.
   * @return on success returns OK(), otherwise errorcode.
   */
  static Status Create(
    std::string cluster_name,
    std::string config_url,
    std::string credentials,
    std::chrono::milliseconds timeout,
    int num_workers,
    size_t max_payload_size,
    std::string ssl_boundary,
    std::string my_location,
    Env* env,
    std::shared_ptr<Logger> info_log,
    LogDeviceStorage** storage);

  /**
   * Constructs a LogDeviceStorage using a previously created Client object.
   *
   * @param client Previously created client object.
   * @param storage output parameter to store the constructed LogDeviceStorage
   *        object.
   * @param env Env object for platform specific operations.
   * @param info_log For logging.
   * @return on success returns OK(), otherwise errorcode.
   */
  static Status Create(
    std::shared_ptr<facebook::logdevice::Client> client,
    Env* env,
    std::shared_ptr<Logger> info_log,
    LogDeviceStorage** storage);

  ~LogDeviceStorage() final;

  Status AppendAsync(LogID id,
                     std::string data,
                     AppendCallback callback) final;

  Status Trim(LogID id,
              SequenceNumber seqno) final;

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
  LogDeviceStorage(std::shared_ptr<facebook::logdevice::Client> client,
                   Env* env,
                   std::shared_ptr<Logger> info_log);

  std::shared_ptr<facebook::logdevice::Client> client_;
  std::shared_ptr<Logger> info_log_;
};

}  // namespace rocketspeed
