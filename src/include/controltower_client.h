// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <errno.h>
#include <string.h>
#include <string>
#include "include/Env.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/messages.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/auto_roll_logger.h"

/**
 * This is the client-side api to interact with the control tower
 */

namespace rocketspeed {

class ControlTowerClient {
 public:
  // Create a new instance of a ControlTowerClient. A client
  // can internally open multiple connections to the ControlTower.
  // Multiple ControlTowerClients can also share a common pool
  // of connections to the ControlTower
  static Status CreateNewInstance(const Env& env,
                                  const Configuration& conf,
                                  const std::string& hostname,
                                  const int port_number,
                                  Logger* info_log,
                                  ControlTowerClient** client);

  // Send a subscription/unsubscription message to the ControlTower
  Status Send(const MessageMetadata& msg);

 private:
  const Configuration conf_;
  const std::string hostname_;
  const int port_;
  Logger* info_log_;
  int is_connected_;
  const EnvOptions env_options_;

  // A connection to the specified server. In the future, we can
  // fetch/release connections from a common connection pool.
  unique_ptr<Connection> connection_;

  // private constructor
  ControlTowerClient(const Configuration& conf,
                     const std::string& hostname,
                     const int port_number,
                     Logger* info_log);
};
}  // namespace rocketspeed
