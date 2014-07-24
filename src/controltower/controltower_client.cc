//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/include/controltower_client.h"
#include "src/controltower/controltower.h"

namespace rocketspeed {
/**
 * Private constructor for a Control Tower
 */
ControlTowerClient::ControlTowerClient(const Configuration& conf,
                                       const std::string& hostname,
                                       const int port,
                                       Logger* info_log) :
  conf_(conf),
  hostname_(hostname),
  port_(port),
  info_log_(info_log),
  is_connected_(false) {
  if (info_log != nullptr) {
    Log(InfoLogLevel::INFO_LEVEL, info_log,
      "Created a new Client for Control Tower.");
  }
}

/**
 * This is a static method to create a ControlTowerClient
 */
Status
ControlTowerClient::CreateNewInstance(const Env& env,
                                      const Configuration& conf,
                                      const std::string& hostname,
                                      const int port,
                                      Logger* info_log,
                                      ControlTowerClient** ct) {
  *ct = new ControlTowerClient(conf, hostname, port, info_log);
  Status st =  env.NewConnection(hostname, port, &(*ct)->connection_,
                                  (*ct)->env_options_);

  // If the connection to the server is not successful, then free
  // up all allocated resources and return error to the caller.
  if (!st.ok()) {
    delete *ct;
    *ct = nullptr;
  } else {
    (*ct)->is_connected_ = true;
  }
  return st;
}

Status
ControlTowerClient::Send(const MessageMetadata& msg) {
  if (!is_connected_) {
    return Status::IOError("Not connected");
  }
  return connection_->Send(msg.Serialize());
}

}  // namespace rocketspeed
