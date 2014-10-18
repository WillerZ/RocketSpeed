// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <string>
#include "include/Types.h"
#include "src/messages/messages.h"

namespace rocketspeed {

/**
 * Interface class for sending messages from any thread to the event loop
 * for processing on the event loop thread.
 */
class Command {
 public:
  // Default constructor.
  Command() {}

  // Default destructor.
  virtual ~Command() {}

  // Is this a message send-command? The msg-send command is special because
  virtual void GetMessage(std::string* out) = 0;

  // Is this a message send-command? The msg-send command is special because
  // the event loop processes it inline instead of invoking the application
  // callback. If this is a SendCommand, then the event loop sends out the
  // message associated with this Command to the host specified via a call
  // to GetDestination().
  virtual bool IsSendCommand() const = 0;

  // If this is a command to send a mesage to remote hosts, then
  // returns the list of destination HostIds.
  // are returned in num_hosts.
  virtual const std::vector<HostId>& GetDestination() const = 0;
};

}  // namespace rocketspeed
