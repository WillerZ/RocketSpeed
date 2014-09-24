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
};

}  // namespace rocketspeed
