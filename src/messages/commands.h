// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "src/messages/messages.h"

namespace rocketspeed {

/**
 * Enumeration of command types.
 */
enum class CommandType {
  mMessage
};

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

  // Get the type of this command.
  virtual CommandType GetType() const = 0;
};

/**
 * Command containing a pre-serialized message.
 */
class MessageCommand : public Command {
 public:
  // Construct from message.
  explicit MessageCommand(Message* message);

  // Get the type of this command.
  virtual CommandType GetType() const {
    return CommandType::mMessage;
  }

  // Get the Message object stored in this Command.
  Message* GetMessage() {
    return message_.get();
  }

 private:
  std::unique_ptr<Message> message_;
};

}  // namespace rocketspeed
