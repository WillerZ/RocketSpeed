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

/**
 * Command containing a pre-serialized message.
 */
class MessageCommand : public Command {
 public:
  // Construct from message.
  MessageCommand(const MsgId& msgid,
                 const HostId& recipient,
                 const Message& message);

  // Get the message ID.
  const MsgId& GetMessageId() const {
    return msgid_;
  }

  // Get the intended recipient of the message.
  const HostId& GetRecipient() const {
    return recipient_;
  }

  // Get the serialized message data stored in this Command.
  Slice GetMessage() {
    return Slice(message_);
  }

 private:
  MsgId msgid_;
  HostId recipient_;
  std::string message_;
};

}  // namespace rocketspeed
