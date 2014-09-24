// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "src/messages/commands.h"
#include "src/messages/messages.h"

namespace rocketspeed {

/**
 * Interface class for sending messages from any thread to the event loop
 * for processing on the event loop thread.
 */

/**
 * Command containing a pre-serialized message.
 */
class ClientCommand : public Command {
 public:
  // The various types of commands sent from application
  // thread to the event loop thread
  enum Type : char {
    Data,        // data
    DataAck,     // data ack
    MetaData,    // data
  };

  // Construct from data message.
  ClientCommand(const MsgId& msgid,
                const HostId& recipient,
                const MessageData& message) :
    type_(Type::Data),
    msgid_(msgid),
    recipient_(recipient),
    message_(message.Serialize().ToString()) {}

  // Construct from metadata message.
  ClientCommand(const HostId& recipient,
                const MessageMetadata& message) :
    type_(Type::MetaData),
    recipient_(recipient),
    message_(message.Serialize().ToString()) {}

  // Get the message ID.
  const MsgId& GetMessageId() const {
    assert(type_ == Type::Data);
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

  Type GetType() {
    return type_;
  }

 private:

  Type type_;
  MsgId msgid_;
  HostId recipient_;
  std::string message_;
};

}  // namespace rocketspeed
