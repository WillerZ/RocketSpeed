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

  // Construct from data message.
  ClientCommand(const MsgId& msgid,
                const HostId& recipient,
                unique_ptr<Message> msg) :
    msgid_(msgid),
    recipient_(recipient),
    msg_(std::move(msg)) {
    assert(msg_.get()->GetMessageType() == MessageType::mPublish ||
           msg_.get()->GetMessageType() == MessageType::mDeliver);
  }

  // Construct from metadata message.
  ClientCommand(const HostId& recipient,
                unique_ptr<MessageMetadata> msg) :
    recipient_(recipient),
    msg_(std::move(msg)) {
    assert(msg_.get()->GetMessageType() == MessageType::mMetadata);
  }

  // Get the message ID.
  const MsgId& GetMessageId() const {
    assert(msg_.get()->GetMessageType() == MessageType::mPublish ||
           msg_.get()->GetMessageType() == MessageType::mDeliver);
    return msgid_;
  }

  // Get the message data stored in this Command.
  unique_ptr<Message> GetMessage() {
    return std::move(msg_);
  }

  MessageType GetType() {
    return msg_.get()->GetMessageType();
  }

  const HostId& GetDestination() const {
    return recipient_;
  }
  bool IsSendCommand() const {
    return true;
  }
 private:
  MsgId msgid_;
  HostId recipient_;
  unique_ptr<Message> msg_;
};

}  // namespace rocketspeed
