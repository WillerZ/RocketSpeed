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
                HostId recipient,
                std::string msg,
                uint64_t issued_time) :
    Command(issued_time),
    msgid_(msgid),
    msg_(std::move(msg)) {
    recipient_.push_back(std::move(recipient));
  }

  // Construct from metadata message.
  ClientCommand(HostId recipient,
                std::string msg,
                uint64_t issued_time) :
    Command(issued_time),
    msg_(std::move(msg)) {
    recipient_.push_back(std::move(recipient));
  }

  // Get the message ID.
  const MsgId& GetMessageId() const {
    return msgid_;
  }

  // Get the message data stored in this Command.
  void GetMessage(std::string* out) {
    out->assign(std::move(msg_));
  }

  const Recipients& GetDestination() const {
    return recipient_;
  }
  bool IsSendCommand() const {
    return true;
  }
 private:
  MsgId msgid_;
  Recipients recipient_;
  std::string msg_;
};

}  // namespace rocketspeed
