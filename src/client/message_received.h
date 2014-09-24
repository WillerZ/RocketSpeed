// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "src/messages/messages.h"

namespace rocketspeed {

/**
 * Interface class for transferring a message from RS-client to application.
 */

class MessageReceivedClient : public MessageReceived {
 public:
  explicit MessageReceivedClient(unique_ptr<Message> m) : msg_(std::move(m)) {
    data_ = static_cast<const MessageData*>(msg_.get());
  }

  // The sequence number of this message
  virtual SequenceNumber GetSequenceNumber() {
    return data_->GetSequenceNumber();
  }

  // The Topic name
  virtual const Slice GetTopicName() {
    return data_->GetTopicName();
  }

  // The contents of the message
  virtual const Slice GetContents() {
    return data_->GetPayload();
  }

  virtual ~MessageReceivedClient();

 private:
  const unique_ptr<Message> msg_;
  const MessageData* data_;
};

}  // namespace rocketspeed
