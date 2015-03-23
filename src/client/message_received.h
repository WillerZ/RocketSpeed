// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Types.h"
#include "src/messages/messages.h"

namespace rocketspeed {

/**
 * Interface class for transferring a message from RS-client to application.
 */

class MessageReceivedClient : public MessageReceived {
 public:
  explicit MessageReceivedClient(std::unique_ptr<Message> m) :
      msg_(std::move(m)) {
    data_ = static_cast<const MessageData*>(msg_.get());
  }

  // The sequence number of this message
  virtual SequenceNumber GetSequenceNumber() const {
    return data_->GetSequenceNumber();
  }

  // The namespace id of this message
  virtual Slice GetNamespaceId() const {
    return data_->GetNamespaceId();
  }

  // The Topic name
  virtual Slice GetTopicName() const {
    return data_->GetTopicName();
  }

  // The contents of the message
  virtual Slice GetContents() const {
    return data_->GetPayload();
  }

  virtual ~MessageReceivedClient();

 private:
  const std::unique_ptr<Message> msg_;
  const MessageData* data_;
};

}  // namespace rocketspeed
