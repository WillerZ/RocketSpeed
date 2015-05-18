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
  MessageReceivedClient(NamespaceID namespace_id,
                        Topic topic_name,
                        std::unique_ptr<MessageDeliverData> data)
      : namespace_id_(std::move(namespace_id)),
        topic_name_(std::move(topic_name)),
        data_(std::move(data)) {}

  SubscriptionHandle GetSubscriptionHandle() const override {
    return data_->GetSubID();
  }

  // The sequence number of this message
  virtual SequenceNumber GetSequenceNumber() const {
    return data_->GetSequenceNumber();
  }

  // The namespace id of this message
  virtual Slice GetNamespaceId() const { return Slice(namespace_id_); }

  // The Topic name
  virtual Slice GetTopicName() const { return Slice(topic_name_); }

  // The contents of the message
  virtual Slice GetContents() const { return data_->GetPayload(); }

 private:
  NamespaceID namespace_id_;
  Topic topic_name_;
  std::unique_ptr<MessageDeliverData> data_;
};

}  // namespace rocketspeed
