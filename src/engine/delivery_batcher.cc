// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#define __STDC_FORMAT_MACROS
#include "src/engine/delivery_batcher.h"

#include "include/Assert.h"

#include "src/messages/msg_loop.h"

namespace rocketspeed {

DeliveryBatcher::DeliveryBatcher(Sink* sink, TenantID tenant_id, Policy policy)
: sink_(sink), tenant_id_(tenant_id), policy_(policy) {}

bool DeliveryBatcher::Write(std::unique_ptr<Message>& value) {
  const auto message_type = value->GetMessageType();
  RS_ASSERT(message_type == MessageType::mDeliverData ||
            message_type == MessageType::mDeliverBatch);

  // The message is already batched
  // Dispatch any batched messages by the batcher and then the batch message
  // TODO Combine Instead
  if (message_type == MessageType::mDeliverBatch) {
    bool dispatch_batched = Dispatch();
    bool dispatch_message = sink_->Write(value);
    return dispatch_batched && dispatch_message;
  }

  // Check if we can add more messages to batch permited by the policy
  if (CanAddMore()) {
    if (!AddToBatch(std::move(value))) {
      // Can't add more to batch, so dispatch immediately
      return Dispatch();
    }
    return true;
  }

  // Can't add any more messages.
  // Add one more to the batch, this "might" violate the policy if the batch
  // size was reached but avoids extra added latency for a single delivery
  AddToBatch(std::move(value));
  return Dispatch();
}

bool DeliveryBatcher::CanAddMore() {
  if (messages_batched_.empty()) {
    // The batch is empty at the moment, begin the batching.
    batch_start_time_ = Clock::now();
    return true;
  }
  auto now = Clock::now();
  return messages_batched_.size() < policy_.limit &&
         now - batch_start_time_ < policy_.duration;
}

bool DeliveryBatcher::FlushPending() {
  return sink_->FlushPending();
}

bool DeliveryBatcher::AddToBatch(std::unique_ptr<Message> value) {
  std::unique_ptr<MessageDeliverData> message(
      static_cast<MessageDeliverData*>(value.release()));
  messages_batched_.emplace_back(std::move(message));

  return messages_batched_.size() < policy_.limit;
}

bool DeliveryBatcher::Dispatch() {
  if (messages_batched_.empty()) {
    return true;
  }

  std::unique_ptr<Message> message;
  if (messages_batched_.size() == 1) {
    message = std::move(messages_batched_.front());
  } else {
    message = std::make_unique<MessageDeliverBatch>(
        tenant_id_, std::move(messages_batched_));
  }

  messages_batched_.clear();
  return sink_->Write(message);
}

std::unique_ptr<EventCallback> DeliveryBatcher::CreateWriteCallback(
    EventLoop* event_loop, std::function<void()> callback) {
  return sink_->CreateWriteCallback(event_loop, std::move(callback));
}

}  // namespace rocketspeed
