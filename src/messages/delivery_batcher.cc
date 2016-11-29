// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#define __STDC_FORMAT_MACROS
#include "src/messages/delivery_batcher.h"

#include "include/Assert.h"

#include "src/messages/msg_loop.h"
#include "src/messages/scheduled_executor.h"

namespace rocketspeed {

DeliveryBatcher::DeliveryBatcher(Sink* sink,
                                 std::shared_ptr<ScheduledExecutor> scheduler,
                                 Policy policy)
: sink_(sink), policy_(policy), scheduler_(scheduler) {}

bool DeliveryBatcher::Write(std::unique_ptr<Message>& value) {
  const auto message_type = value->GetMessageType();

  // TODO: Batch any message kind
  if (message_type != MessageType::mDeliverData) {
    // Dispatch any existing batch
    bool dispatch_batched = Dispatch();
    // Then dispatch the message
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
    // Schedule a timeout after which we dispatch whatever we have batched
    // until that point
    scheduler_->Schedule([ this, start_time = batch_start_time_ ]() {
      // Check if the batch start_time matches the current batch start time
      // otherwise the message was already dispatched
      if (start_time == batch_start_time_) {
        Dispatch();
      }
    },
                         policy_.duration);
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
    auto msg = static_cast<Message*>(messages_batched_.front().get());
    auto tenant_id = msg->GetTenantID();
    message = std::make_unique<MessageDeliverBatch>(
        tenant_id, std::move(messages_batched_));
  }

  messages_batched_.clear();
  return sink_->Write(message);
}

std::unique_ptr<EventCallback> DeliveryBatcher::CreateWriteCallback(
    EventLoop* event_loop, std::function<void()> callback) {
  return sink_->CreateWriteCallback(event_loop, std::move(callback));
}

}  // namespace rocketspeed
