// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#define __STDC_FORMAT_MACROS
#include "src/engine/delivery_throttler.h"

#include "src/messages/combined_callback.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"

namespace rocketspeed {

DeliveryThrottler::DeliveryThrottler(Sink* sink, Policy policy)
: sink_(sink)
, policy_(policy)
, rate_limiter_(policy.limit, policy.duration)
, event_state_(std::make_shared<std::pair<bool, bool>>(true, true)) {}

bool DeliveryThrottler::Write(std::unique_ptr<Message>& value) {
  const auto message_type = value->GetMessageType();
  RS_ASSERT(message_type == MessageType::mDeliverData ||
            message_type == MessageType::mDeliverBatch);

  if (FlushPending()) {
    WriteAndUpdateState(value);
    return event_state_->first && event_state_->second;
  }
  deliveries_.emplace_back(std::move(value));
  return false;
}

bool DeliveryThrottler::FlushPending() {
  event_state_->first = sink_->FlushPending();
  while (event_state_->first && event_state_->second && !deliveries_.empty()) {
    WriteAndUpdateState(deliveries_.front());
    deliveries_.pop_front();
  }
  return event_state_->first && event_state_->second;
}

void DeliveryThrottler::WriteAndUpdateState(std::unique_ptr<Message>& value) {
  event_state_->first = sink_->Write(value);
  event_state_->second = rate_limiter_.IsAllowed() && rate_limiter_.TakeOne();
}

std::unique_ptr<EventCallback> DeliveryThrottler::CreateWriteCallback(
    EventLoop* event_loop, std::function<void()> callback) {
  // Create the first create_callback function which takes callback
  // as a parameter.
  auto event_cb_1 = std::bind(
      &Sink::CreateWriteCallback, sink_, event_loop, std::placeholders::_1);

  // Create the second create_callback function which takes callback
  // as a parameter. This is a timed callback to flush after the rate limit
  // throttling has been relaxed.
  auto event_cb_2 = std::bind(&EventLoop::CreateTimedEventCallback,
                              event_loop,
                              std::placeholders::_1,
                              policy_.duration);

  return std::unique_ptr<EventCallback>(new CombinedCallback(
      event_loop, std::move(callback), event_state_, event_cb_1, event_cb_2));
}

}  // namespace rocketspeed
