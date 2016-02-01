// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/messages/combined_callback.h"

#include "src/messages/event_loop.h"

namespace rocketspeed {

CombinedCallback::CombinedCallback(
  EventLoop* event_loop, std::function<void()> cb,
  std::shared_ptr<std::pair<bool, bool>> event_state,
  CreateEvent create_event_first, CreateEvent create_event_second)
  : event_loop_(event_loop)
  , cb_(std::move(cb))
  , event_state_(event_state) {

  // Create the first event callback with a custom callback which sets
  // first event_state to true when completed and tries to invoke
  // the callback.
  first_event_ = create_event_first([this]() {
    event_state_->first = true;
    Invoke();
  });

  // Create the second event callback with a custom callback which sets
  // second event_state to true when completed and tries to invoke
  // the callback.
  second_event_ = create_event_second([this]() {
    event_state_->second = true;
    Invoke();
  });
}

void CombinedCallback::Enable() {
  event_loop_->ThreadCheck();

  RS_ASSERT(!event_state_->first || !event_state_->second);

  if (!event_state_->first) {
    first_event_->Enable();
  }
  if (!event_state_->second) {
    second_event_->Enable();
  }
}

void CombinedCallback::Disable() {
  event_loop_->ThreadCheck();

  first_event_->Disable();
  second_event_->Disable();
}

} // namespace rocketspeed
