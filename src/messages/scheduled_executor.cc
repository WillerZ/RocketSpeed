// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#define __STDC_FORMAT_MACROS
#include "src/messages/scheduled_executor.h"

#include "include/Assert.h"

#include "src/messages/event_loop.h"
#include "src/messages/timed_callback.h"

namespace rocketspeed {

ScheduledExecutor::ScheduledExecutor(EventLoop* event_loop,
                                     std::chrono::milliseconds tick_time)
: event_loop_(event_loop) {
  RS_ASSERT(tick_time.count() > 0);
  // Wake up every tick_time to process expired events
  timer_callback_ = event_loop->RegisterTimerCallback(
      [this]() { ProcessExpired(); }, tick_time, true);
}

void ScheduledExecutor::Schedule(std::function<void()> callback,
                                 std::chrono::milliseconds timeout) {
  event_loop_->ThreadCheck();

  auto schedule_time = Clock::now() + timeout;
  timed_events_.insert({schedule_time, std::move(callback)});
}

void ScheduledExecutor::ProcessExpired() {
  auto now = Clock::now();
  auto it = timed_events_.begin();
  while (it != timed_events_.end()) {
    if (now < it->first) {
      // No more events to process as they are yet to timeout
      break;
    }
    // Execute the callback
    (it->second)();
    it = timed_events_.erase(it);
  }
}

}  // namespace rocketspeed
