// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/messages/timed_callback.h"

#include "src/messages/event2_version.h"
#include <event2/event.h>

#include "include/Logger.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"

namespace rocketspeed {

TimedCallback::TimedCallback(
  EventLoop* event_loop, std::function<void()> cb,
  std::chrono::microseconds duration)
  : event_loop_(event_loop)
  , cb_(std::move(cb))
  , duration_(duration)
  , enabled_(false)
  , event_(nullptr)
  , info_log_(event_loop_->GetLog()) {
}

TimedCallback::~TimedCallback() {
  event_free(event_);
}

void TimedCallback::AddEvent(event* event, bool enable) {
  RS_ASSERT(!event_);
  event_ = event;
  if (enable) {
    Enable();
  }
}

void TimedCallback::Enable() {
  event_loop_->ThreadCheck();

  timeval timer_seconds;
  timer_seconds.tv_sec = duration_.count() / 1000000ULL;
  timer_seconds.tv_usec = static_cast<decltype(timer_seconds.tv_usec)>(
      duration_.count() % 1000000ULL);

  if (!enabled_) {
    if (event_add(event_, &timer_seconds)) {
      LOG_ERROR(info_log_, "Failed to add timer event");
      info_log_->Flush();
      return;
    }
    enabled_ = true;
  }
}

void TimedCallback::Disable() {
  event_loop_->ThreadCheck();

  if (enabled_) {
    if (event_del(event_)) {
      LOG_ERROR(info_log_, "Failed to delete timer event");
      info_log_->Flush();
      return;
    }
    enabled_ = false;
  }
}

} // namespace rocketspeed
