// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <chrono>
#include <functional>

#include "src/messages/event_callback.h"

struct event;

namespace rocketspeed {

class EventCallback;
class EventLoop;
class Logger;

/**
 * Creates a TimedCallback which gets invoked every time the specified duration
 * is passed, unless disabled.
 */
class TimedCallback : public EventCallback {
 public:

  /**
   * TimedCallback Constructor.
   *
   * @param event_loop the event_loop to run the callback on.
   * @param cb the callback to be invoked.
   * @param duration time period (in microseconds).
   */
  TimedCallback(
    EventLoop* event_loop, std::function<void()> cb,
    std::chrono::microseconds duration);

  ~TimedCallback();

  void Enable() final override;

  void Disable() final override;

  void Invoke() const {
    RS_ASSERT(enabled_);
    cb_();
  }

  /**
   * Adds an event to the callback.
   * Note: Should only be called once, else will throw assertion error.
   *
   * @param event the event to add.
   * @param enable whether to enable the event immediately when added.
   */
  void AddEvent(event* event, bool enable);

 private:
  EventLoop* event_loop_;
  const std::function<void()> cb_;
  std::chrono::microseconds duration_;
  bool enabled_;
  event* event_;
  const std::shared_ptr<Logger> info_log_;
};

} // namespace rocketspeed
