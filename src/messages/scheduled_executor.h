// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>

#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"

namespace rocketspeed {

class EventLoop;
class EventCallback;

/**
 * ScheduledExecutor
 * Ticks every specified tick duration and executes callback
 * on the timedout events if any.
 * The callback runs on the same event_loop on which it is created
 */
class ScheduledExecutor : public NonCopyable, public NonMovable {
 public:
  using Clock = std::chrono::steady_clock;

  /*
   * @param event_loop EventLoop of the timer
   * @param tick_time time after which the timer wakes up to check for
   *        expired events
   */
  explicit ScheduledExecutor(EventLoop* event_loop,
                             std::chrono::milliseconds tick_time);

  ~ScheduledExecutor() = default;

  /// Schedule the callback to be run after timeout from now()
  /// Callback will be invoked on the same event_loop
  void Schedule(std::function<void()> callback,
                std::chrono::milliseconds timeout);

 private:
  EventLoop* event_loop_;

  std::map<Clock::time_point, std::function<void()>> timed_events_;
  std::unique_ptr<EventCallback> timer_callback_;

  /// Invoke the callback on the timedout events
  void ProcessExpired();
};

}  // namespace rocketspeed
