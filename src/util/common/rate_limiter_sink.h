// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <chrono>
#include <functional>

#include "include/RateLimiter.h"
#include "src/messages/combined_callback.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/util/common/flow.h"

namespace rocketspeed {

class EventCallback;
class EventLoop;

/**
 * A sink which limits the write rate.
 * RateLimiterSink forwards the write to the underlying sink.
 *
 * Note: RateLimiterSink is tightly coupled with CombinedCallback using
 * "event_state" (a shared_ptr of pair of bools).
 * More about event_state in combined_callback.h.
 *
 * event_state_->first will be set to false if the underlying sink is blocking
 * writes to it. (will enable an FdCallback in this case)
 * event_state_->second will be set to false if the rate limit is reached for
 * the current period. (will enable a TimedCallback in this case)
 *
 */
template <typename T>
class RateLimiterSink : public Sink<T> {
 public:
  virtual ~RateLimiterSink() = default;

  /**
   * RateLimiter Constructor.
   *
   * @param limit number of writes allowed.
   * @param duration time period (in microseconds).
   * @param sink underlying sink to pass the write to.
   */
  RateLimiterSink(
    size_t limit, std::chrono::microseconds duration, Sink<T>* sink);

  bool Write(T& value) final override;

  bool FlushPending() final override;

  std::unique_ptr<EventCallback>
  CreateWriteCallback(EventLoop* event_loop,
                      std::function<void()> callback) final override;

 private:
  RateLimiter rate_limiter_;
  std::chrono::microseconds duration_;
  Sink<T>* sink_;
  std::shared_ptr<std::pair<bool, bool>> event_state_;
};

template <typename T>
RateLimiterSink<T>::RateLimiterSink(
  size_t limit, std::chrono::microseconds duration, Sink<T>* sink)
  : rate_limiter_(limit,
      std::chrono::duration_cast<std::chrono::milliseconds>(duration))
  , duration_(duration)
  , sink_(sink)
  , event_state_(
    std::make_shared<std::pair<bool, bool>>(true, true)) {
}

template <typename T>
bool RateLimiterSink<T>::Write(T& value) {
  if (sink_->FlushPending()) {
    event_state_->first = sink_->Write(value);
    rate_limiter_.TakeOne();
    event_state_->second = rate_limiter_.IsAllowed();
    return event_state_->first && event_state_->second;
  }
  return false;
}

template <typename T>
bool RateLimiterSink<T>::FlushPending() {
  event_state_->first = sink_->FlushPending();
  event_state_->second = rate_limiter_.IsAllowed();
  return event_state_->first && event_state_->second;
}

template <typename T>
std::unique_ptr<EventCallback>
RateLimiterSink<T>::CreateWriteCallback(
  EventLoop* event_loop, std::function<void()> callback) {

  // Create the first create_callback function which takes callback
  // as a parameter.
  auto event_cb_1 = std::bind(&Sink<T>::CreateWriteCallback,
    sink_, event_loop, std::placeholders::_1);

  // Create the second create_callback function which takes callback
  // as a parameter. This is a timed callback to flush after the rate limit
  // throttling has been relaxed. The factor of 10 is to make the retry checks
  // more granular, which accounts for clock skew between the
  // rate_limiter_ clock, and the timer that controls the flush wake up.
  auto event_cb_2 = std::bind(&EventLoop::CreateTimedEventCallback,
    event_loop, std::placeholders::_1, duration_ / 10);

  return std::unique_ptr<EventCallback>(new CombinedCallback(
    event_loop, callback, event_state_, event_cb_1, event_cb_2));
}

} // namespace rocketspeed
