#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include "include/BackPressure.h"
#include "src/messages/event_loop.h"
#include "src/util/common/flow.h"

namespace rocketspeed {

/**
 * The RetryLaterSink forwards writes to a callable target, which has the
 * option of applying backpressure to request the write to be retried at a
 * later time. The callable target must return a retry delay, which is a time
 * to wait before retrying.
 */
template <typename T>
class RetryLaterSink : public SinkWithOverflow<T> {
 public:
  explicit RetryLaterSink(std::function<BackPressure(T&)>&& handler)
  : handler_(std::move(handler)) {}

  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
    // When blocked, the sink will wake up every 10ms to check if it's time
    // to write again (see TryWrite). This isn't ideal as it would be more
    // efficient to just schedule to wake up at the right time, but the flow
    // control interfaces don't allow us to modify the callback itself from
    // the Write call.
    // TODO(pja) - Improve flow control interfaces to allow this.
    return event_loop->CreateTimedEventCallback(
        std::move(callback), std::chrono::milliseconds(10));
  }

 protected:
  bool TryWrite(T& value) final override {
    // Check if we are after the requested retry time.
    if (std::chrono::steady_clock::now() < next_time_) {
      return false;
    }
    const BackPressure backoff = handler_(value);
    if (backoff) {
      // Handler has requested that we retry later.
      // Update the time for next allowed write and back-off the sources.
      next_time_ = std::chrono::steady_clock::now() + backoff.Delay();
      return false;
    }
    return true;
  }

 private:
  std::function<BackPressure(T&)> handler_;
  std::chrono::steady_clock::time_point next_time_;
};

} // naemspace rocketspeed
