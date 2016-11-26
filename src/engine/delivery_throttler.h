// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <deque>
#include <functional>

#include "include/RateLimiter.h"
#include "include/Rocketeer.h"

#include "src/util/common/flow.h"

namespace rocketspeed {

class Message;
class EventLoop;
class EventCallback;

/**
 * Delivery Throttler
 * Throttles delivery of messages on a particular stream.
 * Throttling is defined by the throttling policy.
 *
 * DeliveryThrottler will immideatly deliver the message if allowed under the
 * policy else will queue the message for later delivery.
 */
class DeliveryThrottler : public Sink<std::unique_ptr<Message>> {
 public:
  struct Policy {
    explicit Policy(size_t limit_, std::chrono::milliseconds duration_)
    : limit(limit_), duration(duration_) {}

    size_t limit;
    std::chrono::milliseconds duration;
  };

  /*
   * @param sink Underlying sink to forward the write to
   * @param policy Rate Limiting policy
   */
  explicit DeliveryThrottler(Sink* sink, Policy policy);

  ~DeliveryThrottler() = default;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  /// Writes a message for delivery.
  /// If failed it will enqueue the message for later delivery.
  bool Write(std::unique_ptr<Message>& value) final override;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  /// Flush all pending deliveries.
  /// Returns true iff all the deliveries were flushed.
  bool FlushPending() final override;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) final override;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  std::string GetSinkName() const override { return "delivery-throttler-sink"; }

 private:
  Sink* sink_;
  const Policy policy_;

  RateLimiter rate_limiter_;
  std::deque<std::unique_ptr<Message>> deliveries_;
  std::shared_ptr<std::pair<bool, bool>> event_state_;

  /// Forward the write for delivery and update state based on write
  /// and rate limit
  void WriteAndUpdateState(std::unique_ptr<Message>& value);
};

}  // namespace rocketspeed
