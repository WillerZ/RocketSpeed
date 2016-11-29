// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <functional>

#include "src/messages/messages.h"
#include "src/util/common/flow.h"

namespace rocketspeed {

class EventLoop;
class ScheduledExecutor;

/**
 * Delivery Batcher
 * Batches messages on a stream and forwards the write for delivery to
 * the Underlying sink (Delivery Throttler)
 */
class DeliveryBatcher : public Sink<std::unique_ptr<Message>> {
 public:
  using Clock = std::chrono::steady_clock;

  struct Policy {
    Policy() = default;

    explicit Policy(size_t limit_, std::chrono::milliseconds duration_)
    : limit(limit_), duration(duration_) {}

    size_t limit = 100;
    std::chrono::milliseconds duration = std::chrono::milliseconds(10);
  };

  /*
   * @param sink Underlying sink (DeliveryThrottler) to forward the batch
   * @param scheduler ScheduledExecutor to schedule the timeout on batching
   * @param policy batching policy to be used
   */
  explicit DeliveryBatcher(Sink* sink,
                           std::shared_ptr<ScheduledExecutor> scheduler,
                           Policy policy);

  ~DeliveryBatcher() = default;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  /// Adds the message to the batch
  bool Write(std::unique_ptr<Message>& value) final override;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  bool FlushPending() final override;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) final override;

  /** Inherited from Sink<std::unique_ptr<Message>> */
  std::string GetSinkName() const override { return "delivery-batcher-sink"; }

 private:
  Sink* sink_;
  const Policy policy_;

  std::shared_ptr<ScheduledExecutor> scheduler_;

  MessageDeliverBatch::MessagesVector messages_batched_;
  Clock::time_point batch_start_time_;

  /// Adds the message to the batch and
  /// Returns true if more messages can be added to the batch else false
  bool AddToBatch(std::unique_ptr<Message> value);

  /// Returns true if more messages can be added to the batch
  bool CanAddMore();

  /// Forward the batched messages for delivery
  bool Dispatch();
};

}  // namespace rocketspeed
