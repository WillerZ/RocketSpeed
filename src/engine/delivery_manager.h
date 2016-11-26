// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/timeout_list.h"

#include "src/engine/delivery_batcher.h"
#include "src/engine/delivery_throttler.h"

namespace rocketspeed {

class EventLoop;
class EventCallback;
class Flow;
class Message;

/**
 * Delivery Manager
 * DeliveryManager manages deliveries to be made per stream
 *
 * Flow:
 *
 *   -----------        --------      ----------       --------
 *  | Deliveries | -> | Batcher | -> | Throttler | -> | Stream |
 *   -----------        --------      ----------       --------
 */
class DeliveryManager : public NonCopyable, public NonMovable {
 public:
  /*
   * @param event_loop EventLoop
   * @param throttler_policy Throttling Policy to be used
   * @param batcher_policy Batching Policy to be used
   */
  explicit DeliveryManager(EventLoop* event_loop,
                           DeliveryThrottler::Policy throttler_policy,
                           DeliveryBatcher::Policy batcher_policy);

  /// Deliver the data on the stream
  /// Forwards the write to the Batcher
  void Deliver(Flow* flow,
               StreamID stream_id,
               std::unique_ptr<Message> message);

  /// Registers a new stream to enable batching and throttling
  void RegisterStream(StreamID stream_id, TenantID tenant_id);

  /// UnRegister the stream
  void UnRegisterStream(StreamID stream_id);

  ~DeliveryManager() = default;

 private:
  EventLoop* const event_loop_;
  const DeliveryThrottler::Policy throttler_policy_;
  const DeliveryBatcher::Policy batcher_policy_;

  struct StreamInfo {
    explicit StreamInfo(std::unique_ptr<DeliveryThrottler> _throttler,
                        std::unique_ptr<DeliveryBatcher> _batcher)
    : throttler(std::move(_throttler)), batcher(std::move(_batcher)) {}

    std::unique_ptr<DeliveryThrottler> throttler;
    std::unique_ptr<DeliveryBatcher> batcher;
  };

  std::unordered_map<StreamID, StreamInfo> streams_;
  TimeoutList<StreamInfo*> streams_pending_deliveries_;
  std::unique_ptr<EventCallback> timer_callback_;

  /// Deliver any pending deliveries that have timed out for batching
  void DeliverPending();
};

}  // namespace rocketspeed
