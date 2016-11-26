// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#define __STDC_FORMAT_MACROS
#include "src/engine/delivery_manager.h"

#include "include/Assert.h"

#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
#include "src/messages/timed_callback.h"
#include "src/util/common/flow.h"

namespace rocketspeed {

DeliveryManager::DeliveryManager(EventLoop* event_loop,
                                 DeliveryThrottler::Policy throttler_policy,
                                 DeliveryBatcher::Policy batcher_policy)
: event_loop_(event_loop)
, throttler_policy_(throttler_policy)
, batcher_policy_(batcher_policy) {
  RS_ASSERT(batcher_policy_.limit >= 1);
  RS_ASSERT(throttler_policy_.limit >= 1);
  // Wake up every 10 milliseconds to make the pending deliveries if any
  timer_callback_ = event_loop_->RegisterTimerCallback(
      [this]() { DeliverPending(); }, std::chrono::milliseconds(10), true);
}

void DeliveryManager::Deliver(Flow* flow,
                              StreamID stream_id,
                              std::unique_ptr<Message> message) {
  auto it = streams_.find(stream_id);
  if (it != streams_.end()) {
    auto stream_info = &it->second;
    if (flow) {
      flow->Write(stream_info->batcher.get(), message);
    } else {
      SourcelessFlow no_flow(event_loop_->GetFlowControl());
      no_flow.Write(stream_info->batcher.get(), message);
    }
    streams_pending_deliveries_.AddIfNotExist(stream_info);
  } else {
    LOG_WARN(event_loop_->GetLog(),
             "Stream: %llu not found, dropping message",
             stream_id);
  }
}

void DeliveryManager::RegisterStream(StreamID stream_id, TenantID tenant_id) {
  auto it = streams_.find(stream_id);
  if (it == streams_.end()) {
    auto stream = event_loop_->GetInboundStream(stream_id);

    // Create Throttler
    auto throttler =
        std::make_unique<DeliveryThrottler>(stream, throttler_policy_);

    // Create Batcher
    auto batcher = std::make_unique<DeliveryBatcher>(
        throttler.get(), tenant_id, batcher_policy_);

    StreamInfo info(std::move(throttler), std::move(batcher));
    streams_.emplace(stream_id, std::move(info));
  }
}

void DeliveryManager::UnRegisterStream(StreamID stream_id) {
  auto streamIt = streams_.find(stream_id);
  if (streamIt != streams_.end()) {
    streams_pending_deliveries_.Erase(&streamIt->second);
  }
  streams_.erase(stream_id);
}

void DeliveryManager::DeliverPending() {
  // Dispatch batches from pending streams for which the batching duration has
  // expired and remove the stream from streams_pending_deliveries_
  streams_pending_deliveries_.ProcessExpired(
      batcher_policy_.duration,
      [this](StreamInfo* stream_info) { stream_info->batcher->Dispatch(); },
      -1);
}

}  // namespace rocketspeed
