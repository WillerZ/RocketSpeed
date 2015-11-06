// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <functional>
#include <memory>

#include "external/folly/move_wrapper.h"

#include "src/messages/commands.h"
#include "src/messages/queues.h"
#include "src/util/common/flow_control.h"

namespace rocketspeed {

class Logger;
class QueueStats;

/**
 * Creates a queue and registers a callback on a flow control object.
 *
 * @param event_loop EventLoop the flow control was bound to.
 * @param info_log Logging for queue.
 * @param queue_stats Statistics for queue.
 * @param size Size of the queue (in elements).
 * @param flow_control Flow control to use for processing the queue elements.
 * @param callback Callback to invoke on queue reads.
 */
template <typename T>
std::shared_ptr<Queue<T>>
InstallQueue(EventLoop* event_loop,
             std::shared_ptr<Logger> info_log,
             std::shared_ptr<QueueStats> queue_stats,
             size_t size,
             FlowControl* flow_control,
             std::function<void(Flow*, T)> callback) {
  auto queue = std::make_shared<Queue<T>>(std::move(info_log),
                                          std::move(queue_stats),
                                          size);
  InstallSource(event_loop, queue.get(), flow_control, std::move(callback));
  return queue;
}

/**
 * Installs a Source on the event loop. When items are available on the source
 * the callback will be invoked with the available items, and flow control
 * context from the source.
 *
 * @param event_loop EventLoop the flow control was bound to.
 * @param source The source to begin processing.
 * @param flow_control Flow control to use for processing the elements.
 * @param callback Callback to invoke on source reads.
 */
template <typename T>
void InstallSource(EventLoop* event_loop,
                   Source<T>* source,
                   FlowControl* flow_control,
                   std::function<void(Flow*, T)> callback) {
  auto moved_callback = folly::makeMoveWrapper(std::move(callback));
  std::unique_ptr<Command> cmd(
    MakeExecuteCommand([moved_callback, source, flow_control]() mutable {
      flow_control->Register(source, moved_callback.move());
    }));
  event_loop->SendControlCommand(std::move(cmd));
}


}  // namespace rocketspeed
