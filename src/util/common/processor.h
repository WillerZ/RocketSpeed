// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <functional>
#include <memory>

#include "src/messages/commands.h"
#include "src/messages/queues.h"
#include "src/messages/flow_control.h"

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
 * @param callback Callback to invoke on queue reads.
 */
template <typename T>
std::shared_ptr<SPSCQueue<T>>
InstallSPSCQueue(EventLoop* event_loop,
                 std::shared_ptr<Logger> info_log,
                 size_t size,
                 std::function<void(Flow*, T)> callback) {
  auto queue = std::make_shared<SPSCQueue<T>>(std::move(info_log),
                                          event_loop->GetQueueStats(),
                                          size);
  InstallSource(event_loop, queue.get(), std::move(callback));
  return queue;
}

/**
 * Installs a Source on the event loop. When items are available on the source
 * the callback will be invoked with the available items, and flow control
 * context from the source.
 *
 * @param event_loop EventLoop the flow control was bound to.
 * @param source The source to begin processing.
 * @param callback Callback to invoke on source reads.
 */
template <typename T>
void InstallSource(EventLoop* event_loop,
                   Source<T>* source,
                   std::function<void(Flow*, T)> callback) {
  std::unique_ptr<Command> cmd(
    MakeExecuteCommand(
      [callback = std::move(callback), source, event_loop]() mutable {
      event_loop->GetFlowControl()->Register(source, std::move(callback));
    }));
  event_loop->SendControlCommand(std::move(cmd));
}

}  // namespace rocketspeed
