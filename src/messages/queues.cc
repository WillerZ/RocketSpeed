// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/queues.h"

#include "include/Logger.h"
#include "src/messages/commands.h"
#include "src/messages/event_loop.h"
#include "include/BaseEnv.h"

namespace rocketspeed {

QueueStats::QueueStats(const std::string& prefix) {
  batched_read_size = all.AddHistogram(
      prefix + ".batched_read_size", 0, kMaxQueueBatchReadSize, 1, 1.1f);
  size_on_read = all.AddHistogram(
      prefix + ".size_on_read", 0, kMaxQueueSize, 1, 1.1f);
  response_latency = all.AddLatency(prefix + ".response_latency");
  num_reads = all.AddCounter(prefix + ".num_reads");
  eventfd_num_writes = all.AddCounter(prefix + ".eventfd_num_writes");
  eventfd_num_reads = all.AddCounter(prefix + ".eventfd_num_reads");
}

std::unique_ptr<EventCallback>
CreateEventFdReadCallback(EventLoop* event_loop,
                          int fd,
                          std::function<void()> callback) {
  return EventCallback::CreateFdReadCallback(event_loop,
                                             fd,
                                             std::move(callback));
}

}  // namespace rocketspeed
