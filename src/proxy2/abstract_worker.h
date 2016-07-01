/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "include/Assert.h"
#include "include/ProxyServer.h"
#include "src/messages/types.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

class EventLoop;
class Message;
class ProxyServerOptions;
template <typename Item>
class SPSCQueue;
class QueueStats;

using MessageAndStream = std::pair<StreamID, std::unique_ptr<Message>>;
using MessageQueue = SPSCQueue<MessageAndStream>;
using WorkerQueues = std::vector<std::shared_ptr<MessageQueue>>;

/// Shared functionality of all proxy workers.
///
/// Some people moan how inheritance is not the right tool to reuse code.
/// It probaby isn't, but it does work.
class AbstractWorker {
 public:
  AbstractWorker(const ProxyServerOptions& options,
                 EventLoop* event_loop,
                 size_t num_inbound_queues,
                 size_t num_outbound_queues);

  Statistics* GetStatistics() { return &statistics_; }

  std::shared_ptr<MessageQueue> CreateInboundQueue(size_t inbound_id);

  void ConnectOutboundQueue(size_t outbound_id,
                            std::shared_ptr<MessageQueue> queue);

  virtual void ReceiveFromQueue(Flow* flow,
                                size_t inbound_id,
                                MessageAndStream elem) = 0;

  virtual ~AbstractWorker() = 0;

 protected:
  const ProxyServerOptions options_;
  EventLoop* const event_loop_;
  Statistics statistics_;

  MessageQueue* GetOutboundQueue(size_t outbound_id);

 private:
  std::shared_ptr<QueueStats> queue_stats_;
  WorkerQueues inbound_queues_;
  WorkerQueues outbound_queues_;
};

}  // namespace rocketspeed
