/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/proxy2/abstract_worker.h"

#include "include/Assert.h"
#include "include/ProxyServer.h"
#include "src/messages/queues.h"
#include "src/messages/types.h"
#include "src/messages/flow_control.h"

namespace rocketspeed {

AbstractWorker::AbstractWorker(const ProxyServerOptions& options,
                               EventLoop* event_loop,
                               size_t num_inbound_queues,
                               size_t num_outbound_queues)
: options_(options)
, event_loop_(event_loop)
, queue_stats_(new QueueStats("proxy2-worker"))
, inbound_queues_(num_inbound_queues)
, outbound_queues_(num_outbound_queues) {}

std::shared_ptr<MessageQueue> AbstractWorker::CreateInboundQueue(
    size_t inbound_id) {
  RS_ASSERT(inbound_id < inbound_queues_.size());
  RS_ASSERT(!inbound_queues_[inbound_id]);
  inbound_queues_[inbound_id] =
      std::make_shared<MessageQueue>(options_.info_log, queue_stats_, 10000);
  event_loop_->GetFlowControl()->Register<MessageAndStream>(
      inbound_queues_[inbound_id].get(),
      [this, inbound_id](Flow* flow, MessageAndStream message) {
        ReceiveFromQueue(flow, inbound_id, std::move(message));
      });
  return inbound_queues_[inbound_id];
}

void AbstractWorker::ConnectOutboundQueue(size_t outbound_id,
                                          std::shared_ptr<MessageQueue> queue) {
  RS_ASSERT(outbound_id < outbound_queues_.size());
  RS_ASSERT(!outbound_queues_[outbound_id]);
  outbound_queues_[outbound_id] = std::move(queue);
}

AbstractWorker::~AbstractWorker() = default;

MessageQueue* AbstractWorker::GetOutboundQueue(size_t outbound_id) {
  RS_ASSERT(outbound_id < outbound_queues_.size());
  return outbound_queues_[outbound_id].get();
}

}  // namespace rocketspeed
