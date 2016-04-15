/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/proxy2/downstream_worker.h"

#include "include/Assert.h"
#include "include/ProxyServer.h"
#include "src/messages/event_loop.h"
#include "src/messages/queues.h"
#include "src/messages/stream.h"
#include "src/messages/flow_control.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

DownstreamWorker::DownstreamWorker(const ProxyServerOptions& options,
                                   EventLoop* event_loop)
: AbstractWorker(options,
                 event_loop,
                 options.num_upstream_threads,
                 options.num_downstream_threads) {}

void DownstreamWorker::ReceiveFromQueue(Flow* flow,
                                        size_t,
                                        MessageAndStream message) {
  StreamID stream_id = message.first;
  auto type = message.second->GetMessageType();

  auto stream = event_loop_->GetInboundStream(stream_id);
  if (!stream) {
    LOG_WARN(options_.info_log, "Unknown subscriber stream: %llu", stream_id);
    return;
  }

  // Forward.
  auto ts = stream->ToTimestampedString(*message.second);
  flow->Write(stream, ts);
  stream = nullptr;

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(stream_id);
  }
}

void DownstreamWorker::operator()(StreamReceiveArg<Message> arg) {
  Flow* flow = arg.flow;
  MessageAndStream message = {arg.stream_id, std::move(arg.message)};
  StreamID stream_id = arg.stream_id;
  auto type = message.second->GetMessageType();

  // Find an UpstreamWorker to handle this stream.
  auto it = stream_to_upstream_worker_.find(stream_id);
  if (it == stream_to_upstream_worker_.end()) {
    // It's a new stream, hence we must deduce worker affinity based on the
    // shard it belongs to.
    // We determine the shard based on the first MessageSubscribe found in a
    // stream, which is okay as the subscriber always starts a stream with
    // MessageSubscribe.
    if (type == MessageType::mSubscribe) {
      auto subscribe = static_cast<MessageSubscribe*>(message.second.get());
      // TODO(stupaq) get rid of ToString()
      auto shard_id =
          options_.routing->GetShard(subscribe->GetNamespace().ToString(),
                                     subscribe->GetTopicName().ToString());
      // TODO(stupaq) power of two random choices?
      size_t id =
          MurmurHash2<size_t>()(shard_id) % options_.num_upstream_threads;
      auto result = stream_to_upstream_worker_.emplace(stream_id, id);
      RS_ASSERT(result.second);
      it = result.first;
    } else {
      LOG_ERROR(options_.info_log,
                "First message on stream: %llu type: %s, cannot route stream",
                stream_id,
                MessageTypeName(type));
      return;
    }
  }

  // Forward.
  auto id = it->second;
  flow->Write(GetOutboundQueue(id), message);

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(stream_id);
  }
}

void DownstreamWorker::CleanupState(StreamID stream_id) {
  auto erased = stream_to_upstream_worker_.erase(stream_id);
  RS_ASSERT(erased == 1);
  (void)erased;
}

}  // namespace rocketspeed
