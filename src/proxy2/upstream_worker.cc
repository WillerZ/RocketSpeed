/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/proxy2/upstream_worker.h"

#include <chrono>
#include <vector>

#include "external/folly/Memory.h"
#include "external/folly/move_wrapper.h"

#include "include/Assert.h"
#include "include/Logger.h"
#include "include/ProxyServer.h"
#include "src/client/subscriber.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/queues.h"
#include "src/messages/stream.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
PerShard::PerShard(UpstreamWorker* worker, size_t shard_id)
: worker_(worker)
, shard_id_(shard_id)
, timer_(GetLoop()->CreateTimedEventCallback([this]() { CheckRoutes(); },
                                             std::chrono::milliseconds(100)))
, router_(worker_->options_.routing->GetRouter(shard_id))
, router_version_(router_->GetVersion())
, host_(router_->GetHost())
, multiplexer_(this) {
  timer_->Enable();
}

void PerShard::AddPerStream(PerStream* per_stream) {
  auto result = streams_on_shard_.emplace(per_stream);
  (void)result;
  RS_ASSERT(result.second);
}

void PerShard::RemovePerStream(PerStream* per_stream) {
  auto result = streams_on_shard_.erase(per_stream);
  RS_ASSERT(result > 0);
}

PerShard::~PerShard() = default;

void PerShard::CheckRoutes() {
  size_t new_version = router_->GetVersion();
  // Bail out quickly if versions match.
  if (new_version == router_version_) {
    return;
  }
  router_version_ = new_version;
  {
    auto new_host = router_->GetHost();
    if (host_ == new_host) {
      return;
    }
    host_ = new_host;
  }
  LOG_INFO(GetOptions().info_log,
           "Router version changed to: %zu for shard: %zu, new host: %s",
           router_version_,
           shard_id_,
           host_.ToString().c_str());

  // Firstly notify the forwarder, so that any subscriptions can be terminated
  // before the Multiplexer starts moving them to a new host.
  std::vector<PerStream*> streams(streams_on_shard_.begin(),
                                  streams_on_shard_.end());
  for (auto per_stream : streams) {
    if (streams_on_shard_.count(per_stream)) {
      per_stream->ChangeRoute();
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
UpstreamWorker::UpstreamWorker(
    const ProxyServerOptions& options,
    EventLoop* event_loop,
    const StreamAllocator::DivisionMapping& stream_to_id)
: AbstractWorker(options,
                 event_loop,
                 options.num_upstream_threads,
                 options.num_downstream_threads)
, stream_to_id_(stream_to_id) {}

void UpstreamWorker::ReceiveFromQueue(Flow* flow,
                                      size_t inbound_id,
                                      MessageAndStream message) {
  StreamID stream_id = message.first;
  auto type = message.second->GetMessageType();
  LOG_DEBUG(GetOptions().info_log,
            "Received from queue: %zu, stream: %llu, type: %s",
            inbound_id,
            stream_id,
            MessageTypeName(type));

  // Find the PerStream to handle this stream.
  auto it = streams_.find(stream_id);
  if (it == streams_.end()) {
    // Create new stream context.
    // We determine the shard based on the first MessageSubscribe found in a
    // stream, which is okay as the subscriber always starts a stream with
    // MessageSubscribe.
    if (type == MessageType::mSubscribe) {
      auto subscribe = static_cast<MessageSubscribe*>(message.second.get());
      // TODO(stupaq) get rid of ToString()
      auto shard_id =
          options_.routing->GetShard(subscribe->GetNamespace().ToString(),
                                     subscribe->GetTopicName().ToString());

      // Reuse or create PerShard for the shard.
      auto it1 = shard_cache_.find(shard_id);
      if (it1 == shard_cache_.end()) {
        auto result = shard_cache_.emplace(
            shard_id, folly::make_unique<PerShard>(this, shard_id));
        RS_ASSERT(result.second);
        it1 = result.first;
      }

      auto result = streams_.emplace(
          stream_id,
          folly::make_unique<PerStream>(this, it1->second.get(), stream_id));
      RS_ASSERT(result.second);
      it = result.first;
    } else {
      // This may happen if routes change or there is a race between server and
      // client closing the stream.
      LOG_WARN(GetOptions().info_log,
               "First message on unknown stream: %llu type: %s, cannot "
               "determine shard",
               stream_id,
               MessageTypeName(type));
      return;
    }
  }

  // Forward.
  auto per_stream = it->second.get();
  per_stream->ReceiveFromWorker(flow, std::move(message));

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(per_stream);
    per_stream = nullptr;
  }
}

void UpstreamWorker::ReceiveFromStream(Flow* flow,
                                       PerStream* per_stream,
                                       MessageAndStream message) {
  StreamID stream_id = message.first;
  auto type = message.second->GetMessageType();
  LOG_DEBUG(GetOptions().info_log,
            "Received from stream: %llu (%p), type: %s",
            stream_id,
            per_stream,
            MessageTypeName(type));

  // Forward.
  size_t id = stream_to_id_(stream_id);
  flow->Write(GetOutboundQueue(id), message);

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(per_stream);
    per_stream = nullptr;
  }
}

UpstreamWorker::~UpstreamWorker() = default;

void UpstreamWorker::CleanupState(PerStream* per_stream) {
  auto per_shard = per_stream->GetShard();
  auto stream_id = per_stream->GetStream();

  auto erased = streams_.erase(stream_id);
  RS_ASSERT(erased == 1);
  (void)erased;

  if (per_shard->IsEmpty()) {
    auto it = shard_cache_.find(per_shard->GetShardID());
    RS_ASSERT(it != shard_cache_.end());
    if (it != shard_cache_.end()) {
      auto moved_per_shard = folly::makeMoveWrapper(std::move(it->second));
      GetLoop()->AddTask(
          [moved_per_shard]() mutable { moved_per_shard->reset(); });
      shard_cache_.erase(it);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
PerStream::PerStream(UpstreamWorker* worker,
                     PerShard* per_shard,
                     StreamID downstream_id)
: worker_(worker), per_shard_(per_shard), downstream_id_(downstream_id) {
  per_shard_->AddPerStream(this);
}

void PerStream::ReceiveFromWorker(Flow* flow, MessageAndStream message) {
  StreamID downstream_id = message.first;
  auto type = message.second->GetMessageType();

  // Determine whether the topic of the subscription is hot and perform
  // subscription-level proxying if it is.
  // FIXME

  // The topic of the subscription is not hot. Perform stream-level proxying.
  if (!upstream_) {
    const auto& host = per_shard_->GetHost();
    // Create an upstream for the downstream.
    if (!host) {
      LOG_ERROR(GetOptions().info_log,
                "Failed to obtain host for shard %zu",
                per_shard_->GetShardID());
      // We cannot obtain host for a shard and we should not queue up messages,
      // hence we must deliver a goodbye message back to the client. There is no
      // need to deliver a goodbye message to the server, as the stream have not
      // yet reached it.
      ForceCloseStream();
      return;
    }
    upstream_ = GetLoop()->OpenStream(host);
    if (!upstream_) {
      LOG_ERROR(GetOptions().info_log,
                "Failed to open connection to %s",
                host.ToString().c_str());
      // This error, although synchronous, is equivalent to a receipt of
      // MessageGoodbye. There is no need to deliver a goodbye message to the
      // server, as the stream have not yet reached it.
      ForceCloseStream();
      return;
    }

    // Create and set a receiver that performs the remapping and manages its own
    // lifetime.
    class TheReceiver : public StreamReceiver {
     public:
      TheReceiver(PerStream* per_stream, StreamID stream_id)
      : per_stream_(per_stream), downstream_id_(stream_id) {}

      void operator()(StreamReceiveArg<Message> arg) override {
        RS_ASSERT(per_stream_->upstream_->GetLocalID() == arg.stream_id);
        per_stream_->ReceiveFromStream(
            arg.flow, {downstream_id_, std::move(arg.message)});
        // TODO(stupaq) MarkHostDown
      }

      void EndStream(StreamID) override {
        // It is guaranteed that the stream will not receive any more signals
        // and we never use the same receiver for two different streams, hence
        // it's safe to commit suicide.
        delete this;
      }

     private:
      PerStream* const per_stream_;
      const StreamID downstream_id_;
    };
    upstream_->SetReceiver(new TheReceiver(this, downstream_id));
  }

  // Forward.
  auto ts = upstream_->ToTimestampedString(*message.second);
  flow->Write(upstream_.get(), ts);

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState();
  }
}

void PerStream::ReceiveFromStream(Flow* flow, MessageAndStream message) {
  RS_ASSERT(downstream_id_ == message.first);
  auto type = message.second->GetMessageType();

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState();
  }

  // Forward (the StreamID is already remapped by the StreamReceiver).
  worker_->ReceiveFromStream(flow, this, std::move(message));
}

void PerStream::ChangeRoute() {
  // We pretend that each downstream received a goodbye message.
  ForceCloseStream();
}

PerStream::~PerStream() {
  per_shard_->RemovePerStream(this);
}

void PerStream::CleanupState() {
  upstream_.reset();
}

void PerStream::ForceCloseStream() {
  MessageAndStream message;
  message.first = downstream_id_;
  message.second.reset(new MessageGoodbye(Tenant::GuestTenant,
                                          MessageGoodbye::Code::SocketError,
                                          MessageGoodbye::OriginType::Server));
  SourcelessFlow no_flow(GetLoop()->GetFlowControl());
  ReceiveFromStream(&no_flow, std::move(message));
  // A MessageGoodbye will be send to the server as a result of state cleanup
  // performed in ::ReceiveFromStream.
}

////////////////////////////////////////////////////////////////////////////////
Multiplexer::Multiplexer(PerShard* per_shard) : per_shard_(per_shard) {}

EventLoop* Multiplexer::GetLoop() const {
  return per_shard_->GetLoop();
}

const ProxyServerOptions& Multiplexer::GetOptions() const {
  return per_shard_->GetOptions();
}

bool Multiplexer::TryHandle(Flow* flow, const MessageAndStream& message) {
  auto type = message.second->GetMessageType();
  switch (type) {
    case MessageType::mSubscribe: {
      // auto subscribe =
      // static_cast<MessageSubscribe*>(message.second.get());
      // FIXME
      return false;
    }
    case MessageType::mUnsubscribe: {
      // auto unsubscribe =
      // static_cast<MessageUnsubscribe*>(message.second.get());
      // FIXME
      return false;
    }
    case MessageType::mGoodbye: {
      // FIXME
      return true;
    }
    default: {
      LOG_WARN(GetOptions().info_log,
               "Weird message type from DownstreamWorker: %s",
               MessageTypeName(type));
      return false;
    }
  }
}

Multiplexer::~Multiplexer() = default;

/*
// FIXME
void PerShard::Handle(Flow* flow, MessageAndStream message) {
  auto type = message.second->GetMessageType();
  switch (type) {
    case MessageType::mSubscribe: {
      auto subscribe = static_cast<MessageSubscribe*>(message.second.get());
      if (worker_->options_.hot_topics->IsHotTopic(subscribe->GetNamespace(),
                                                   subscribe->GetTopicName())) {
        auto handled = multiplexer_.TryHandle(flow, message);
        RS_ASSERT(handled);
        (void)handled;
      } else {
        per_shard_.Handle(flow, std::move(message));
      }
    } break;
    case MessageType::mUnsubscribe: {
      if (!multiplexer_.TryHandle(flow, message)) {
        per_shard_.Handle(flow, std::move(message));
      }
    } break;
    case MessageType::mGoodbye: {
      auto handled = multiplexer_.TryHandle(flow, message);
      RS_ASSERT(handled);
      (void)handled;
      per_shard_.Handle(flow, std::move(message));
    } break;
    default: {
      LOG_WARN(GetOptions().info_log,
               "Weird message type from DownstreamWorker: %s",
               MessageTypeName(type));
    }
  }
}

void PerShard::ReceiveFromForwarder(Flow* flow, MessageAndStream message) {
  auto type = message.second->GetMessageType();
  if (type == MessageType::mGoodbye) {
    // Upon receipt of a MessageGoodbye from the server we must perform the same
    // actions as if we have received the message from the subscriber, except
    // that we send the message in the opposite direction.
    auto handled = multiplexer_->TryHandle(flow, message);
    RS_ASSERT(handled);
    (void)handled;
  }

  // Forward.
  worker_->ReceiveFromShard(flow, this, std::move(message));
}
*/

}  // namespace rocketspeed
