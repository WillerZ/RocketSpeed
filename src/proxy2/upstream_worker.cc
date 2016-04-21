/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/proxy2/upstream_worker.h"

#include <chrono>

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
, multiplexer_(this)
, forwarder_(this) {
  timer_->Enable();
  auto new_host = router_->GetHost();
  forwarder_.ChangeRoute(new_host);
  multiplexer_.ChangeRoute(new_host);
}

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
        forwarder_.Handle(flow, std::move(message));
      }
    } break;
    case MessageType::mUnsubscribe: {
      if (!multiplexer_.TryHandle(flow, message)) {
        forwarder_.Handle(flow, std::move(message));
      }
    } break;
    case MessageType::mGoodbye: {
      auto handled = multiplexer_.TryHandle(flow, message);
      RS_ASSERT(handled);
      (void)handled;
      forwarder_.Handle(flow, std::move(message));
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
    auto handled = multiplexer_.TryHandle(flow, message);
    RS_ASSERT(handled);
    (void)handled;
  }

  // Forward.
  worker_->ReceiveFromShard(flow, this, std::move(message));
}

PerShard::~PerShard() = default;

void PerShard::CheckRoutes() {
  size_t new_version = router_->GetVersion();
  // Bail out quickly if versions match.
  if (new_version == router_version_) {
    return;
  }
  router_version_ = new_version;
  auto new_host = router_->GetHost();
  LOG_INFO(GetOptions().info_log,
           "Router version changed to: %zu for shard: %zu, new host: %s",
           router_version_,
           shard_id_,
           new_host.ToString().c_str());

  // First notify the forwrder, so that any subscriptions can be terminated
  // before the Multiplexer starts moving them to a new host.
  forwarder_.ChangeRoute(new_host);
  multiplexer_.ChangeRoute(new_host);
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
                                      size_t,
                                      MessageAndStream message) {
  StreamID stream_id = message.first;
  auto type = message.second->GetMessageType();

  // Find the PerShard to handle this stream.
  auto it = stream_to_shard_.find(stream_id);
  if (it == stream_to_shard_.end()) {
    // It's a new stream, hence we must deduce PerShard affinity based on the
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

      // Reuse or create PerShard for the shard.
      auto it1 = shard_cache_.find(shard_id);
      if (it1 == shard_cache_.end()) {
        auto result = shard_cache_.emplace(
            shard_id, folly::make_unique<PerShard>(this, shard_id));
        RS_ASSERT(result.second);
        it1 = result.first;
      }

      auto result = stream_to_shard_.emplace(stream_id, it1->second.get());
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
  auto per_shard = it->second;
  per_shard->Handle(flow, std::move(message));

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(per_shard, stream_id);
    per_shard = nullptr;
  }
}

void UpstreamWorker::ReceiveFromShard(Flow* flow,
                                      PerShard* per_shard,
                                      MessageAndStream message) {
  StreamID stream_id = message.first;
  auto type = message.second->GetMessageType();

  // Forward.
  size_t id = stream_to_id_(stream_id);
  flow->Write(GetOutboundQueue(id), message);

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(per_shard, stream_id);
    per_shard = nullptr;
  }
}

UpstreamWorker::~UpstreamWorker() = default;

void UpstreamWorker::CleanupState(PerShard* per_shard, StreamID stream_id) {
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
  auto erased = stream_to_shard_.erase(stream_id);
  RS_ASSERT(erased == 1);
  (void)erased;
}

////////////////////////////////////////////////////////////////////////////////
Forwarder::Forwarder(PerShard* per_shard) : per_shard_(per_shard) {}

EventLoop* Forwarder::GetLoop() const {
  return per_shard_->GetLoop();
}

const ProxyServerOptions& Forwarder::GetOptions() const {
  return per_shard_->GetOptions();
}

void Forwarder::Handle(Flow* flow, MessageAndStream message) {
  StreamID downstream_id = message.first;
  auto type = message.second->GetMessageType();

  // Find the upstream stream to send this message on.
  auto it = downstream_to_upstream_.find(downstream_id);
  if (it == downstream_to_upstream_.end()) {
    // Create an upstream for the downstream.
    if (!host_) {
      LOG_ERROR(GetOptions().info_log,
                "Failed to obtain host for shard %zu",
                per_shard_->GetShardID());
      // We cannot obtain host for a shard and we should not queue up messages,
      // hence we must deliver a goodbye message back to the client. There is no
      // need to deliver a goodbye message to the server, as the stream have not
      // yet reached it.
      ForceCloseStream(downstream_id);
      return;
    }
    auto new_upstream = GetLoop()->OpenStream(host_);
    if (!new_upstream) {
      LOG_ERROR(GetOptions().info_log,
                "Failed to open connection to %s",
                host_.ToString().c_str());
      // This error, although synchronous, is equivalent to a receipt of
      // MessageGoodbye. There is no need to deliver a goodbye message to the
      // server, as the stream have not yet reached it.
      ForceCloseStream(downstream_id);
      return;
    }

    // Create and set a receiver that performs the remapping and manages its own
    // lifetime.
    class TheReceiver : public StreamReceiver {
     public:
      TheReceiver(Forwarder* forwarder, StreamID stream_id)
      : forwarder_(forwarder), downstream_id_(stream_id) {}

      void operator()(StreamReceiveArg<Message> arg) override {
        forwarder_->ReceiveFromStream(arg.flow,
                                      {downstream_id_, std::move(arg.message)});
        // TODO(stupaq) MarkHostDown
      }

      void EndStream(StreamID) override {
        // It is guaranteed that the stream will not receive any more signals
        // and we never use the same receiver for two different streams, hence
        // it's safe to commit suicide.
        delete this;
      }

     private:
      Forwarder* const forwarder_;
      const StreamID downstream_id_;
    };
    new_upstream->SetReceiver(new TheReceiver(this, downstream_id));

    auto result =
        downstream_to_upstream_.emplace(downstream_id, std::move(new_upstream));
    RS_ASSERT(result.second);
    it = result.first;
  }

  // Forward.
  auto upstream = it->second.get();
  auto ts = upstream->ToTimestampedString(*message.second);
  flow->Write(upstream, ts);

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(downstream_id);
  }
}

void Forwarder::ReceiveFromStream(Flow* flow, MessageAndStream message) {
  StreamID downstream_id = message.first;
  auto type = message.second->GetMessageType();

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(downstream_id);
  }

  // Forward (the StreamID is already remapped by the StreamReceiver).
  per_shard_->ReceiveFromForwarder(flow, std::move(message));
}

void Forwarder::ChangeRoute(const HostId& new_host) {
  host_ = new_host;
  // We pretend that each downstream received a goodbye message. Since all
  // upstreams are owned by this forwarder, servers will receive notifications
  // as a result of cleaning up the state.
  while (!downstream_to_upstream_.empty()) {
    auto downstream_id = downstream_to_upstream_.begin()->first;
    ForceCloseStream(downstream_id);
  }
}

Forwarder::~Forwarder() = default;

void Forwarder::ForceCloseStream(StreamID downstream_id) {
  MessageAndStream message;
  message.first = downstream_id;
  message.second.reset(new MessageGoodbye(Tenant::GuestTenant,
                                          MessageGoodbye::Code::SocketError,
                                          MessageGoodbye::OriginType::Server));
  SourcelessFlow no_flow(GetLoop()->GetFlowControl());
  ReceiveFromStream(&no_flow, std::move(message));
  // A MessageGoodbye will be send to the server as a result of state cleanup
  // performed in ::ReceiveFromStream.
}

void Forwarder::CleanupState(StreamID downstream_id) {
  downstream_to_upstream_.erase(downstream_id);
  // Could erase no element if the stream was closed as a result of a failure
  // in initial routing, see invocations of ::ForceCloseStream.
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

void Multiplexer::ChangeRoute(const HostId& new_host) {
  // FIXME
}

Multiplexer::~Multiplexer() = default;

}  // namespace rocketspeed
