/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/proxy2/upstream_worker.h"

#include <algorithm>
#include <chrono>
#include <vector>

#include "include/Assert.h"
#include "include/Logger.h"
#include "include/ProxyServer.h"
#include "src/messages/event_loop.h"
#include "src/messages/flow_control.h"
#include "src/messages/queues.h"
#include "src/messages/stream.h"
#include "src/proxy2/multiplexer.h"
#include "src/util/memory.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
PerShard::PerShard(UpstreamWorker* worker, size_t shard_id)
: worker_(worker)
, shard_id_(shard_id)
, timer_(GetLoop()->CreateTimedEventCallback([this]() { CheckRoutes(); },
                                             std::chrono::milliseconds(100)))
, router_(worker_->GetOptions().routing)
, router_version_(router_->GetVersion())
, host_(router_->GetHost(shard_id)) {
  timer_->Enable();
}

void PerShard::AddPerStream(PerStream* per_stream, IntroProperties props) {
  // Check if multiplexer already exists
  // Its ok to loop since the number of multiplexers is small
  auto it = std::find_if(
      multiplexers_.begin(), multiplexers_.end(), [&props](const auto& m) {
        return m.first->GetProperties() == props;
      });

  if (it != multiplexers_.end()) {
    auto ptr = it->first;
    streams_on_shard_.emplace(per_stream, ptr);

    auto mit = multiplexers_ref_count_.find(ptr);
    RS_ASSERT(mit != multiplexers_ref_count_.end());
    ++mit->second;
    return;
  }

  auto multiplexer = std::make_unique<Multiplexer>(this, std::move(props));
  auto ptr = multiplexer.get();

  auto result = multiplexers_.emplace(ptr, std::move(multiplexer));
  RS_ASSERT(result.second);

  if (multiplexers_.size() >= 10) {
    // Number of multiplexers should be low, WARN if its not the case
    LOG_WARN(GetOptions().info_log,
             "Number of multiplexers created for shard %zu is too high : %zu",
             shard_id_,
             multiplexers_.size());
  }

  streams_on_shard_.emplace(per_stream, ptr);
  multiplexers_ref_count_.emplace(ptr, 1);
}

void PerShard::RemovePerStream(PerStream* per_stream) {
  auto it = streams_on_shard_.find(per_stream);
  RS_ASSERT(it != streams_on_shard_.end());

  Multiplexer* multiplexer = it->second;

  auto mit = multiplexers_ref_count_.find(multiplexer);
  RS_ASSERT(mit != multiplexers_ref_count_.end());

  if (--mit->second == 0) {
    multiplexers_.erase(multiplexer);
    multiplexers_ref_count_.erase(mit);
  }

  streams_on_shard_.erase(per_stream);
}

Multiplexer* PerShard::GetMultiplexer(PerStream* per_stream) {
  auto it = streams_on_shard_.find(per_stream);
  // We should have a multiplexer
  RS_ASSERT(it != streams_on_shard_.end());
  return it->second;
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
    auto new_host = router_->GetHost(shard_id_);
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

  // Firstly notify the forwarder, so that any subscriptions can be
  // terminated before the Multiplexer starts moving them to a new host.
  // Copy stream ptr's as streams might get cleaned up and ptr rendered
  // invalid.
  std::vector<PerStream*> streams(streams_on_shard_.size());
  std::transform(streams_on_shard_.begin(),
                 streams_on_shard_.end(),
                 streams.begin(),
                 [](auto kv) { return kv.first; });
  for (auto stream : streams) {
    if (streams_on_shard_.count(stream)) {
      stream->ChangeRoute();
    }
  }

  // Afterwards, notify Multiplexers.
  for (auto& multiplexer : multiplexers_) {
    multiplexer.first->ChangeRoute();
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
, stream_to_id_(stream_to_id) {
}

void UpstreamWorker::Start() {
  auto prefix = GetOptions().stats_prefix + "upstream.";
  stats_.num_streams = statistics_.AddCounter(prefix + "num_streams");
  stats_.num_shards = statistics_.AddCounter(prefix + "num_shards");
}

void UpstreamWorker::ReceiveFromQueue(Flow* flow,
                                      size_t inbound_id,
                                      MessageAndStream message) {
  StreamID stream_id = message.first;
  auto type = message.second->GetMessageType();
  LOG_DEBUG(GetOptions().info_log,
            "UpstreamWorker(%p)::ReceiveFromQueue(%zu, %" PRIu64 ", %s)",
            this,
            inbound_id,
            stream_id,
            MessageTypeName(type));

  // Find the PerStream to handle this stream.
  auto it = streams_.find(stream_id);
  if (it == streams_.end()) {
    // Create new stream context.
    // We determine the shard from the stream MessageIntroduction
    // It is guaranteed that the first message on the stream would be an
    // introduction message.
    RS_ASSERT_DBG(type == MessageType::mIntroduction);
    if (type != MessageType::mIntroduction) {
      // TODO(rishijhelumi) : close stream instead
      LOG_ERROR(
          options_.info_log,
          "First message on stream: %" PRIu64 " type: %s, cannot determine shard",
          stream_id,
          MessageTypeName(type));
      return;
    }

    auto introduction = static_cast<MessageIntroduction*>(message.second.get());
    const auto& props = introduction->GetStreamProperties();

    auto shard = props.find(PropertyShardID);
    RS_ASSERT_DBG(shard != props.end());
    if (shard == props.end()) {
      LOG_ERROR(options_.info_log,
                "Cannot get shard for stream: %" PRIu64 " from introduction message, "
                "property not set, cannot route stream.",
                stream_id);
      return;
    }

    size_t shard_id = std::numeric_limits<size_t>::max();
    try {
      shard_id = std::stoul(shard->second, nullptr, 0);
    } catch (const std::exception& ex) {
      LOG_WARN(options_.info_log,
               "Cannot decode shard : %s, error : %s",
               shard->second.c_str(),
               ex.what());
    }

    // Reuse or create PerShard for the shard.
    auto it1 = shard_cache_.find(shard_id);
    if (it1 == shard_cache_.end()) {
      auto result = shard_cache_.emplace(
          shard_id, std::make_unique<PerShard>(this, shard_id));
      RS_ASSERT(result.second);
      it1 = result.first;
      stats_.num_shards->Add(1);
    }

    auto result = streams_.emplace(
        stream_id,
        std::make_unique<PerStream>(this, it1->second.get(), stream_id));
    RS_ASSERT(result.second);
    it = result.first;
    stats_.num_streams->Add(1);
  }

  // Forward.
  auto per_stream = it->second.get();
  per_stream->ReceiveFromWorker(flow, std::move(message));

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(per_stream);
  }
}

void UpstreamWorker::ReceiveFromStream(Flow* flow,
                                       PerStream* per_stream,
                                       MessageAndStream message) {
  StreamID stream_id = message.first;
  auto type = message.second->GetMessageType();
  LOG_DEBUG(GetOptions().info_log,
            "UpstreamWorker(%p)::ReceiveFromStream(%p (%" PRIu64 "), %s)",
            this,
            per_stream,
            stream_id,
            MessageTypeName(type));

  // Forward.
  size_t id = stream_to_id_(stream_id);
  flow->Write(GetOutboundQueue(id), message);

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState(per_stream);
  }
}

UpstreamWorker::~UpstreamWorker() = default;

void UpstreamWorker::CleanupState(PerStream* per_stream) {
  auto per_shard = per_stream->GetShard();
  auto stream_id = per_stream->GetStream();

  auto erased = streams_.erase(stream_id);
  RS_ASSERT(erased == 1);
  stats_.num_streams->Add(-1);

  if (per_shard->IsEmpty()) {
    auto it = shard_cache_.find(per_shard->GetShardID());
    RS_ASSERT(it != shard_cache_.end());
    if (it != shard_cache_.end()) {
      GetLoop()->AddTask(MakeDeferredDeleter(it->second));
      shard_cache_.erase(it);
      stats_.num_shards->Add(-1);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
PerStream::PerStream(UpstreamWorker* worker,
                     PerShard* per_shard,
                     StreamID downstream_id)
: worker_(worker), per_shard_(per_shard), downstream_id_(downstream_id) {
  // Create stats.
  auto prefix = per_shard->GetOptions().stats_prefix + "per_stream.";
  auto stats = per_shard->GetStatistics();
  stats_.num_downstream_subscriptions =
      stats->AddCounter(prefix + "num_downstream_subscriptions");
}

namespace {

// Create and set a receiver that performs the remapping and manages its own
// lifetime.
class TheReceiver : public StreamReceiver {
 public:
  explicit TheReceiver(PerStream* per_stream) : per_stream_(per_stream) {}

  void operator()(StreamReceiveArg<Message> arg) override {
    StreamID upstream_id = arg.stream_id;
    StreamID downstream_id = per_stream_->GetStream();
    auto type = arg.message->GetMessageType();
    LOG_DEBUG(per_stream_->GetOptions().info_log,
              "PerStream(%" PRIu64 ")::TheReceiver::operator()(%" PRIu64 ", %s)",
              downstream_id,
              upstream_id,
              MessageTypeName(type));

    per_stream_->ReceiveFromStream(arg.flow,
                                   {downstream_id, std::move(arg.message)});
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
};

}  // namespace

void PerStream::ReceiveFromWorker(Flow* flow, MessageAndStream message) {
  RS_ASSERT(downstream_id_ == message.first);
  auto type = message.second->GetMessageType();
  LOG_DEBUG(GetOptions().info_log,
            "PerStream(%" PRIu64 ")::ReceivedFromWorker(%s)",
            downstream_id_,
            MessageTypeName(type));

  // Determine whether the topic of the subscription is hot and perform
  // subscription-level proxying if it is.
  switch (type) {
    case MessageType::mIntroduction: {
      // The first message on the stream would be an introduction message
      // Store the message for this stream, to be used for
      // proxy stream introduction if multiplexer is not used
      RS_ASSERT_DBG(intro_parameters_ == nullptr);
      if (intro_parameters_ || upstream_) {
        // We have already received an introduction
        return;
      }
      auto intro = static_cast<MessageIntroduction*>(message.second.get());

      // Set intro_properties for this stream
      intro_parameters_.reset(
          new IntroParameters(intro->GetTenantID(),
                              intro->GetStreamProperties(),
                              intro->GetClientProperties()));
      // Add Stream to shard with these properties
      per_shard_->AddPerStream(this, intro->GetStreamProperties());
    }
      return;
    case MessageType::mSubscribe: {
      auto subscribe = static_cast<MessageSubscribe*>(message.second.get());
      auto namespace_id = subscribe->GetNamespace();
      auto topic_name = subscribe->GetTopicName();
      if (GetOptions().hot_topics->IsHotTopic(namespace_id, topic_name)) {
        auto downstream_sub = subscribe->GetSubID();
        // Let Multiplexer handle the subscription and record handle.
        auto ptr = per_shard_->GetMultiplexer(this)->Subscribe(
            flow,
            subscribe->GetTenantID(),
            namespace_id,
            topic_name,
            subscribe->GetStartSequenceNumber(),
            this,
            downstream_sub);
        auto result = downstream_to_upstream_.emplace(downstream_sub, ptr);
        RS_ASSERT(result.second);
        (void)result;
        stats_.num_downstream_subscriptions->Add(1);
        return;
      }
      // Otherwise perform stream-level proxying.
    } break;
    case MessageType::mUnsubscribe: {
      auto unsubscribe = static_cast<MessageUnsubscribe*>(message.second.get());
      auto downstream_sub = unsubscribe->GetSubID();
      // Find out if a subscription has been multiplexed, let Multiplexer handle
      // this even if so.
      auto it = downstream_to_upstream_.find(downstream_sub);
      if (it != downstream_to_upstream_.end()) {
        per_shard_->GetMultiplexer(this)->Unsubscribe(
            flow, it->second, this, it->first);
        downstream_to_upstream_.erase(it);
        stats_.num_downstream_subscriptions->Add(-1);
        return;
      }
      // Otherwise perform stream-level proxying.
    } break;
    default:
      // Other messages than metadata updates must be handled by the
      // stream-level proxy.
      break;
  }

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

    // Open an upstream for the downstream with the params that was received
    // from the inbound stream.
    auto params = *intro_parameters_;
    upstream_ = GetLoop()->OpenStream(host, std::move(params));
    // Intro Parameters are not needed anymore
    intro_parameters_ = nullptr;

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

    upstream_->SetReceiver(new TheReceiver(this));
  }

  // Forward.
  flow->Write(upstream_.get(), message.second);

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState();
  }
}

void PerStream::ReceiveFromStream(Flow* flow, MessageAndStream message) {
  RS_ASSERT(downstream_id_ == message.first);
  auto type = message.second->GetMessageType();
  LOG_DEBUG(GetOptions().info_log,
            "PerStream(%" PRIu64 ")::ReceivedFromStream(%s)",
            downstream_id_,
            MessageTypeName(type));

  // Clean up the state if this is the last message on the stream.
  if (type == MessageType::mGoodbye) {
    CleanupState();
  }

  // Forward (the StreamID is already remapped by the StreamReceiver).
  worker_->ReceiveFromStream(flow, this, std::move(message));
}

void PerStream::ReceiveFromMultiplexer(Flow* flow, MessageAndStream message) {
  RS_ASSERT(downstream_id_ == message.first);
  const auto type = message.second->GetMessageType();
  RS_ASSERT(
      type == MessageType::mDeliverGap || type == MessageType::mDeliverData ||
      type == MessageType::mDeliverBatch || type == MessageType::mUnsubscribe);
  LOG_DEBUG(GetOptions().info_log,
            "PerStream(%" PRIu64 ")::ReceivedFromStream(%s)",
            downstream_id_,
            MessageTypeName(type));

  // Clear the state for that subscripion on forced unsubscribe.
  if (MessageType::mUnsubscribe == message.second->GetMessageType()) {
    auto unsubscribe = static_cast<MessageUnsubscribe*>(message.second.get());
    auto downstream_sub = unsubscribe->GetSubID();
    auto result = downstream_to_upstream_.erase(downstream_sub);
    RS_ASSERT(result == 1);
    stats_.num_downstream_subscriptions->Add(-1);
  }

  // Forward.
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
  // Close the stream to the server.
  upstream_.reset();
  // Terminate all subscriptions.
  SourcelessFlow no_flow(GetLoop()->GetFlowControl());
  auto subscriptions = std::move(downstream_to_upstream_);
  for (auto& entry : subscriptions) {
    stats_.num_downstream_subscriptions->Add(-1);
    per_shard_->GetMultiplexer(this)->Unsubscribe(
        &no_flow, entry.second, this, entry.first);
  }
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

}  // namespace rocketspeed
