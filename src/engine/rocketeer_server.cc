// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "include/RocketeerServer.h"

#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "include/Env.h"
#include "include/HostId.h"
#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/flow_control.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
#include "src/messages/stream_allocator.h"
#include "src/util/common/guid_generator.h"
#include "src/util/common/subscription_id.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
RocketeerOptions::RocketeerOptions()
: env(Env::Default())
, port(DEFAULT_PORT)
, stats_prefix("rocketeer.") {
  Status st = env->StdErrLogger(&info_log);
  if (!st.ok()) {
    fprintf(stderr, "Failed to create stderr logger, logging disabled!\n");
    info_log = std::make_shared<NullLogger>();
  }
}

////////////////////////////////////////////////////////////////////////////////
class InboundSubscription {
 public:
  explicit InboundSubscription(SequenceNumber _prev_seqno)
  : prev_seqno(_prev_seqno) {}

  SequenceNumber prev_seqno;
};

////////////////////////////////////////////////////////////////////////////////
class CommunicationRocketeer : public Rocketeer {
 public:
  explicit CommunicationRocketeer(Rocketeer* rocketeer);

  void HandleNewSubscription(Flow* flow,
                             InboundID inbound_id,
                             SubscriptionParameters params) final override;

  void HandleTermination(Flow* flow,
                         InboundID inbound_id,
                         Rocketeer::TerminationSource source) final override;

  void HandleHasMessageSince(Flow* flow,
                             InboundID inbound_id,
                             NamespaceID namespace_id,
                             Topic topic,
                             Epoch epoch,
                             SequenceNumber seqno) final override;

  void HandleDisconnect(Flow* flow, StreamID stream_id) final override;

  void Deliver(Flow* flow,
               InboundID inbound_id,
               NamespaceID namespace_id,
               Topic topic,
               SequenceNumber seqno,
               std::string payload,
               MsgId msg_id = MsgId()) final override;

  void DeliverBatch(Flow* flow,
                    StreamID stream_id,
                    std::vector<RocketeerMessage> messages) final override;

  void Advance(Flow* flow,
               InboundID inbound_id,
               NamespaceID namespace_id,
               Topic topic,
               SequenceNumber seqno) final override;

  void NotifyDataLoss(Flow* flow,
                      InboundID inbound_id,
                      NamespaceID namespace_id,
                      Topic topic,
                      SequenceNumber seqno) final override;

  void Unsubscribe(Flow* flow,
                   InboundID inbound_id,
                   NamespaceID namespace_id,
                   Topic topic,
                   UnsubscribeReason reason) final override;


  void HasMessageSinceResponse(Flow* flow,
                               InboundID inbound_id,
                               NamespaceID namespace_id,
                               Topic topic,
                               Epoch epoch,
                               SequenceNumber seqno,
                               HasMessageSinceResult response,
                               std::string info) final override;

  size_t GetID() const;

 private:
  friend class RocketeerServer;

  struct Stats {
    explicit Stats(const std::string& prefix);

    Counter* subscribes;
    Counter* unsubscribes;
    Counter* terminations;
    Counter* inbound_subscriptions;
    Counter* dropped_reordered;
    Statistics all;
  };

  ThreadCheck thread_check_;
  // RocketeerServer, which owns both the implementation and this object.
  RocketeerServer* server_;

  // The rocketeer that is being wrapped
  Rocketeer* above_rocketeer_;

  // An ID assigned by the server.
  size_t id_;
  std::unique_ptr<Stats> stats_;

  struct StreamState {
    explicit StreamState(TenantID _tenant_id)
    : tenant_id(_tenant_id)
    , num_subscriptions(0) {}

    TenantID tenant_id;
    size_t num_subscriptions;
    std::unordered_map<SubscriptionID, InboundSubscription> inbound;
  };
  std::unordered_map<StreamID, StreamState> stream_state_;

  void Initialize(RocketeerServer* server, size_t id);

  const Statistics& GetStatisticsInternal();

  TenantID GetTenant(StreamID stream_id) const;

  InboundSubscription* Find(const InboundID& inbound_id);

  void SendResponse(Flow* flow,
                    StreamID stream_id,
                    std::unique_ptr<Message> message);

  void Receive(
      Flow* flow, std::unique_ptr<MessageSubscribe> subscribe, StreamID origin);

  void Receive(
      Flow* flow, std::unique_ptr<MessageUnsubscribe> unsubscribe,
      StreamID origin);

  void Receive(
      Flow* flow, std::unique_ptr<MessageBacklogQuery> query,
      StreamID origin);

  void Receive(
      Flow* flow, std::unique_ptr<MessageGoodbye> goodbye, StreamID origin);

  void SendGapMessage(
      Flow* flow, InboundID inbound_id, NamespaceID namespace_id, Topic topic,
      SequenceNumber seqno, GapType gap_type);
};

CommunicationRocketeer::CommunicationRocketeer(Rocketeer* rocketeer)
: server_(nullptr),
  above_rocketeer_(rocketeer) {
  rocketeer->SetBelowRocketeer(this);
}

void CommunicationRocketeer::HandleNewSubscription(
    Flow* flow, InboundID inbound_id, SubscriptionParameters params) {
  above_rocketeer_->HandleNewSubscription(flow, inbound_id, std::move(params));
}

void CommunicationRocketeer::HandleTermination(
    Flow* flow, InboundID inbound_id, TerminationSource source) {
  above_rocketeer_->HandleTermination(flow, inbound_id, source);
}

void CommunicationRocketeer::HandleHasMessageSince(
    Flow* flow, InboundID inbound_id, NamespaceID namespace_id, Topic topic,
    Epoch epoch, SequenceNumber seqno) {
  above_rocketeer_->HandleHasMessageSince(flow, inbound_id,
      std::move(namespace_id), std::move(topic), std::move(epoch), seqno);
}

void CommunicationRocketeer::HandleDisconnect(Flow* flow, StreamID stream_id) {
  above_rocketeer_->HandleDisconnect(flow, stream_id);
}

void CommunicationRocketeer::Deliver(Flow* flow,
                                     InboundID inbound_id,
                                     NamespaceID namespace_id,
                                     Topic topic,
                                     SequenceNumber seqno,
                                     std::string payload,
                                     MsgId msg_id) {
  thread_check_.Check();

  if (msg_id.Empty()) {
    msg_id = GUIDGenerator::ThreadLocalGUIDGenerator()->Generate();
  }
  if (auto* sub = Find(inbound_id)) {
    if (sub->prev_seqno < seqno) {
      auto tenant_id = GetTenant(inbound_id.stream_id);
      auto data = std::make_unique<MessageDeliverData>(
          tenant_id, std::move(namespace_id), std::move(topic),
          inbound_id.GetSubID(), msg_id, payload);
      data->SetSequenceNumbers(sub->prev_seqno, seqno);
      sub->prev_seqno = seqno;
      SendResponse(flow, inbound_id.stream_id, std::move(data));
    } else {
      stats_->dropped_reordered->Add(1);
      LOG_WARN(server_->options_.info_log,
               "Attempted to deliver data at %" PRIu64
               ", but subscription has previous seqno %" PRIu64,
               seqno,
               sub->prev_seqno);
    }
  }
}

void CommunicationRocketeer::DeliverBatch(
    Flow* flow, StreamID stream_id, std::vector<RocketeerMessage> messages) {
  thread_check_.Check();

  MessageDeliverBatch::MessagesVector messages_vec;
  messages_vec.reserve(messages.size());
  auto tenant_id = GetTenant(stream_id);
  for (auto& msg : messages) {
    if (msg.msg_id.Empty()) {
      msg.msg_id = GUIDGenerator::ThreadLocalGUIDGenerator()->Generate();
    }
    if (auto* sub = Find(InboundID(stream_id, msg.GetSubID()))) {
      if (sub->prev_seqno < msg.seqno) {
        messages_vec.emplace_back(
            new MessageDeliverData(tenant_id,
                                   std::move(msg.namespace_id),
                                   std::move(msg.topic),
                                   msg.GetSubID(),
                                   msg.msg_id,
                                   std::move(msg.payload)));
        messages_vec.back()->SetSequenceNumbers(sub->prev_seqno, msg.seqno);
        sub->prev_seqno = msg.seqno;
      } else {
        stats_->dropped_reordered->Add(1);
        LOG_WARN(server_->options_.info_log,
                 "Attempted to deliver data at %" PRIu64
                 ", but subscription has previous seqno %" PRIu64,
                 msg.seqno,
                 sub->prev_seqno);
      }
    }
  }
  if (!messages_vec.empty()) {
    auto batch = std::make_unique<MessageDeliverBatch>(
        tenant_id, std::move(messages_vec));
    SendResponse(flow, stream_id, std::move(batch));
  }
}

void CommunicationRocketeer::SendGapMessage(Flow* flow,
                                            InboundID inbound_id,
                                            NamespaceID namespace_id,
                                            Topic topic,
                                            SequenceNumber seqno,
                                            GapType gap_type) {
  thread_check_.Check();

  if (auto* sub = Find(inbound_id)) {
    if (sub->prev_seqno < seqno) {
      auto tenant_id = GetTenant(inbound_id.stream_id);
      auto gap = std::make_unique<MessageDeliverGap>(
          tenant_id, std::move(namespace_id), std::move(topic),
          inbound_id.GetSubID(), gap_type);
      gap->SetSequenceNumbers(sub->prev_seqno, seqno);
      sub->prev_seqno = seqno;
      SendResponse(flow, inbound_id.stream_id, std::move(gap));
    } else {
      stats_->dropped_reordered->Add(1);
      LOG_WARN(server_->options_.info_log,
               "Attempted to deliver gap at %" PRIu64
               ", but subscription has previous seqno %" PRIu64,
               seqno,
               sub->prev_seqno);
    }
  }
}

void CommunicationRocketeer::Advance(Flow* flow,
                                     InboundID inbound_id,
                                     NamespaceID namespace_id,
                                     Topic topic,
                                     SequenceNumber seqno) {
  SendGapMessage(flow, inbound_id, std::move(namespace_id), std::move(topic),
      seqno, GapType::kBenign);
}

void CommunicationRocketeer::NotifyDataLoss(Flow* flow,
                                            InboundID inbound_id,
                                            NamespaceID namespace_id,
                                            Topic topic,
                                            SequenceNumber seqno) {
  SendGapMessage(flow, inbound_id, std::move(namespace_id), std::move(topic),
      seqno, GapType::kDataLoss);
}

void CommunicationRocketeer::Unsubscribe(Flow* flow,
                                         InboundID inbound_id,
                                         NamespaceID namespace_id,
                                         Topic topic,
                                         UnsubscribeReason reason) {
  thread_check_.Check();

  StreamID origin = inbound_id.stream_id;
  SubscriptionID sub_id = inbound_id.GetSubID();
  auto it = stream_state_.find(origin);
  if (it != stream_state_.end()) {
    StreamState& state = it->second;
    auto it1 = state.inbound.find(sub_id);
    if (it1 != state.inbound.end()) {
      state.inbound.erase(it1);
      stats_->inbound_subscriptions->Add(-1);
      stats_->terminations->Add(1);
      state.num_subscriptions--;
      HandleTermination(flow,
                        InboundID(origin, sub_id),
                        TerminationSource::Rocketeer);

      MessageUnsubscribe::Reason msg_reason =
          MessageUnsubscribe::Reason::kInvalid;
      switch (reason) {
        case UnsubscribeReason::Requested:
          msg_reason = MessageUnsubscribe::Reason::kRequested;
          break;
        case UnsubscribeReason::Invalid:
          msg_reason = MessageUnsubscribe::Reason::kInvalid;
          break;
      }
      auto unsubscribe = std::make_unique<MessageUnsubscribe>(
          state.tenant_id, std::move(namespace_id), std::move(topic),
          inbound_id.GetSubID(), msg_reason);
      SendResponse(flow, inbound_id.stream_id, std::move(unsubscribe));
      return;
    }
  }
  LOG_DEBUG(server_->options_.info_log,
            "Missing subscription on stream: %llu, sub_id: %llu. "
            "Likely a race with termination on this subscription.",
            origin,
            sub_id.ForLogging());
}

void CommunicationRocketeer::HasMessageSinceResponse(
      Flow* flow, InboundID inbound_id, NamespaceID namespace_id, Topic topic,
      Epoch epoch, SequenceNumber seqno, HasMessageSinceResult response,
      std::string info) {
  thread_check_.Check();

  if (auto* sub = Find(inbound_id)) {
    auto tenant_id = GetTenant(inbound_id.stream_id);
    auto message = std::make_unique<MessageBacklogFill>(
        tenant_id, std::move(namespace_id), std::move(topic), std::move(epoch),
        seqno, sub->prev_seqno, response, std::move(info));
    SendResponse(flow, inbound_id.stream_id, std::move(message));
  }
}

size_t CommunicationRocketeer::GetID() const {
  auto worker_id = server_->msg_loop_->GetThreadWorkerIndex();
  RS_ASSERT(static_cast<size_t>(worker_id) == id_);
  ((void)worker_id);
  return id_;
}

void CommunicationRocketeer::Initialize(RocketeerServer* server, size_t id) {
  RS_ASSERT(!server_);
  server_ = server;
  id_ = id;
  stats_.reset(new Stats(server->options_.stats_prefix));
}

const Statistics& CommunicationRocketeer::GetStatisticsInternal() {
  RS_ASSERT(stats_);
  return stats_->all;
}

TenantID CommunicationRocketeer::GetTenant(StreamID stream_id) const {
  auto it = stream_state_.find(stream_id);
  if (it != stream_state_.end()) {
    return it->second.tenant_id;
  }
  LOG_ERROR(server_->options_.info_log,
            "Stream(%llu) does not have a tenant ID yet.", stream_id);
  return Tenant::InvalidTenant;
}

InboundSubscription* CommunicationRocketeer::Find(const InboundID& inbound_id) {
  auto it = stream_state_.find(inbound_id.stream_id);
  if (it != stream_state_.end()) {
    StreamState& state = it->second;
    auto it1 = state.inbound.find(inbound_id.GetSubID());
    if (it1 != state.inbound.end()) {
      return &it1->second;
    }
  }
  LOG_DEBUG(server_->options_.info_log,
            "Missing subscription on stream (%llu) with ID (%llu). "
            "Likely a race with termination on this subscription.",
            inbound_id.stream_id,
            inbound_id.GetSubID().ForLogging());
  return nullptr;
}

void CommunicationRocketeer::SendResponse(Flow* flow,
                                          StreamID stream_id,
                                          std::unique_ptr<Message> message) {
  auto loop = server_->msg_loop_->GetEventLoop((int)GetID());
  if (auto stream = loop->GetDeliverySink(stream_id)) {
    if (flow) {
      flow->Write(stream, message);
    } else {
      SourcelessFlow no_flow(loop->GetFlowControl());
      no_flow.Write(stream, message);
    }
  } else {
    LOG_WARN(server_->options_.info_log,
             "Stream: %llu not found, dropping message",
             stream_id);
  }
}

void CommunicationRocketeer::Receive(
    Flow* flow, std::unique_ptr<MessageSubscribe> subscribe, StreamID origin) {
  thread_check_.Check();

  // For now, we determine a stream's tenant by the first subscribe we see.
  // In the future, we should require streams to introduce themselves first to
  // set the tenant, among other things.
  auto it = stream_state_.find(origin);
  if (it == stream_state_.end()) {
    it = stream_state_.emplace(
        origin, StreamState(subscribe->GetTenantID())).first;
  }

  StreamState& state = it->second;
  SubscriptionID sub_id = subscribe->GetSubID();
  SequenceNumber start_seqno = subscribe->GetStartSequenceNumber();
  auto result = state.inbound.emplace(
      sub_id,
      InboundSubscription(start_seqno == 0 ? start_seqno : start_seqno - 1));
  if (!result.second) {
    LOG_WARN(server_->options_.info_log,
             "Duplicated subscription stream: %llu, sub_id: %llu",
             origin,
             sub_id.ForLogging());
    return;
  }
  // TODO(stupaq) store subscription parameters in a message and move them out
  SubscriptionParameters params(subscribe->GetTenantID(),
                                subscribe->GetNamespace().ToString(),
                                subscribe->GetTopicName().ToString(),
                                subscribe->GetStartSequenceNumber());
  HandleNewSubscription(flow, InboundID(origin, sub_id), std::move(params));
  stats_->subscribes->Add(1);
  stats_->inbound_subscriptions->Add(1);
  state.num_subscriptions++;
}

void CommunicationRocketeer::Receive(
    Flow* flow, std::unique_ptr<MessageUnsubscribe> unsubscribe,
    StreamID origin) {
  thread_check_.Check();

  SubscriptionID sub_id = unsubscribe->GetSubID();
  auto it = stream_state_.find(origin);
  if (it != stream_state_.end()) {
    StreamState& state = it->second;
    auto removed = state.inbound.erase(sub_id);
    if (removed > 0) {
      stats_->inbound_subscriptions->Add(-1);
      stats_->unsubscribes->Add(1);
      state.num_subscriptions--;
      HandleTermination(flow,
                        InboundID(origin, sub_id),
                        TerminationSource::Subscriber);
    } else {
      LOG_WARN(server_->options_.info_log,
         "Missing subscription on stream: %llu, sub_id: %llu",
         origin,
         sub_id.ForLogging());
    }
  } else {
    LOG_WARN(server_->options_.info_log,
        "Received Unsubscribe before a Subscribe on stream %llu", origin);
  }
}

void CommunicationRocketeer::Receive(
      Flow* flow, std::unique_ptr<MessageBacklogQuery> query,
      StreamID origin) {
  thread_check_.Check();
  SubscriptionID sub_id = query->GetSubID();
  HandleHasMessageSince(flow, InboundID(origin, sub_id), query->GetNamespace(),
      query->GetTopicName(), query->GetEpoch(), query->GetSequenceNumber());
}

void CommunicationRocketeer::Receive(
    Flow* flow, std::unique_ptr<MessageGoodbye> goodbye, StreamID origin) {
  thread_check_.Check();

  auto it = stream_state_.find(origin);
  if (it == stream_state_.end()) {
    LOG_WARN(server_->options_.info_log, "Missing stream: %llu", origin);
    return;
  }
  StreamState& state = it->second;
  if (server_->options_.terminate_on_disconnect) {
    for (const auto& entry : state.inbound) {
      HandleTermination(flow,
                        InboundID(origin, entry.first),
                        TerminationSource::Subscriber);
    }
  }
  const int64_t subs = static_cast<int64_t>(state.num_subscriptions);
  stats_->inbound_subscriptions->Add(-subs);
  stats_->unsubscribes->Add(subs);

  stream_state_.erase(it);
  HandleDisconnect(flow, origin);
}

////////////////////////////////////////////////////////////////////////////////
CommunicationRocketeer::Stats::Stats(const std::string& prefix) {
  subscribes = all.AddCounter(prefix + "subscribes");
  unsubscribes = all.AddCounter(prefix + "unsubscribes");
  terminations = all.AddCounter(prefix + "terminations");
  inbound_subscriptions = all.AddCounter(prefix + "inbound_subscriptions");
  dropped_reordered = all.AddCounter(prefix + "dropped_reordered");
}

////////////////////////////////////////////////////////////////////////////////
RocketeerServer::RocketeerServer(RocketeerOptions options)
: options_(std::move(options)) {}

RocketeerServer::~RocketeerServer() {
  // Stop threads before any Rocketeer is destroyed.
  Stop();
}

size_t RocketeerServer::Register(Rocketeer* rocketeer) {
  RS_ASSERT(!msg_loop_);
  RS_ASSERT(rocketeer);
  auto id = rocketeers_.size();
  std::unique_ptr<CommunicationRocketeer> com_rocketeer;
  com_rocketeer.reset(new CommunicationRocketeer(rocketeer));
  com_rocketeer->Initialize(this, id);
  rocketeers_.push_back(std::move(com_rocketeer));
  return id;
}

Status RocketeerServer::Start() {
  MsgLoop::Options msg_loop_options;
  auto& eopts = msg_loop_options.event_loop;
  eopts.heartbeat_period = options_.heartbeat_period;
  eopts.heartbeat_timeout = std::chrono::milliseconds(0);  // not a client
  eopts.command_queue_size = options_.queue_size;
  eopts.socket_timeout = options_.socket_timeout;
  eopts.use_heartbeat_deltas = options_.use_heartbeat_deltas;
  eopts.enable_throttling = options_.enable_throttling;
  eopts.enable_batching = options_.enable_batching;
  eopts.throttler_policy =
      DeliveryThrottler::Policy(options_.rate_limit, options_.rate_duration);
  eopts.batcher_policy = DeliveryBatcher::Policy(
      options_.batch_max_limit, options_.batch_max_duration);

  msg_loop_.reset(new MsgLoop(options_.env,
                              EnvOptions(),
                              options_.port,
                              static_cast<int>(rocketeers_.size()),
                              options_.info_log,
                              "rocketeer",
                              msg_loop_options));

  Status st = msg_loop_->Initialize();
  if (!st.ok()) {
    return st;
  }

  msg_loop_->RegisterCallbacks({
      {MessageType::mSubscribe, CreateCallback<MessageSubscribe>()},
      {MessageType::mUnsubscribe, CreateCallback<MessageUnsubscribe>()},
      {MessageType::mBacklogQuery, CreateCallback<MessageBacklogQuery>()},
      {MessageType::mGoodbye, CreateCallback<MessageGoodbye>()},
  });

  msg_loop_thread_.reset(
      new MsgLoopThread(options_.env, msg_loop_.get(), "rocketeer"));

  return Status::OK();
}

void RocketeerServer::Stop() {
  msg_loop_thread_.reset();
}

bool RocketeerServer::Deliver(InboundID inbound_id,
                              NamespaceID namespace_id,
                              Topic topic,
                              SequenceNumber seqno,
                              std::string payload,
                              MsgId msg_id) {
  auto worker_id = GetWorkerID(inbound_id);
  auto command = [this, worker_id, inbound_id,
                  namespace_id = std::move(namespace_id),
                  topic = std::move(topic), seqno,
                  payload = std::move(payload), msg_id](Flow* flow) mutable {
    rocketeers_[worker_id]->Deliver(
        flow, inbound_id, std::move(namespace_id),
        std::move(topic), seqno, std::move(payload), msg_id);
  };
  return msg_loop_
      ->SendCommand(MakeExecuteWithFlowCommand(std::move(command)), worker_id)
      .ok();
}

bool RocketeerServer::DeliverBatch(StreamID stream_id,
                                   int worker_id,
                                   std::vector<RocketeerMessage> messages) {
  auto command =
      [this, stream_id, worker_id, messages = std::move(messages)]
      (Flow* flow) mutable {
        rocketeers_[worker_id]->DeliverBatch(
            flow, stream_id, std::move(messages));
      };
  return msg_loop_
      ->SendCommand(MakeExecuteWithFlowCommand(std::move(command)), worker_id)
      .ok();
}

bool RocketeerServer::Advance(InboundID inbound_id, NamespaceID namespace_id,
    Topic topic, SequenceNumber seqno) {
  auto worker_id = GetWorkerID(inbound_id);
  auto command = [this, worker_id, inbound_id,
                  namespace_id = std::move(namespace_id),
                  topic = std::move(topic), seqno](Flow* flow) mutable {
    rocketeers_[worker_id]->Advance(flow, inbound_id, std::move(namespace_id),
        std::move(topic), seqno);
  };
  return msg_loop_
      ->SendCommand(MakeExecuteWithFlowCommand(std::move(command)), worker_id)
      .ok();
}

bool RocketeerServer::NotifyDataLoss(InboundID inbound_id,
    NamespaceID namespace_id, Topic topic, SequenceNumber seqno) {
  auto worker_id = GetWorkerID(inbound_id);
  auto command = [this, worker_id, inbound_id,
                  namespace_id = std::move(namespace_id),
                  topic = std::move(topic), seqno](Flow* flow) mutable {
    rocketeers_[worker_id]->NotifyDataLoss(flow, inbound_id,
        std::move(namespace_id), std::move(topic), seqno);
  };
  return msg_loop_
      ->SendCommand(MakeExecuteWithFlowCommand(std::move(command)), worker_id)
      .ok();
}

bool RocketeerServer::Unsubscribe(InboundID inbound_id,
                                  NamespaceID namespace_id,
                                  Topic topic,
                                  Rocketeer::UnsubscribeReason reason) {
  auto worker_id = GetWorkerID(inbound_id);
  auto command =
    [this, worker_id, inbound_id, namespace_id = std::move(namespace_id),
     topic = std::move(topic), reason](Flow* flow) mutable {
    rocketeers_[worker_id]->Unsubscribe(
        flow, inbound_id, std::move(namespace_id), std::move(topic), reason);
  };
  return msg_loop_
      ->SendCommand(MakeExecuteWithFlowCommand(std::move(command)), worker_id)
      .ok();
}

bool RocketeerServer::HasMessageSinceResponse(
    InboundID inbound_id, NamespaceID namespace_id, Topic topic, Epoch epoch,
    SequenceNumber seqno, HasMessageSinceResult response, std::string info) {
  auto worker_id = GetWorkerID(inbound_id);
  auto command = [this, worker_id, inbound_id,
                  namespace_id = std::move(namespace_id),
                  topic = std::move(topic), epoch = std::move(epoch), seqno,
                  response, info = std::move(info)](Flow* flow) mutable {
    rocketeers_[worker_id]->HasMessageSinceResponse(flow, inbound_id,
        std::move(namespace_id), std::move(topic), std::move(epoch), seqno,
        response, std::move(info));
  };
  return msg_loop_
      ->SendCommand(MakeExecuteWithFlowCommand(std::move(command)), worker_id)
      .ok();
}

Statistics RocketeerServer::GetStatisticsSync() const {
  auto stats = msg_loop_->AggregateStatsSync(
      [this](int i) { return rocketeers_[i]->GetStatisticsInternal(); });
  stats.Aggregate(msg_loop_->GetStatisticsSync());
  return stats;
}

void RocketeerServer::ExportStatistics(StatisticsVisitor* visitor) const {
  GetStatisticsSync().Export(visitor);
}

int RocketeerServer::GetWorkerID(const InboundID& inbound_id) const {
  auto worker_id = msg_loop_->GetStreamMapping()(inbound_id.stream_id);
  return static_cast<int>(worker_id);
}

template <typename Msg>
MsgCallbackType RocketeerServer::CreateCallback() {
  return [this](Flow* flow, std::unique_ptr<Message> message, StreamID origin) {
    std::unique_ptr<Msg> casted(static_cast<Msg*>(message.release()));
    auto worker_id = msg_loop_->GetThreadWorkerIndex();
    rocketeers_[worker_id]->Receive(flow, std::move(casted), origin);
  };
}

const HostId& RocketeerServer::GetHostId() const {
  return msg_loop_->GetHostId();
}

}  // namespace rocketspeed
