// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "rocketeer.h"

#include <cassert>
#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "external/folly/move_wrapper.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/msg_loop.h"
#include "src/port/Env.h"
#include "src/util/common/guid_generator.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
std::string InboundID::ToString() const {
  std::ostringstream ss;
  ss << "InboundID(" << stream_id << ", " << sub_id << ")";
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////
RocketeerOptions::RocketeerOptions()
    : env(Env::Default())
    , info_log(std::make_shared<NullLogger>())
    , port(DEFAULT_PORT) {
}

////////////////////////////////////////////////////////////////////////////////
Rocketeer::Rocketeer() : server_(nullptr) {
}

bool Rocketeer::Deliver(InboundID inbound_id,
                        SequenceNumber seqno,
                        std::string payload,
                        MsgId msg_id) {
  if (msg_id.Empty()) {
    msg_id = GUIDGenerator::ThreadLocalGUIDGenerator()->Generate();
  }
  auto moved_payload = folly::makeMoveWrapper(std::move(payload));
  auto command = [this, inbound_id, seqno, moved_payload, msg_id]() {
    thread_check_.Check();
    if (auto* sub = Find(inbound_id)) {
      MessageDeliverData data(
          sub->tenant_id, inbound_id.sub_id, msg_id, *moved_payload);
      data.SetSequenceNumbers(sub->prev_seqno, seqno);
      sub->prev_seqno = seqno;
      server_->msg_loop_->SendResponse(
          data, inbound_id.stream_id, inbound_id.worker_id);
    }
  };
  return server_->msg_loop_->SendCommand(
                                 std::unique_ptr<Command>(
                                     MakeExecuteCommand(std::move(command))),
                                 inbound_id.worker_id).ok();
}

bool Rocketeer::Advance(InboundID inbound_id, SequenceNumber seqno) {
  auto command = [this, inbound_id, seqno]() {
    thread_check_.Check();
    if (auto* sub = Find(inbound_id)) {
      MessageDeliverGap gap(
          sub->tenant_id, inbound_id.sub_id, GapType::kBenign);
      gap.SetSequenceNumbers(sub->prev_seqno, seqno);
      sub->prev_seqno = seqno;
      server_->msg_loop_->SendResponse(
          gap, inbound_id.stream_id, inbound_id.worker_id);
    }
  };
  return server_->msg_loop_->SendCommand(
                                 std::unique_ptr<Command>(
                                     MakeExecuteCommand(std::move(command))),
                                 inbound_id.worker_id).ok();
}

bool Rocketeer::Terminate(InboundID inbound_id,
                          MessageUnsubscribe::Reason reason) {
  auto command = [this, inbound_id, reason]() {
    thread_check_.Check();
    if (auto* sub = Find(inbound_id)) {
      MessageUnsubscribe unsubscribe(sub->tenant_id, inbound_id.sub_id, reason);
      server_->msg_loop_->SendResponse(
          unsubscribe, inbound_id.stream_id, inbound_id.worker_id);
    }
  };
  return server_->msg_loop_->SendCommand(
                                 std::unique_ptr<Command>(
                                     MakeExecuteCommand(std::move(command))),
                                 inbound_id.worker_id).ok();
}

InboundSubscription* Rocketeer::Find(const InboundID& inbound_id) {
  auto it = inbound_subscriptions_.find(inbound_id.stream_id);
  if (it != inbound_subscriptions_.end()) {
    auto it1 = it->second.find(inbound_id.sub_id);
    if (it1 != it->second.end()) {
      return &it1->second;
    }
  }
  LOG_WARN(server_->options_.info_log,
           "Missing subscription on stream (%llu) with ID (%" PRIu64 ")",
           inbound_id.stream_id,
           inbound_id.sub_id);
  return nullptr;
}

void Rocketeer::Receive(std::unique_ptr<MessageSubscribe> subscribe,
                        StreamID origin) {
  thread_check_.Check();
  SubscriptionID sub_id = subscribe->GetSubID();
  SequenceNumber start_seqno = subscribe->GetStartSequenceNumber();
  auto result = inbound_subscriptions_[origin].emplace(
      sub_id,
      InboundSubscription(subscribe->GetTenantID(),
                          start_seqno == 0 ? start_seqno : start_seqno - 1));
  if (!result.second) {
    LOG_WARN(server_->options_.info_log,
             "Duplicated subscription stream: %llu, sub_id: %" PRIu64,
             origin,
             subscribe->GetSubID());
    return;
  }
  // TODO(stupaq) store subscription parameters in a message and move them out
  SubscriptionParameters params(subscribe->GetTenantID(),
                                subscribe->GetNamespace(),
                                subscribe->GetTopicName(),
                                subscribe->GetStartSequenceNumber());
  HandleNewSubscription(InboundID(origin, sub_id, GetID()), std::move(params));
}

void Rocketeer::Receive(std::unique_ptr<MessageUnsubscribe> unsubscribe,
                        StreamID origin) {
  thread_check_.Check();
  SubscriptionID sub_id = unsubscribe->GetSubID();
  auto it = inbound_subscriptions_.find(origin);
  if (it != inbound_subscriptions_.end()) {
    auto removed = it->second.erase(sub_id);
    if (removed > 0) {
      HandleTermination(InboundID(origin, sub_id, GetID()));
      if (it->second.empty()) {
        inbound_subscriptions_.erase(it);
      }
      return;
    }
  }
  LOG_WARN(server_->options_.info_log,
           "Missing subscription on stream: %llu, sub_id: %" PRIu64,
           origin,
           sub_id);
}

void Rocketeer::Receive(std::unique_ptr<MessageGoodbye> goodbye,
                        StreamID origin) {
  thread_check_.Check();
  auto it = inbound_subscriptions_.find(origin);
  if (it == inbound_subscriptions_.end()) {
    LOG_WARN(server_->options_.info_log, "Missing stream: %llu", origin);
    return;
  }
  auto worker_id = server_->msg_loop_->GetThreadWorkerIndex();
  for (const auto& entry : it->second) {
    HandleTermination(InboundID(origin, entry.first, worker_id));
  }
  inbound_subscriptions_.erase(it);
}

////////////////////////////////////////////////////////////////////////////////
RocketeerServer::RocketeerServer(RocketeerOptions options)
    : options_(std::move(options)) {
}

RocketeerServer::~RocketeerServer() {
  // Stop threads before any Rocketeer is destroyed.
  Stop();
}

size_t RocketeerServer::Register(Rocketeer* rocketeer) {
  assert(!msg_loop_);
  assert(rocketeer);
  assert(!rocketeer->server_);
  rocketeer->server_ = this;
  rocketeer->id_ = rocketeers_.size();
  rocketeers_.push_back(rocketeer);
  return rocketeer->id_;
}

Status RocketeerServer::Start() {
  msg_loop_.reset(new MsgLoop(options_.env,
                              EnvOptions(),
                              options_.port,
                              static_cast<int>(rocketeers_.size()),
                              options_.info_log,
                              "rocketeer"));

  Status st = msg_loop_->Initialize();
  if (!st.ok()) {
    return st;
  }

  msg_loop_->RegisterCallbacks({
      {MessageType::mSubscribe, CreateCallback<MessageSubscribe>()},
      {MessageType::mUnsubscribe, CreateCallback<MessageUnsubscribe>()},
      {MessageType::mGoodbye, CreateCallback<MessageGoodbye>()},
  });

  msg_loop_thread_.reset(
      new MsgLoopThread(options_.env, msg_loop_.get(), "rocketeer"));
  return Status::OK();
}

void RocketeerServer::Stop() {
  msg_loop_thread_.reset();
}

template <typename Msg>
std::function<void(std::unique_ptr<Message>, StreamID)>
RocketeerServer::CreateCallback() {
  return [this](std::unique_ptr<Message> message, StreamID origin) {
    std::unique_ptr<Msg> casted(static_cast<Msg*>(message.release()));
    auto worker_id = msg_loop_->GetThreadWorkerIndex();
    rocketeers_[worker_id]->Receive(std::move(casted), origin);
  };
}

}  // namespace rocketspeed
