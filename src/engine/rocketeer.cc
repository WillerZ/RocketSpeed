// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "include/Rocketeer.h"

#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "include/Env.h"
#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/flow_control.h"
#include "src/util/common/flow.h"
#include "src/util/common/retry_later_sink.h"
#include "src/util/common/subscription_id.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
SubscriptionID InboundID::GetSubID() const {
  return SubscriptionID::Unsafe(sub_id);
}

size_t InboundID::GetShard() const {
  return GetSubID().GetShardID();
}

size_t InboundID::Hash() const {
  return MurmurHash2<StreamID, uint64_t>()(stream_id, sub_id);
}

std::string InboundID::ToString() const {
  std::ostringstream ss;
  ss << "InboundID(" << stream_id << ", " << sub_id << ")";
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////
SubscriptionID RocketeerMessage::GetSubID() const {
  return SubscriptionID::Unsafe(sub_id);
}

////////////////////////////////////////////////////////////////////////////////
struct RocketeerHasMessageSinceMessage {
  NamespaceID namespace_id;
  Topic topic;
  Epoch epoch;
  SequenceNumber seqno;
};

struct RocketeerMetadataMessage {
  // Represents either a subscribe or termination.
  enum { kSubscribe, kTerminate, kHasMessageSince } type;
  InboundID inbound_id;
  SubscriptionParameters params;        // only valid for type == kSubscribe
  Rocketeer::TerminationSource source;  // only valid for type == kTerminate
  RocketeerHasMessageSinceMessage has_msg_since;  // for kHasMessageSince
};

Rocketeer::Rocketeer()
: below_rocketeer_(nullptr)
, metadata_sink_(new RetryLaterSink<RocketeerMetadataMessage>(
    [this] (RocketeerMetadataMessage& msg) mutable {
      switch (msg.type) {
        case RocketeerMetadataMessage::kSubscribe:
          // Cannot move msg.params since it may need to be retried later.
          return TryHandleNewSubscription(msg.inbound_id, msg.params);
        case RocketeerMetadataMessage::kTerminate:
          return TryHandleTermination(msg.inbound_id, msg.source);
        case RocketeerMetadataMessage::kHasMessageSince:
          return TryHandleHasMessageSince(
              msg.inbound_id,
              msg.has_msg_since.namespace_id,
              msg.has_msg_since.topic,
              msg.has_msg_since.epoch,
              msg.has_msg_since.seqno);
      }
    })) {}

Rocketeer::~Rocketeer() = default;

BackPressure Rocketeer::TryHandleNewSubscription(
    InboundID inbound_id, SubscriptionParameters params) {
  RS_ASSERT(false) << "TryHandleNewSubscription is not implemented.";
  return BackPressure::None();
}

void Rocketeer::HandleNewSubscription(Flow* flow,
                                      InboundID inbound_id,
                                      SubscriptionParameters params) {
  // This is the default implementation of HandleNewSubscription.
  // Most application Rocketeers will implement TryHandleNewSubscription, but
  // internally RocketSpeed calls HandleNewSubscription.
  //
  // The default implementation forward the call to TryHandleNewSubscription
  // through a RetryLaterSink, which will retry the call later if the Try
  // called requested a retry.
  RocketeerMetadataMessage msg;
  msg.type = RocketeerMetadataMessage::kSubscribe;
  msg.inbound_id = inbound_id;
  msg.params = std::move(params);
  flow->Write(metadata_sink_.get(), msg);
}

BackPressure Rocketeer::TryHandleTermination(
    InboundID inbound_id, TerminationSource source) {
  RS_ASSERT(false) << "TryHandleTermination is not implemented.";
  return BackPressure::None();
}

void Rocketeer::HandleTermination(
    Flow* flow, InboundID inbound_id, TerminationSource source) {
  // This is the default implementation of HandleTermination.
  // Most application Rocketeers will implement TryHandleTermination, but
  // internally RocketSpeed calls HandleTermination.
  //
  // The default implementation forward the call to TryHandleTermination
  // through a RetryLaterSink, which will retry the call later if the Try
  // called requested a retry.
  RocketeerMetadataMessage msg;
  msg.type = RocketeerMetadataMessage::kTerminate;
  msg.inbound_id = inbound_id;
  msg.source = source;
  flow->Write(metadata_sink_.get(), msg);
}

BackPressure Rocketeer::TryHandleHasMessageSince(
      InboundID inbound_id, NamespaceID namespace_id, Topic topic, Epoch epoch,
      SequenceNumber seqno) {
  RS_ASSERT(false) << "TryHandleHasMessageSince is not implemented.";
  return BackPressure::None();
}

void Rocketeer::HandleHasMessageSince(
      Flow* flow, InboundID inbound_id, NamespaceID namespace_id, Topic topic,
      Epoch epoch, SequenceNumber seqno) {
  // This is the default implementation of HandleHasMessageSince.
  // Most application Rocketeers will implement TryHandleHasMessageSince, but
  // internally RocketSpeed calls HandleHasMessageSince.
  //
  // The default implementation forward the call to TryHandleHasMessageSince
  // through a RetryLaterSink, which will retry the call later if the Try
  // called requested a retry.
  RocketeerMetadataMessage msg;
  msg.type = RocketeerMetadataMessage::kHasMessageSince;
  msg.inbound_id = inbound_id;
  msg.has_msg_since.namespace_id = std::move(namespace_id);
  msg.has_msg_since.topic = std::move(topic);
  msg.has_msg_since.epoch = std::move(epoch);
  msg.has_msg_since.seqno = seqno;
  flow->Write(metadata_sink_.get(), msg);
}

void Rocketeer::Deliver(Flow* flow,
                        InboundID inbound_id,
                        SequenceNumber seqno,
                        std::string payload,
                        MsgId msg_id) {
  GetBelowRocketeer()->Deliver(
      flow, inbound_id, seqno, std::move(payload), msg_id);
}

void Rocketeer::DeliverBatch(Flow* flow,
                             StreamID stream_id,
                             std::vector<RocketeerMessage> messages) {
  GetBelowRocketeer()->DeliverBatch(flow, stream_id, std::move(messages));
}

void Rocketeer::Advance(Flow* flow,
                        InboundID inbound_id,
                        SequenceNumber seqno) {
  GetBelowRocketeer()->Advance(flow, inbound_id, seqno);
}

void Rocketeer::Terminate(Flow* flow,
                          InboundID inbound_id,
                          UnsubscribeReason reason) {
  GetBelowRocketeer()->Terminate(flow, inbound_id, reason);
}

void Rocketeer::HasMessageSinceResponse(
      Flow* flow, InboundID inbound_id, NamespaceID namespace_id, Topic topic,
      Epoch epoch, SequenceNumber seqno, HasMessageSinceResult response) {
  GetBelowRocketeer()->HasMessageSinceResponse(flow, inbound_id,
      std::move(namespace_id), std::move(topic), std::move(epoch), seqno,
      response);
}

}  // namespace rocketspeed
