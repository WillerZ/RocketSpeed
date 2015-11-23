//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/types.h"

#include "include/Slice.h"
#include "src/messages/messages.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

void EncodeOrigin(std::string* out, const StreamID origin) {
  PutFixed64(out, static_cast<uint64_t>(origin));
}

bool DecodeOrigin(Slice* in, StreamID* origin) {
  uint64_t origin_fixed;
  if (!GetFixed64(in, &origin_fixed)) {
    return false;
  }
  *origin = static_cast<StreamID>(origin_fixed);
  return true;
}

void StreamReceiver::operator()(StreamReceiveArg<Message> arg) {
  auto flow = arg.flow;
  auto stream_id = arg.stream_id;
  auto message = std::move(arg.message);
  const auto type = message->GetMessageType();
  switch (type) {
    case MessageType::mPing:
      ReceivePing(PrepareArguments<MessagePing>(flow, stream_id, message));
      return;
    case MessageType::mPublish:
      ReceiveData(PrepareArguments<MessageData>(flow, stream_id, message));
      return;
    case MessageType::mDataAck:
      ReceiveDataAck(
          PrepareArguments<MessageDataAck>(flow, stream_id, message));
      return;
    case MessageType::mGoodbye:
      ReceiveGoodbye(
          PrepareArguments<MessageGoodbye>(flow, stream_id, message));
      return;
    case MessageType::mSubscribe:
      ReceiveSubscribe(
          PrepareArguments<MessageSubscribe>(flow, stream_id, message));
      return;
    case MessageType::mUnsubscribe:
      ReceiveUnsubscribe(
          PrepareArguments<MessageUnsubscribe>(flow, stream_id, message));
      return;
    case MessageType::mDeliverData:
      ReceiveDeliverData(
          PrepareArguments<MessageDeliverData>(flow, stream_id, message));
      return;
    case MessageType::mDeliverGap:
      ReceiveDeliverGap(
          PrepareArguments<MessageDeliverGap>(flow, stream_id, message));
      return;
    case MessageType::mFindTailSeqno:
      ReceiveFindTailSeqno(
          PrepareArguments<MessageFindTailSeqno>(flow, stream_id, message));
      return;
    case MessageType::mTailSeqno:
      ReceiveTailSeqno(
          PrepareArguments<MessageTailSeqno>(flow, stream_id, message));
      return;
    default:
      RS_ASSERT(false);
  }
}

void StreamReceiver::ReceiveDeliverData(
    StreamReceiveArg<MessageDeliverData> arg) {
  ReceiveDeliver(
      PrepareArguments<MessageDeliver>(arg.flow, arg.stream_id, arg.message));
}

void StreamReceiver::ReceiveDeliverGap(
    StreamReceiveArg<MessageDeliverGap> arg) {
  ReceiveDeliver(
      PrepareArguments<MessageDeliver>(arg.flow, arg.stream_id, arg.message));
}

template <typename T, typename M>
StreamReceiveArg<T> StreamReceiver::PrepareArguments(
    Flow* flow, StreamID stream_id, std::unique_ptr<M>& message) {
  StreamReceiveArg<T> arg;
  arg.flow = flow;
  arg.stream_id = stream_id;
  arg.message.reset(static_cast<T*>(message.release()));
  return arg;
}

}  // namespace rocketspeed
