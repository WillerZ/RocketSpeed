//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <string>

#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"

namespace rocketspeed {

class Flow;
class Message;
class MessagePing;
class MessageData;
class MessageDataAck;
class MessageGoodbye;
class MessageSubscribe;
class MessageUnsubscribe;
class MessageDeliver;
class MessageDeliverGap;
class MessageDeliverData;
class MessageFindTailSeqno;
class MessageTailSeqno;
class MessageDeliverBatch;
template<typename>
class Sink;
class Slice;

/**
 * Identifies a stream, which is a pair of unidirectional channels, one in each
 * direction. Messages flowing in one direction within given stream are linearly
 * ordered. Two messages flowing in opposite directions have no ordering
 * guarantees.
 * The ID uniquely identifies a stream within a single physical connection only,
 * that means if streams are multiplexed on the same connection and have the
 * same IDs, the IDs need to be remapped. The IDs do not need to be unique
 * system-wide.
 */
typedef unsigned long long int StreamID;
static_assert(sizeof(StreamID) == 8, "Invalid StreamID size.");

/**
 * Encodes stream ID onto wire.
 *
 * @param out Output string to append encoded origin to.
 * @param origin Origin stream ID.
 */
void EncodeOrigin(std::string* out, StreamID origin);

/**
 * Decodes wire format of stream origin.
 *
 * @param in Input slice of encoded stream spec. Will be advanced beyond spec.
 * @param origin Output parameter for decoded stream.
 * @return true iff successfully decoded.
 */
bool DecodeOrigin(Slice* in, StreamID* origin);

struct SerializedOnStream {
  StreamID stream_id;
  std::string serialised;
};


class ConnectionObserver {
 public:
  virtual ~ConnectionObserver() = default;

  void ConnectionDropped();
  void ConnectionCreated(std::unique_ptr<Sink<std::unique_ptr<Message>>> sink);

 protected:
  virtual void ConnectionChanged() = 0;

  Sink<std::unique_ptr<Message>>* GetConnection() { return sink_.get(); }

 private:
  std::shared_ptr<Sink<std::unique_ptr<Message>>> sink_;
};

/** An object which receives messages from a Stream. */
template <typename T>
struct StreamReceiveArg {
  Flow* flow;
  StreamID stream_id;
  std::unique_ptr<T> message;
};
class StreamReceiver : public NonMovable, public NonCopyable {
 public:
  virtual ~StreamReceiver() = default;

  virtual void operator()(StreamReceiveArg<Message> arg);

  /**
   * Invoked after the stream is definitively closed and the receiver
   * will not receive any more messages on it.
   */
  virtual void EndStream(StreamID stream_id) {}

  /**
   * Invoked when a stream is made aware that it is either healthy or
   * unhealthy. This represents a transient state change.
   */
  virtual void NotifyHealthy(bool isHealthy) {}

 protected:
  virtual void ReceivePing(StreamReceiveArg<MessagePing>) {}
  virtual void ReceiveData(StreamReceiveArg<MessageData>) {}
  virtual void ReceiveDataAck(StreamReceiveArg<MessageDataAck>) {}
  virtual void ReceiveGoodbye(StreamReceiveArg<MessageGoodbye>) {}
  virtual void ReceiveSubscribe(StreamReceiveArg<MessageSubscribe>) {}
  virtual void ReceiveUnsubscribe(StreamReceiveArg<MessageUnsubscribe>) {}
  virtual void ReceiveDeliver(StreamReceiveArg<MessageDeliver>) {}
  virtual void ReceiveDeliverData(StreamReceiveArg<MessageDeliverData>);
  virtual void ReceiveDeliverGap(StreamReceiveArg<MessageDeliverGap>);
  virtual void ReceiveFindTailSeqno(StreamReceiveArg<MessageFindTailSeqno>) {}
  virtual void ReceiveTailSeqno(StreamReceiveArg<MessageTailSeqno>) {}
  virtual void ReceiveDeliverBatch(StreamReceiveArg<MessageDeliverBatch>) {}

 private:
  template <typename T, typename M>
  static StreamReceiveArg<T> PrepareArguments(Flow* flow,
                                              StreamID stream_id,
                                              std::unique_ptr<M>& message);
};

class ConnectionAwareReceiver : public StreamReceiver,
                                public ConnectionObserver {};

}  // namespace rocketspeed
