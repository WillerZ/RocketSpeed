//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include "src/port/port.h"
#include "src/messages/types.h"
#include "src/messages/flow_control.h"
#include "include/HostId.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class EventCallback;
class Flow;
class Message;
class SocketEvent;
class Stream;

namespace access {
/**
 * This grants access to a certain subset of methods of Stream.
 * We don't want to do it for every class, but only for those that constitute
 * the public API. Friends stink, we have a tradition of abusing them.
 */
class Stream {
 private:
  friend class rocketspeed::SocketEvent;
  friend class rocketspeed::Stream;
  Stream() = default;
};
}  // namespace access

class Stream : public Sink<SharedTimestampedString> {
 public:
  Stream(SocketEvent* socket_event, StreamID remote_id, StreamID local_id_);

  /**
   * Sets an object responsible for receiving messages on the stream.
   * The receiver must outlive this stream object.
   *
   * @param receiver A pointer to receiver object or null, if received messages
   *                 shall be dropped.
   */
  void SetReceiver(StreamReceiver* receiver) { receiver_ = receiver; }

  /** Closes the stream gracefully. */
  ~Stream();

  /** Inherited from Sink<SharedTimestampedString>. */
  bool Write(SharedTimestampedString& value) final override;

  static SharedTimestampedString ToTimestampedString(const Message& value);
  static SharedTimestampedString ToTimestampedString(const std::string& value);

  bool Write(const Message& msg) {
    auto ts = ToTimestampedString(msg);
    return Write(ts);
  }

  bool Write(const std::string& s) {
    auto ts = ToTimestampedString(s);
    return Write(ts);
  }

  /** Inherited from Sink<Message>. */
  bool FlushPending() final override;

  /** Inherited from Sink<Message>. */
  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) final override;

  /** Closes a stream without sending or delivering a MessageGoodbye. */
  void CloseFromSocketEvent(access::Stream);

  /**
   * Receives a message from the socket and invokes appropriate callback.
   * Closes the stream if MessageGoodbye was received.
   *
   * @param flow The flow this message belongs to.
   * @param message The message.
   */
  void Receive(access::Stream, Flow* flow, std::unique_ptr<Message> message);

  StreamID GetLocalID() const { return local_id_; }

  StreamID GetRemoteID() const { return remote_id_; }

  std::string GetSinkName() const override;

 private:
  ThreadCheck thread_check_;

  /** A socket this stream operates on. */
  SocketEvent* socket_event_;
  /**
   * A _remote_ ID of this stream.
   * In case of an outbound stream this is an ID assigned by the EventLoop.
   * In case of an inbound stream this is an ID that we've read from the wire.
   */
  const StreamID remote_id_;
  /**
   * A _local_ ID of this stream, unique within the EventLoop (at least).
   * Equals remote_id_ for outbound streams.
   */
  const StreamID local_id_;
  /** An object to accept received messages. */
  StreamReceiver* receiver_;
};

}  // namespace rocketspeed
