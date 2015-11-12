//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <deque>
#include <string>

#include "src/util/common/flow_control.h"
#include "src/util/common/host_id.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class EventLoop;
typedef long long unsigned int StreamID;

/**
 * Maximum number of iovecs to write at once. Note that an array of iovec will
 * be allocated on the stack with this length, so it should not be too high.
 */
static constexpr size_t kMaxIovecs = 256;

/** Size (in octets) of an encoded message header. */
static constexpr size_t kMessageHeaderEncodedSize =
    sizeof(uint8_t) + sizeof(uint32_t);

class SocketEvent : public Source<MessageOnStream> {
 public:
  /**
   * Creates a new SocketEvent for provided physical socket.
   *
   * @param event_loop An event loop to register the socket with.
   * @param fd The physical socket.
   * @param destination An optional destination, if present indicates that this
   *                    is an outbound socket.
   */
  static std::unique_ptr<SocketEvent> Create(EventLoop* event_loop,
                                             int fd,
                                             HostId destination = HostId());

  /**
   * Does everything necessary to close a socket connection:
   * - removes streams,
   * - cleans up connection cache,
   * - dispatches goodbyes,
   * - frees the SocketEvent.
   *
   * @param sev The SocketEvent to disconnect.
   * @param timed_out True is the socket is descroyed due to connect timeout.
   */
  static void Disconnect(SocketEvent* sev, bool timed_out);

  ~SocketEvent();

  /** Inherited from Source<MessageOnStream>. */
  void RegisterReadEvent(EventLoop* event_loop) final override {
    assert(event_loop_ == event_loop);
    // We create the event while constructing the socket, as we cannot propagate
    // or handle errors in this method.
  }

  /** Inherited from Source<MessageOnStream>. */
  void SetReadEnabled(EventLoop* event_loop, bool enabled) final override {
    assert(event_loop_ == event_loop);
    if (enabled) {
      read_ev_->Enable();
    } else {
      read_ev_->Disable();
    }
  }

  /** Enqueues the message to be sent out when the socket becomes writable. */
  Status Enqueue(StreamID local,
                 std::shared_ptr<TimestampedString> message_ser);

  const HostId& GetDestination() const { return destination_; }

  std::list<std::unique_ptr<SocketEvent>>::iterator GetListHandle() const {
    return list_handle_;
  }

  void SetListHandle(std::list<std::unique_ptr<SocketEvent>>::iterator it) {
    list_handle_ = it;
  }

 private:
  ThreadCheck thread_check_;

  /** Handles write availability events from EventLoop. */
  Status WriteCallback();

  /** Handles read availability events from EventLoop. */
  Status ReadCallback();

  /** Reader and deserializer state. */
  size_t hdr_idx_;
  char hdr_buf_[kMessageHeaderEncodedSize];
  size_t msg_idx_;
  size_t msg_size_;
  std::unique_ptr<char[]> msg_buf_;  // receive buffer

  /** Writer and serializer state. */
  /** A list of chunks of data to be written. */
  std::deque<std::shared_ptr<TimestampedString>> send_queue_;
  /** The next valid offset in the earliest chunk of data to be written. */
  Slice partial_;

  /** The physical socket. */
  int fd_;

  std::unique_ptr<EventCallback> read_ev_;
  std::unique_ptr<EventCallback> write_ev_;

  EventLoop* event_loop_;

  bool write_ev_added_;     // is the write event added?
  bool was_initiated_;      // was this connection initiated by us?
  bool timeout_cancelled_;  // have we removed from EventLoop connect_timeout_?

  /** A remote destination, non-empty for outbound connections only. */
  HostId destination_;

  // Handle into the EventLoop's socket event list (for fast removal).
  std::list<std::unique_ptr<SocketEvent>>::iterator list_handle_;

  SocketEvent(EventLoop* event_loop, int fd, bool initiated);
};

}  // namespace rocketspeed
