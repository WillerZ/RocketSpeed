// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>

#include "include/Types.h"
#include "src/messages/stream_allocator.h"

namespace rocketspeed {

/** Keeps state of the stream as seen by its creator. */
class StreamSocket {
 public:
  /**
   * A unary predicate which returns true iff destination of its argument equals
   * given host. Note that it does not copy or take ownership of provided host,
   * so caller must ensure that the referent has longer lifetime than returned
   * predicate.
   */
  struct Equals {
    const ClientID& host;
    explicit Equals(const ClientID& _host) : host(_host) {
    }
    bool operator()(const StreamSocket& socket) const {
      return socket.GetDestination() == host;
    }
  };

  /** Creates socket which doesn't point to any stream. */
  StreamSocket() : is_open_(false) {
  }

  /**
   * Creates a socket representing a stream.
   * @param destination The destination client ID.
   * @param stream_id ID of the stream.
   */
  // TODO(stupaq) remove once everyone can allocate stream IDs
  StreamSocket(ClientID destination, StreamID stream_id)
      : destination_(std::move(destination))
      , stream_id_(stream_id)
      , is_open_(false) {
  }

  /**
   * Creates a socket representing a stream.
   * @param destination The destination client ID.
   * @param alloc Allocator, which will provide stream ID.
   */
  StreamSocket(ClientID destination, StreamAllocator* alloc)
      : destination_(std::move(destination))
      , stream_id_(alloc->Next())
      , is_open_(false) {
  }

  // Noncopyable
  StreamSocket(const StreamSocket&) = delete;
  StreamSocket& operator=(const StreamSocket&) = delete;
  // Movable
  StreamSocket(StreamSocket&&) = default;
  StreamSocket& operator=(StreamSocket&&) = default;

  bool IsValid() const {
    return !stream_id_.empty();
  }

  const ClientID& GetDestination() const {
    assert(IsValid());
    return destination_;
  }

  void Open() {
    assert(IsValid());
    is_open_ = true;
  }

  bool IsOpen() const {
    assert(IsValid());
    return is_open_;
  }

  StreamID GetStreamID() const {
    assert(IsValid());
    return stream_id_;
  }

 private:
  ClientID destination_;
  StreamID stream_id_;
  bool is_open_;
};

}  // namespace rocketspeed
