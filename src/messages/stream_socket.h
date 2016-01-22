// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/types.h"
#include "include/HostId.h"

namespace rocketspeed {

class EventLoop;
class Proxy;
class Slice;

/** Keeps state of the stream as seen by its creator. */
class StreamSocket {
 public:
  /** Creates socket which doesn't point to any stream. */
  StreamSocket() : is_open_(false) {
#ifndef NO_RS_ASSERT
    is_valid_ = false;
#endif  // NO_RS_ASSERT
  }

  // Noncopyable
  StreamSocket(const StreamSocket&) = delete;
  StreamSocket& operator=(const StreamSocket&) = delete;

  // Movable
  StreamSocket(StreamSocket&& other) noexcept {
    *this = std::move(other);
  }
  StreamSocket& operator=(StreamSocket&& other) {
    destination_ = std::move(other.destination_);
    stream_id_ = other.stream_id_;
    is_open_ = other.is_open_;
#ifndef NO_RS_ASSERT
    is_valid_ = other.is_valid_;
    other.is_valid_ = false;
#endif  // NO_RS_ASSERT
    return *this;
  }

  const HostId& GetDestination() const {
#ifndef NO_RS_ASSERT
    RS_ASSERT(is_valid_);
#endif  // NO_RS_ASSERT
    RS_ASSERT(!!destination_);
    return destination_;
  }

  void Open() {
#ifndef NO_RS_ASSERT
    RS_ASSERT(is_valid_);
#endif  // NO_RS_ASSERT
    is_open_ = true;
  }

  bool IsOpen() const {
#ifndef NO_RS_ASSERT
    RS_ASSERT(is_valid_);
#endif  // NO_RS_ASSERT
    return is_open_;
  }

  StreamID GetStreamID() const {
#ifndef NO_RS_ASSERT
    RS_ASSERT(is_valid_);
#endif  // NO_RS_ASSERT
    return stream_id_;
  }

 private:
  /** For constructor from the outside world. */
  friend class EventLoop;
  friend class Proxy;

  /**
   * Creates a closed socket representing a stream.
   * @param destination The destination client ID.
   * @param stream_id ID of the stream.
   */
  StreamSocket(HostId destination, StreamID stream_id)
      : destination_(std::move(destination))
      , stream_id_(stream_id)
      , is_open_(false) {
#ifndef NO_RS_ASSERT
    is_valid_ = true;
#endif  // NO_RS_ASSERT
  }

  /**
   * Creates an open socket representing a stream.
   * @param stream_id ID of the stream.
   */
  explicit StreamSocket(StreamID stream_id)
      : stream_id_(stream_id), is_open_(true) {
#ifndef NO_RS_ASSERT
    is_valid_ = true;
#endif  // NO_RS_ASSERT
  }

  HostId destination_;
  StreamID stream_id_;
  bool is_open_;
#ifndef NO_RS_ASSERT
  bool is_valid_;
#endif  // NO_RS_ASSERT
};

}  // namespace rocketspeed
