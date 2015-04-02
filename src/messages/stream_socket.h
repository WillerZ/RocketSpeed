// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>

#include "include/Types.h"

namespace rocketspeed {

class EventLoop;
// TODO(stupaq) until proxy is unbroken
class HostSessionMatrix;

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
typedef std::string StreamID;

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
  /** For constructor from the outside world. */
  friend class EventLoop;
  // TODO(stupaq) until proxy is unbroken
  friend class HostSessionMatrix;

  /**
   * Creates a socket representing a stream.
   * @param destination The destination client ID.
   * @param stream_id ID of the stream.
   */
  StreamSocket(ClientID destination, StreamID stream_id)
      : destination_(std::move(destination))
      , stream_id_(stream_id)
      , is_open_(false) {
  }

  ClientID destination_;
  StreamID stream_id_;
  bool is_open_;
};

}  // namespace rocketspeed
