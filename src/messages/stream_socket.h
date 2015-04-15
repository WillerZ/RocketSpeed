// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Status.h"
#include "include/Types.h"

namespace rocketspeed {

class EventLoop;
class MQTTMsgLoop;
class Proxy;
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
 * @param origin Origin stream ID.
 * @return Encoded origin.
 */
std::string EncodeOrigin(StreamID origin);

/**
 * Decodes wire format of stream origin.
 *
 * @param in Input slice of encoded stream spec. Will be advanced beyond spec.
 * @param origin Output parameter for decoded stream.
 * @return ok() if successfully decoded, otherwise error.
 */
Status DecodeOrigin(Slice* in, StreamID* origin);

/** Keeps state of the stream as seen by its creator. */
class StreamSocket {
 public:
  /** Creates socket which doesn't point to any stream. */
  StreamSocket() : is_open_(false) {
#ifndef NDEBUG
    is_valid_ = false;
#endif  // NDEBUG
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
#ifndef NDEBUG
    is_valid_ = other.is_valid_;
    other.is_valid_ = false;
#endif  // NDEBUG
    return *this;
  }

  const ClientID& GetDestination() const {
    assert(is_valid_);
    assert(!destination_.empty());
    return destination_;
  }

  void Open() {
    assert(is_valid_);
    is_open_ = true;
  }

  bool IsOpen() const {
    assert(is_valid_);
    return is_open_;
  }

  StreamID GetStreamID() const {
    assert(is_valid_);
    return stream_id_;
  }

 private:
  /** For constructor from the outside world. */
  friend class EventLoop;
  friend class MQTTMsgLoop;
  friend class Proxy;

  /**
   * Creates a closed socket representing a stream.
   * @param destination The destination client ID.
   * @param stream_id ID of the stream.
   */
  StreamSocket(ClientID destination, StreamID stream_id)
      : destination_(std::move(destination))
      , stream_id_(stream_id)
      , is_open_(false) {
#ifndef NDEBUG
    is_valid_ = true;
#endif  // NDEBUG
  }

  /**
   * Creates an open socket representing a stream.
   * @param stream_id ID of the stream.
   */
  explicit StreamSocket(StreamID stream_id)
      : stream_id_(stream_id), is_open_(true) {
#ifndef NDEBUG
    is_valid_ = true;
#endif  // NDEBUG
  }

  ClientID destination_;
  StreamID stream_id_;
  bool is_open_;
#ifndef NDEBUG
  bool is_valid_;
#endif  // NDEBUG
};

}  // namespace rocketspeed
