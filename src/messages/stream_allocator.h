// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <limits>

#include "src/messages/stream_socket.h"

namespace rocketspeed {

class MsgLoop;
class MQTTMsgLoop;

/** An allocator for stream IDs. Wraps around if runs out of unique IDs. */
class StreamAllocator {
 public:
  // Noncopyable
  StreamAllocator(const StreamAllocator&) = delete;
  StreamAllocator& operator=(const StreamAllocator&) = delete;

  // Movable
  StreamAllocator(StreamAllocator&& other) noexcept {
    *this = std::move(other);
  }
  StreamAllocator& operator=(StreamAllocator&& other) {
    first_ = other.first_;
    end_ = other.end_;
    next_ = other.next_;
    // Invalidate other.
    other.first_ = kGlobalEnd;
    other.end_ = kGlobalFirst;
    return *this;
  }

  StreamID Next() {
    CheckValid();
    StreamID res = next_;
    next_ = IsLast() ? first_ : next_ + 1;
    return res;
  }

  bool IsLast() const {
    CheckValid();
    return next_ + 1 == end_;
  }

  bool IsEmpty() const {
    CheckValid();
    return first_ == end_;
  }

  bool IsSourceOf(StreamID stream) const {
    CheckValid();
    return first_ <= stream && stream < end_;
  }

  /**
   * Breaks allocator's set of remaining available stream IDs in two (as equal
   * as possible) pieces.
   */
  StreamAllocator Split() {
    CheckValid();
    StreamID mid = next_ + (end_ - next_) / 2;
    StreamAllocator upper(mid, end_);
    upper.Next();
    end_ = mid;
    return upper;
  }

  /**
   * Divides allocator's remaining range into (nearly) equally sized pieces.
   * This allocator remains unchanged after the operation, and it's set of
   * available stream IDs contains the union of sets of available IDs of
   * returned allocators. The converse is not necessarily true.
   */
  std::vector<StreamAllocator> Divide(size_t num_pieces) const {
    CheckValid();
    // Whatever implementation we go with, we need the reverse mapping from
    // stream ID to the index in returned vector (given the original, divided
    // allocator) to be quickly computable
    std::vector<StreamAllocator> allocs;
    allocs.reserve(num_pieces);
    const StreamID min_size = (end_ - first_) / num_pieces;
    StreamID first = first_, end;
    for (size_t piece = 0; piece < num_pieces; ++piece) {
      end = first + min_size;
      assert(first_ <= first);
      assert(first <= end);
      assert(end <= end_);
      allocs.push_back(StreamAllocator(first, end));
      first = end;
    }
    return allocs;
  }

 private:
  /** For hiding constructors from the outside world. */
  friend class MsgLoop;
  friend class MQTTMsgLoop;
  friend class CommandQueueTest;

  static const StreamID kGlobalFirst = std::numeric_limits<StreamID>::min();
  static const StreamID kGlobalEnd = std::numeric_limits<StreamID>::max();

  /** Creates an allocator for entire stream ID space. */
  StreamAllocator() : StreamAllocator(kGlobalFirst, kGlobalEnd) {
  }

  /** Creates an allocator for given range of stream IDs. */
  StreamAllocator(StreamID first, StreamID end)
      : first_(first), end_(end), next_(first_) {
    CheckValid();
  }

  void CheckValid() const {
    assert(first_ <= next_);
    assert(next_ <= end_);
  }

  /** First stream ID available for this allocator. */
  StreamID first_;
  /** The first stream ID not available for this allocator after first_. */
  StreamID end_;
  /** Next stream ID that can be allocated. */
  StreamID next_;
};

}  // namespace rocketspeed
