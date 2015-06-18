// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <assert.h>
#include <functional>
#include <limits>
#include <memory>
#include "include/Status.h"

namespace rocketspeed {

enum class OrderedProcessorMode {
  /**
   * In lossless mode, buffer overflows are unrecoverable.
   */
  kLossless = 1,

  /**
   * In lossy mode, buffer overflows cause the processor to drop the next
   * expected message to ensure that the processor always advances.
   */
  kLossy = 2,
};

/**
 * Utility for processing a numbered, but out of order sequence of events in
 * the proper order. For exampple:
 *
 * OrderedProcessor<string> p(10, f);
 * p.Process("foo", 1);
 * p.Process("bar", 0);  // calls f("bar") then f("foo")
 * p.Process("baz", 2);  // calls f("baz")
 *
 * Unprocessed objects are stored in a bounded circular buffer, so only events
 * that are up to N seqnos into the future can be store.
 * User must provide external synchronisation.
 *
 * Future implementation could use a priority queue.
 */
template <typename T>
struct OrderedProcessor {
  typedef T EventType;

  struct Unprocessed {
    Unprocessed()
    : seqno(kInvalidSeqno) {}

    Unprocessed(T _object, int _seqno)
    : object(std::move(_object))
    , seqno(_seqno) {}

    T object;
    uint64_t seqno;
  };

  OrderedProcessor(
      int buffer_size,
      std::function<void(T)> processor,
      OrderedProcessorMode mode = OrderedProcessorMode::kLossless)
  : buffer_(new Unprocessed[buffer_size])
  , buffer_size_(buffer_size)
  , next_seqno_(0)
  , processor_(std::move(processor))
  , mode_(mode) {
    assert(processor_);
  }

  Status Process(T object, uint64_t seqno) {
    if (seqno == next_seqno_) {
      // This was the next expected seqno, so process immediately, and then
      // process any additional messages at the head.
      processor_(std::move(object));
      ++next_seqno_;
      ProcessHead();
      return Status::OK();
    }

    // Object has arrived out of order.
    // First, check if it fits in the buffer.
    Status st;
    if (mode_ == OrderedProcessorMode::kLossy) {
      // Skip head records until this record fits in the buffer.
      if (seqno > next_seqno_ + buffer_size_) {
        do {
          SkipNext();
        } while (seqno > next_seqno_ + buffer_size_);
        st = Status::NoBuffer();
      }
    } else if (mode_ == OrderedProcessorMode::kLossless) {
      if (seqno > next_seqno_ + buffer_size_) {
        return Status::NoBuffer();
      }
    }

    if (seqno < next_seqno_) {
      return Status::InvalidArgument("Seqno already processed");
    }

    // Fits in buffer, check that it isn't a duplicate.
    uint64_t index = seqno % buffer_size_;  // location for new obj
    if (buffer_[index].seqno != kInvalidSeqno) {
      return Status::InvalidArgument("Duplicate sequence number provided");
    }

    buffer_[index].seqno = seqno;
    buffer_[index].object = std::move(object);
    return st;
  }

  /**
   * Resets processor to initial state, preserving buffer size (no
   * reallocations) and processor functor.
   */
  void Reset() {
    next_seqno_ = 0;
    for (uint64_t i = 0; i < buffer_size_; ++i) {
      buffer_[i] = Unprocessed();
    }
  }

  /**
   * Skips the next expected sequence number.
   */
  void SkipNext() {
    ++next_seqno_;
    ProcessHead();
  }

 private:
  enum : uint64_t {
    kInvalidSeqno = std::numeric_limits<uint64_t>::max()
  };

  /**
   * Process any pending objects at the head.
   */
  void ProcessHead() {
    uint64_t head = next_seqno_ % buffer_size_;
    while (buffer_[head].seqno == next_seqno_) {
      processor_(std::move(buffer_[head].object));
      buffer_[head].seqno = kInvalidSeqno;
      ++next_seqno_;
      if (++head == buffer_size_) {
        head = 0;
      }
    }
  }

 private:
  std::unique_ptr<Unprocessed[]> buffer_;  // cyclic buffer of unprocessed data
  uint64_t buffer_size_;                   // size of buffer_
  uint64_t next_seqno_;                    // seqno of next object to process
  std::function<void(T)> processor_;       // function for processing data
  OrderedProcessorMode mode_;
};

}  // namespace rocketspeed
