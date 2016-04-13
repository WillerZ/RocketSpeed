// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cinttypes>
#include <functional>
#include <limits>
#include <memory>
#include <queue>
#include "include/Logger.h"
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

    bool operator<(const Unprocessed& rhs) const {
      // Priority queue orders using < to mean "lower priority", so objects
      // with higher sequence number have lower priority.
      return seqno > rhs.seqno;
    }

    T object;
    uint64_t seqno;
  };

  struct PriorityQueue : public std::priority_queue<Unprocessed> {
    void clear() {
      // Standard specifies that std::priority_queue has a protected c
      // member that is the underlying container.
      this->c.clear();
      this->c.shrink_to_fit();
    }

    void shrink_to_fit() {
      this->c.shrink_to_fit();
    }

    Unprocessed pop_top() {
      // priority_queue has no way in the interface to move out the top
      // element, so we need to hack around it.
      Unprocessed popped = std::move(const_cast<Unprocessed&>(this->top()));
      this->pop();
      return popped;
    }
  };

  OrderedProcessor(
      std::shared_ptr<Logger> info_log,
      int max_size,
      std::function<void(T)> processor,
      OrderedProcessorMode mode = OrderedProcessorMode::kLossless)
  : max_size_(max_size)
  , next_seqno_(0)
  , processor_(std::move(processor))
  , mode_(mode)
  , info_log_(std::move(info_log)) {
    RS_ASSERT(processor_);
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
      if (queue_.size() >= max_size_) {
        SkipToNext();
        RS_ASSERT(queue_.size() < max_size_);
        st = Status::NoBuffer();
      }
    } else if (mode_ == OrderedProcessorMode::kLossless) {
      if (queue_.size() >= max_size_) {
        return Status::NoBuffer();
      }
    }

    if (seqno < next_seqno_) {
      return Status::InvalidArgument("Seqno already processed");
    }

    // Fits in buffer.
    queue_.emplace(std::move(object), seqno);
    return st;
  }

  /**
   * Resets processor to initial state, preserving buffer size (no
   * reallocations) and processor functor.
   */
  void Reset() {
    next_seqno_ = 0;
    queue_.clear();
  }

 private:
  enum : uint64_t {
    kInvalidSeqno = std::numeric_limits<uint64_t>::max()
  };

  /**
   * Skips sequence numbers until the next item. This only happens in lossy
   * mode where the queue has become full, and the next item isn't available
   * for processing, so we have to skip some sequence numbers.
   */
  void SkipToNext() {
    RS_ASSERT(!queue_.empty());
    uint64_t next_available = queue_.top().seqno;
    LOG_WARN(info_log_, "Dropped %" PRIu64 " sequence numbers (queue full)",
      next_available - next_seqno_);
    next_seqno_ = next_available;
    ProcessHead();
  }

  /**
   * Process any pending objects at the head.
   */
  void ProcessHead() {
    while (!queue_.empty() && queue_.top().seqno <= next_seqno_) {
      if (queue_.top().seqno == next_seqno_) {
        processor_(queue_.pop_top().object);
        ++next_seqno_;
      } else {
        // Was an erroneous, out of order sequence number.
        queue_.pop();
      }
    }
    if (queue_.empty()) {
      // Shrink size when queue becomes empty.
      queue_.shrink_to_fit();
    }
  }

 private:
  PriorityQueue queue_;               // queue of unprocessed data
  uint64_t max_size_;                 // max size of queue_
  uint64_t next_seqno_;               // seqno of next object to process
  std::function<void(T)> processor_;  // function for processing data
  OrderedProcessorMode mode_;
  std::shared_ptr<Logger> info_log_;
};

}  // namespace rocketspeed
