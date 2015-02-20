// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <assert.h>
#include <functional>
#include <memory>
#include "include/Status.h"

namespace rocketspeed {

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
 *
 * Future implementation could use a priority queue.
 */
template <typename T>
struct OrderedProcessor {
  struct Unprocessed {
    Unprocessed()
    : seqno(-1) {}

    Unprocessed(T _object, int _seqno)
    : object(std::move(_object))
    , seqno(_seqno) {}

    T object;
    int seqno;
  };

  OrderedProcessor(int buffer_size,
                   std::function<void(T)> processor)
  : buffer_(new Unprocessed[buffer_size])
  , buffer_size_(buffer_size)
  , next_seqno_(0)
  , processor_(std::move(processor)) {
    assert(processor_);
  }

  Status Process(T object, int seqno) {
    if (seqno == next_seqno_) {
      // This was the next expected seqno, so process immediately.
      processor_(std::move(object));
      ++next_seqno_;

      // Check if there were any enqueued objects to process and process them.
      int head = next_seqno_ % buffer_size_;
      while (buffer_[head].seqno == next_seqno_) {
        processor_(std::move(buffer_[head].object));
        buffer_[head].seqno = -1;
        ++next_seqno_;
        if (++head == buffer_size_) {
          head = 0;
        }
      }
      return Status::OK();
    }

    // Object has arrived out of order.
    // First, check if it fits in the buffer.
    if (seqno > next_seqno_ + buffer_size_) {
      return Status::NoBuffer();
    }

    if (seqno < next_seqno_) {
      return Status::InvalidArgument("Seqno already processed");
    }

    // Fits in buffer, check that it isn't a duplicate.
    int index = seqno % buffer_size_;  // location for new oj
    if (buffer_[index].seqno != -1) {
      return Status::InvalidArgument("Duplicate sequence number provided");
    }

    buffer_[index].seqno = seqno;
    buffer_[index].object = std::move(object);
    return Status::OK();
  }

 private:
  std::unique_ptr<Unprocessed[]> buffer_;  // cyclic buffer of unprocessed data
  int buffer_size_;                        // size of buffer_
  int next_seqno_;                         // seqno of next object to process
  std::function<void(T)> processor_;       // function for processing data
};

}  // namespace rocketspeed
