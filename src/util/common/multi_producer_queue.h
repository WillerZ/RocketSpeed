// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <mutex>
#include <utility>
#include "external/folly/producer_consumer_queue.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * Simple placeholder implementation of a multi producer, single consumer
 * queue.
 */
template<class T>
struct MultiProducerQueue {
  typedef T value_type;

  // non-copyable.
  MultiProducerQueue(const MultiProducerQueue&) = delete;
  MultiProducerQueue& operator=(const MultiProducerQueue&) = delete;

  explicit MultiProducerQueue(uint32_t size)
  : queue_(size) {
  }

  /**
   * Write an element to the queue. Can be called from any thread.
   *
   * @param args Constructor arguments for the new object.
   * @return true iff the write was successful.
   */
  template<class ...Args>
  bool write(Args&&... args) {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.write(std::forward<Args>(args)...);
  }

  /**
   * Read an element from the queue. Must only be called from one thread.
   *
   * @param record Where to write the element to.
   * @return true iff a record was written.
   */
  bool read(T& record) {
    thread_check_.Check();
    return queue_.read(record);
  }

  size_t sizeGuess() const {
    return queue_.sizeGuess();
  }

 private:
  folly::ProducerConsumerQueue<T> queue_;
  std::mutex mutex_;
  ThreadCheck thread_check_;
};

}
