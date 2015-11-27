// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <pthread.h>
#include "include/Assert.h"

#ifndef NO_RS_ASSERT
#ifndef NDEBUG
// Only use thread check if RS_ASSERT is enabled and debugging.
#define RS_USE_THREAD_CHECK
#endif
#endif


namespace rocketspeed {

/**
 * Utility for ensuring that a particular piece of code is always called on the
 * correct thread. The first call to Check() will initialize the expected thread
 * and subsequent calls will assert that it is being called on the same thread.
 * When copied in engaged state, expected thread is preserved, otherwise both
 * copy and oiginal remain not engaged.
 *
 * In release builds, everything is a no-op.
 *
 * Current implmentation works on all platforms, on which pthread_t can be
 * wrapped in std::atomic.
 */
struct ThreadCheck {
 public:
  ThreadCheck()
#ifdef RS_USE_THREAD_CHECK
      : thread_id_(0)
#endif
  {
  }

  ThreadCheck(const ThreadCheck& other) { *this = other; }

  ThreadCheck& operator=(const ThreadCheck& other) {
#ifdef RS_USE_THREAD_CHECK
    auto thread_id = other.thread_id_.load(std::memory_order_consume);
    thread_id_.store(thread_id, std::memory_order_release);
#endif
    return *this;
  }
  /**
   * Return numerical id for current thread on all supporteed OSs
   * Should be used only for debug purposes.
   * On OSX returns a value unique only within one process.
   *
   * @return current thread id
   */
  static uint64_t GetCurrentThreadId();

  /**
   * Checks that this is always called on the same thread.
   *
   * @return False if called on different threads, true otherwise.
   */
  inline bool Ok() const {
#ifdef RS_USE_THREAD_CHECK
    uint64_t desired = GetCurrentThreadId(), expected = 0;

    return thread_id_.compare_exchange_strong(expected, desired) ||
          expected == desired;
#endif
    return true;
  }

  /**
   * Asserts that this is always called on the same thread.
   */
  inline void Check() const {
#ifdef RS_USE_THREAD_CHECK
    RS_ASSERT(Ok());
#endif
  }

  /**
   * Transfers the thread check thread to the current thread.
   */
  inline void Reset() const {
#ifdef RS_USE_THREAD_CHECK
    thread_id_ = GetCurrentThreadId();
#endif
  }

 private:
#ifdef RS_USE_THREAD_CHECK
    mutable std::atomic<uint64_t> thread_id_;
#endif
};

}  // namespace rocketspeed

#undef RS_USE_THREAD_CHECK
