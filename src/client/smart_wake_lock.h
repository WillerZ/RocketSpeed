// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <chrono>
#include <memory>

#include "include/WakeLock.h"

namespace rocketspeed {

class SmartWakeLock final {
 public:
  explicit SmartWakeLock(std::shared_ptr<WakeLock> wake_lock)
      : wake_lock_(wake_lock), last_acquired_(0) {
    acquire_mutex_.clear();
  }

  void AcquireForReceiving() {
    Acquire();
  }

  void AcquireForSending() {
    Acquire();
  }

  void AcquireForLoadingSubscriptions() {
    Acquire();
  }

  void AcquireForUpdatingSubscriptions() {
    Acquire();
  }

  // Noncopyable
  SmartWakeLock(const SmartWakeLock&) = delete;
  void operator=(const SmartWakeLock&) = delete;

 private:
  std::shared_ptr<WakeLock> wake_lock_;
  std::atomic_flag acquire_mutex_;
  int64_t last_acquired_;

  void Acquire() {
    if (wake_lock_) {
      uint64_t acquire_timeout = 1000;
      int64_t best_before = 500;
      if (!acquire_mutex_.test_and_set(std::memory_order_acquire)) {
        // We'll be checking whether the wake lock is good for all threads that
        // find this mutex closed.
        int64_t now = NowTimestamp();
        int64_t elapsed = now - last_acquired_;
        if (elapsed > best_before) {
          // We have to reacquire.
          wake_lock_->Acquire(acquire_timeout);
          last_acquired_ = now;
        }
        acquire_mutex_.clear(std::memory_order_release);
      }
      // All threads and calls to this lock have the same requirements.
      // If two threads contended we expect the thread that entered to satifsy
      // "mutex requirements" for all threads that skiped CS.
    }
  }

  int64_t NowTimestamp() {
    namespace chrono = std::chrono;
    auto ts = chrono::steady_clock::now().time_since_epoch();
    return chrono::duration_cast<chrono::milliseconds>(ts).count();
  }
};

}  // namespace rocketspeed
