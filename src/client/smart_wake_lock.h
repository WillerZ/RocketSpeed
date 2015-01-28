// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "include/WakeLock.h"

namespace rocketspeed {

class SmartWakeLock final {
 public:
  explicit SmartWakeLock(std::shared_ptr<WakeLock> wake_lock)
      : wake_lock_(wake_lock) {
    assert(wake_lock_);
  }

  void AcquireForReceiving() {
    wake_lock_->Acquire(500);
  }

  void AcquireForSending() {
    wake_lock_->Acquire(1000);
  }

  void AcquireForLoadingSubscriptions() {
    wake_lock_->Acquire(100);
  }

  void AcquireForUpdatingSubscriptions() {
    wake_lock_->Acquire(100);
  }

  // Noncopyable
  SmartWakeLock(const SmartWakeLock&) = delete;
  void operator=(const SmartWakeLock&) = delete;

 private:
  std::shared_ptr<WakeLock> wake_lock_;
};

}  // namespace rocketspeed
