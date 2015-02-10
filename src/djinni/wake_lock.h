// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "include/WakeLock.h"

#include "src-gen/djinni/cpp/WakeLockImpl.hpp"

namespace rocketspeed {
namespace djinni {

class WakeLock : public rocketspeed::WakeLock {
 public:
  explicit WakeLock(std::shared_ptr<WakeLockImpl> wake_lock)
      : wake_lock_(std::move(wake_lock)) {
    assert(wake_lock_);
  }

  void Acquire() override {
    wake_lock_->Acquire(-1);
  }

  void Acquire(uint64_t timeout) override {
    auto timeout1 = static_cast<int64_t>(timeout);
    assert(timeout1 == timeout);
    wake_lock_->Acquire(timeout1);
  }

  void Release() override {
    wake_lock_->Release();
  }

 private:
  std::shared_ptr<WakeLockImpl> wake_lock_;
};

}  // namespace djinni
}  // namespace rocketspeed
