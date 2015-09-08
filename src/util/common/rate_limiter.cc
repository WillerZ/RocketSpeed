//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/RateLimiter.h"

namespace rocketspeed {

RateLimiter::RateLimiter(size_t limit, std::chrono::milliseconds duration)
    : limit_(limit), duration_(duration) {}

bool RateLimiter::IsAllowed() {
  auto now = std::chrono::steady_clock::now();
  if (now >= period_start_ + duration_) {
    period_start_ = now;
    available_ = limit_;
  }

  if (available_ > 0) {
    --available_;
    return true;
  }
  return false;
}

} // namespace rocketspeed
