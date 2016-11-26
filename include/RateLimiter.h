// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstddef>
#include <chrono>

namespace rocketspeed {

class RateLimiter {
 public:
  /**
   * @param limit The number of allowed operations in the duration.
   * @param duration The duration as mentioned above.
   */
  RateLimiter(size_t limit, std::chrono::milliseconds duration);

  /** Returns true if the rate limiting allows another event. */
  bool IsAllowed();

  /**
   * Performs a rate limited event.
   * Returns True iff more available else false
   */
  bool TakeOne();

 private:
  const size_t limit_;
  const std::chrono::milliseconds duration_;
  std::chrono::steady_clock::time_point period_start_
    {std::chrono::steady_clock::time_point::min()};
  size_t available_{0};
};

} // namespace rocketspeed
