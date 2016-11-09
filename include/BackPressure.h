// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <stdint.h>
#include "Assert.h"

namespace rocketspeed {

/**
 * Encapsulates responses that processors can return to message sources to
 * indicate back pressure. Different kinds of back pressure are supported:
 *
 * None - No back pressure
 * RetryAfter(delay) - Retry the same request after a delay.
 */
class BackPressure {
 public:
  /**
   * Back pressure response when no back pressure is required, i.e. the
   * messages was processed successfully, and the next message should be
   * sent as soon as it is available.
   */
  static BackPressure None() {
    return BackPressure(Type::kNone);
  }

  /**
   * Request that the message be re-sent after a delay. Messages from the same
   * source will not be sent again until after the delay. Messages from
   * different sources may still be sent.
   *
   * @param delay The delay after which the message will be retried. The delay
   *              is a lower bound: the message will not be retried sooner, but
   *              could be retried at an arbitrarily later time depending on
   *              how contended CPU resources are. With CPU resource available,
   *              the system will do its best to retry as soon as possible
   *              after the requested delay.
   */
  static BackPressure RetryAfter(std::chrono::milliseconds delay) {
    return BackPressure(Type::kRetryAfter, delay);
  }

  /** Checks if not equal to None() */
  explicit operator bool() const {
    return type_ != Type::kNone;
  }

  /** Get the delay, if any. */
  std::chrono::milliseconds Delay() const {
    RS_ASSERT_DBG(type_ == Type::kRetryAfter);
    return delay_;
  }

 private:
  BackPressure() = delete;

  enum class Type {
    kNone,
    kRetryAfter,
  };

  explicit BackPressure(Type type)
  : type_(type), delay_() {}

  explicit BackPressure(Type type, std::chrono::milliseconds delay)
  : type_(type), delay_(delay) {}

  const Type type_;
  const std::chrono::milliseconds delay_{10};
};

}  // namespace rocketspeed
