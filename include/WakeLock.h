// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

namespace rocketspeed {

/**
 * If RocketSpeed client is to be run on Android device, an implementation of
 * this interface should be provided when opening a client, in order for the
 * client to acquire it when processing a message and release it when it's
 * waiting for a message to arrive.
 */
class WakeLock {
 public:
  /**
   * Creates a no-op wake lock.
   */
  static std::shared_ptr<WakeLock> GetNull() {
    return std::make_shared<WakeLock>();
  }

  virtual ~WakeLock() {}

  /**
   * Acquires the wake lock.
   */
  virtual void Acquire() {}

  /**
   * Acquires the wake lock with a timeout.
   * @param timeout The timeout (in milliseconds) after which the wake lock will
   * be released automatically.
   */
  virtual void Acquire(uint64_t timeout) {}

  /**
   * Releases the wake lock.
   */
  virtual void Release() {}
};

}  // namespace rocketspeed
