// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <string>
#include <functional>

#include "include/Status.h"
#include "src/util/common/env_options.h"

namespace rocketspeed {

class BaseEnv {
 public:
  BaseEnv() {}

  virtual ~BaseEnv() {}

  // An identifier for a thread.
  typedef uint64_t ThreadId;

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  // Returns an identifier for the thread that is created.
  virtual ThreadId StartThread(void (*function)(void* arg), void* arg,
                               const std::string& thread_name = "") = 0;

  // Start a new thread, invoking an std::function.
  virtual ThreadId StartThread(std::function<void()> f,
                               const std::string& thread_name = "") = 0;

  // Waits for the specified thread to exit
  virtual void WaitForJoin(ThreadId tid) = 0;

  // Gets the thread ID for the current thread.
  virtual ThreadId GetCurrentThreadId() const = 0;

  // Sets a thread name using the native thread handle if supported.
  virtual void SetCurrentThreadName(const std::string& name);

  // Gets a thread name for current thread.
  virtual const std::string& GetCurrentThreadName();

  // Returns the number of micro-seconds since some fixed point in time.
  // Only useful for computing deltas of time.
  virtual uint64_t NowMicros() = 0;

  // Returns the number of nano-seconds since some fixed point in time.
  // Only useful for computing deltas of time.
  virtual uint64_t NowNanos() = 0;

  // Get the current host name.
  virtual Status GetHostName(char* name, uint64_t len) = 0;

 private:
  // No copying allowed
  BaseEnv(const BaseEnv&);
  void operator=(const BaseEnv&);
};

}  // namespace rocketspeed
