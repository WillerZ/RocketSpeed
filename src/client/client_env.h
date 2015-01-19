// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <stdint.h>

#include <cstdarg>
#include <functional>
#include <string>
#include <memory>
#include <vector>

#include "include/Status.h"
#include "src/util/common/base_env.h"

namespace rocketspeed {

class ClientEnv : public BaseEnv {
 public:
  ClientEnv() {}

  /**
   * Return a default environment suitable for the current operating system.
   * The result of Default() belongs to rocketspeed and must never be deleted.
   */
  static ClientEnv* Default();

  virtual ThreadId StartThread(void (*function)(void* arg),
                               void* arg,
                               const std::string& thread_name = "");

  virtual ThreadId StartThread(std::function<void()> f,
                               const std::string& thread_name = "");

  virtual void WaitForJoin(ThreadId tid);

  virtual ThreadId GetCurrentThreadId() const;

  virtual uint64_t NowMicros();

  virtual uint64_t NowNanos();

  virtual Status GetHostName(char* name, uint64_t len);

 private:
  // No copying allowed
  ClientEnv(const ClientEnv&);
  void operator=(const ClientEnv&);
};

}  // namespace rocketspeed
