// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "include/Logger.h"
#include "src/util/common/client_env.h"

#include "djinni_support.hpp"

namespace rocketspeed {

class JvmEnv final : public ClientEnv {
 public:
  /** Initializes JvmEnv, must be called in JNI_OnLoad method. */
  static jint Init(JavaVM* java_vm);

  /**
   * Shuts down JvmEnv, after and during this call no instance of JvmEnv can be
   * in use. Also all threads created by any instance of JvmEnv must exit before
   * this function can be called.
   */
  static void DeInit();

  static JvmEnv* Default();

  ThreadId StartThread(void (*function)(void* arg),
                               void* arg,
                               const std::string& thread_name = "");

  ThreadId StartThread(std::function<void()> f,
                               const std::string& thread_name = "");

  /**
   * Selects logger implementation based on target platform.
   */
  std::shared_ptr<Logger> CreatePlatformLogger(InfoLogLevel log_level);

 private:
  JvmEnv() {}
};

}  // namespace rocketspeed
