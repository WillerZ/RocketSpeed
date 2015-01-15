//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/base_env.h"

#include <thread>
#include <pthread.h>

namespace {
std::string gettname() {
#if !defined(OS_ANDROID)
  char name[64];
  if (pthread_getname_np(pthread_self(), name, sizeof(name)) == 0) {
    name[sizeof(name)-1] = 0;
    return std::string(name);
  }
#endif
  return "";
}

thread_local std::string thread_name = gettname();
}

namespace rocketspeed {

void BaseEnv::SetCurrentThreadName(const std::string& name) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    {
      thread_name = name.c_str();
      pthread_setname_np(pthread_self(), name.c_str());
    }
#endif
#endif
  }

const std::string& BaseEnv::GetCurrentThreadName() {
    return thread_name;
}

}  // namespace rocketspeed
