//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "client_env.h"

#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "include/Slice.h"
#include "src/util/common/coding.h"

// Get nano time for mach systems
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

namespace {
void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    exit(1);
  }
}
}

namespace rocketspeed {

ClientEnv* ClientEnv::Default() {
  static ClientEnv client_env;
  return &client_env;
}

static void* StartThreadWrapper(void* arg) {
  std::function<void()>* state = reinterpret_cast<std::function<void()>*>(arg);
  (*state)();
  delete state;
  return nullptr;
}

BaseEnv::ThreadId ClientEnv::StartThread(void (*function)(void* arg), void* arg,
                                         const std::string& thread_name) {
  // Forward to std::function version.
  return StartThread([function, arg] () { (*function)(arg); }, thread_name);
}

BaseEnv::ThreadId ClientEnv::StartThread(std::function<void()> f,
                                         const std::string& thread_name) {
  auto named_f = [this, f, thread_name] () {
    SetCurrentThreadName(thread_name);
    f();
  };
  std::function<void()>* state = new std::function<void()>(std::move(named_f));
  pthread_t t;
  PthreadCall("start thread",
              pthread_create(&t, nullptr,  &StartThreadWrapper, state));
  return (BaseEnv::ThreadId)t;
}

void ClientEnv::WaitForJoin(ThreadId tid) {
  PthreadCall("join", pthread_join((pthread_t)tid, nullptr));
}

BaseEnv::ThreadId ClientEnv::GetCurrentThreadId() const {
  return static_cast<BaseEnv::ThreadId>(pthread_self());
}

const std::string ClientEnv::GetCurrentThreadName() {
#if !defined(OS_ANDROID)
  char name[64];
  // this can be optimized by keeping a local hashmap
  if (pthread_getname_np(pthread_self(), name, sizeof(name)) == 0) {
    name[sizeof(name)-1] = 0;
    return std::string(name);
  }
#endif
  return "";
}

void ClientEnv::SetCurrentThreadName(const std::string& name) {
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  pthread_setname_np(pthread_self(), name.c_str());
#endif
#endif
}

uint64_t ClientEnv::NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

uint64_t ClientEnv::NowNanos() {
#if defined(OS_LINUX) || defined(OS_ANDROID)
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#elif __MACH__
  clock_serv_t cclock;
  mach_timespec_t ts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &ts);
  mach_port_deallocate(mach_task_self(), cclock);
#endif
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

Status ClientEnv::GetHostName(char* name, uint64_t len) {
  int ret = gethostname(name, len);
  if (ret < 0) {
    return (errno == EFAULT || errno == EINVAL)
      ? Status::InvalidArgument(strerror(errno))
      : Status::IOError(strerror(errno));
  }
  return Status::OK();
}

}  // namespace rocketspeed
