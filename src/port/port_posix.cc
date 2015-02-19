//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/port/port_posix.h"

#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <cstdlib>
#include "src/util/logging.h"

namespace rocketspeed {
namespace port {

static int PthreadCall(const char* label, int result) {
  if (result != 0 && result != ETIMEDOUT) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
  return result;
}

Mutex::Mutex(bool adaptive) {
#ifdef OS_LINUX
  if (!adaptive) {
    PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
  } else {
    pthread_mutexattr_t mutex_attr;
    PthreadCall("init mutex attr", pthread_mutexattr_init(&mutex_attr));
    PthreadCall("set mutex attr",
                pthread_mutexattr_settype(&mutex_attr,
                                          PTHREAD_MUTEX_ADAPTIVE_NP));
    PthreadCall("init mutex", pthread_mutex_init(&mu_, &mutex_attr));
    PthreadCall("destroy mutex attr",
                pthread_mutexattr_destroy(&mutex_attr));
  }
#else  // ignore adaptive for non-linux platform
  PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
#endif // OS_LINUX
}

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() {
  PthreadCall("lock", pthread_mutex_lock(&mu_));
#ifndef NDEBUG
  locked_ = true;
#endif
}

void Mutex::Unlock() {
#ifndef NDEBUG
  locked_ = false;
#endif
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void Mutex::AssertHeld() {
#ifndef NDEBUG
  assert(locked_);
#endif
}

CondVar::CondVar(Mutex* mu)
    : mu_(mu) {
    PthreadCall("init cv", pthread_cond_init(&cv_, nullptr));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
}

bool CondVar::TimedWait(uint64_t abs_time_us) {
  struct timespec ts;
  ts.tv_sec = abs_time_us / 1000000;
  ts.tv_nsec = (abs_time_us % 1000000) * 1000;

#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  int err = pthread_cond_timedwait(&cv_, &mu_->mu_, &ts);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  if (err == ETIMEDOUT) {
    return true;
  }
  if (err != 0) {
    PthreadCall("timedwait", err);
  }
  return false;
}

void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}

RWMutex::RWMutex() {
  PthreadCall("init mutex", pthread_rwlock_init(&mu_, nullptr));
}

RWMutex::~RWMutex() { PthreadCall("destroy mutex", pthread_rwlock_destroy(&mu_)); }

void RWMutex::ReadLock() { PthreadCall("read lock", pthread_rwlock_rdlock(&mu_)); }

void RWMutex::WriteLock() { PthreadCall("write lock", pthread_rwlock_wrlock(&mu_)); }

void RWMutex::ReadUnlock() { PthreadCall("read unlock", pthread_rwlock_unlock(&mu_)); }

void RWMutex::WriteUnlock() { PthreadCall("write unlock", pthread_rwlock_unlock(&mu_)); }

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

#if defined(OS_MACOSX)
Eventfd::Eventfd(bool nonblock, bool close_on_exec) {
  int flags = 0;
  if (nonblock) flags |= O_NONBLOCK;
  if (close_on_exec) flags |= O_CLOEXEC;

  // Create a pipe
  status_ = pipe(fd_);

  // Set attributes on both the pipe descriptors
  if (!flags) {
    if (!status_) {
      status_ =  fcntl(fd_[0], F_SETFL, flags);
    }
    if (!status_) {
      status_ =  fcntl(fd_[1], F_SETFL, flags);
    }
  }
}
int Eventfd::status() const { return status_; }
int Eventfd::closefd() { int tmp = close(fd_[0]); return close(fd_[1]) || tmp; }
int Eventfd::readfd() const { return fd_[0]; }
int Eventfd::writefd() const { return fd_[1]; }

int Eventfd::read_event(eventfd_t *value) {
  ssize_t num = read(fd_[0], static_cast<void *>(value),
                     sizeof(eventfd_t));
  if (num == -1) {
    return -1;   // error
  }
  if (static_cast<unsigned int>(num) == sizeof(eventfd_t)) {
    return 0;   // success
  }
  return -1;    // error
}
int Eventfd::write_event(eventfd_t value) {
  ssize_t num = write(fd_[1], static_cast<void *>(&value), sizeof(eventfd_t));
  if (num == -1) {
    return -1;   // error
  }
  if (static_cast<unsigned int>(num) == sizeof(eventfd_t)) {
    return 0;   // success
  }
  return -1;    // error
}

#else

Eventfd::Eventfd(bool nonblock, bool close_on_exec) {
  int initial_value = 0;
  int flags = 0;
  if (nonblock) flags |= EFD_NONBLOCK;
  if (close_on_exec) flags |= EFD_CLOEXEC;
  fd_[0] = eventfd(initial_value, flags);
}
int Eventfd::status() const { return fd_[0]; }
int Eventfd::closefd() { return close(fd_[0]); }
int Eventfd::readfd() const { return fd_[0]; }
int Eventfd::writefd() const { return fd_[0]; }
int Eventfd::read_event(eventfd_t *value) { return eventfd_read(fd_[0], value);}
int Eventfd::write_event(eventfd_t value) { return eventfd_write(fd_[0],value);}
#endif

}  // namespace port
}  // namespace rocketspeed
