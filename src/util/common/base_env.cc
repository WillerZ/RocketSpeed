//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/base_env.h"

#include <fcntl.h>
#include <pthread.h>
#include <thread>
#include "src/port/port.h"
#include "src/util/common/thread_local.h"

namespace {
std::string gettname() {
#if !defined(OS_ANDROID)
  char name[64];
  if (pthread_getname_np(pthread_self(), name, sizeof(name)) == 0) {
    name[sizeof(name) - 1] = 0;
    return std::string(name);
  }
#endif
  return "";
}

// This is called at thread exit time
void cache_release(void* ptr) {
  std::string* str = static_cast<std::string *>(ptr);
  delete str;
}

// A thread-local pointer that caches a thread-specific generator
rocketspeed::ThreadLocalPtr thread_names =
  rocketspeed::ThreadLocalPtr(cache_release);

// Returns the thread name from a thread-local variable
std::string* thread_name() {
  void* ptr = static_cast<void *>(thread_names.Get());
  if (ptr == nullptr) {
    ptr = new std::string(gettname());
    thread_names.Reset(ptr);
  }
  return static_cast<std::string *>(ptr);
}
}

namespace rocketspeed {


void BaseEnv::SetCurrentThreadName(const std::string& name) {
  thread_name()->assign(name);
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  {
    pthread_setname_np(pthread_self(), name.c_str());
  }
#endif
#endif
}

const std::string& BaseEnv::GetCurrentThreadName() {
  return *thread_name();
}

class SequentialFileImpl : public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  SequentialFileImpl(std::string fname,
                      FILE* f,
                      const EnvOptions& options)
      : filename_(std::move(fname))
      , file_(f) {}

  virtual ~SequentialFileImpl() { fclose(file_); }

  Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = 0;
    do {
      r = fread_unlocked(scratch, 1, n, file_);
    } while (r == 0 && ferror(file_) && errno == EINTR);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
        // We also clear the error so that the reads can continue
        // if a new data is written to the file
        clearerr(file_);
      } else {
        // A partial read with an error: return a non-ok status
        s = Status::IOError(filename_, strerror(errno));
      }
    }
    return s;
  }

  Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return Status::IOError(filename_, strerror(errno));
    }
    return Status::OK();
  }
};

Status BaseEnv::NewSequentialFile(const std::string& fname,
                         std::unique_ptr<SequentialFile>* result,
                         const EnvOptions& options) {
  result->reset();
  FILE* f = nullptr;
  do {
    f = fopen(fname.c_str(), "r");
  } while (f == nullptr && errno == EINTR);
  if (f == nullptr) {
    return Status::IOError(fname, strerror(errno));
  } else {
    int fd = fileno(f);
    if (options.set_fd_cloexec) {
      fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
    result->reset(new SequentialFileImpl(fname, f, options));
    return Status::OK();
  }
}

}  // namespace rocketspeed
