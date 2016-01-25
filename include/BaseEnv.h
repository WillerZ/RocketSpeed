// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <memory>
#include <functional>
#include <string>

#include "Status.h"
#include "Slice.h"
#include "EnvOptions.h"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif

namespace rocketspeed {

class SequentialFile;

class BaseEnv {
 public:
  BaseEnv() {}

  virtual ~BaseEnv() {}

  /** An identifier for a thread. */
  typedef uint64_t ThreadId;

  /**
   * Start a new thread, invoking "function(arg)" within the new thread.
   * When "function(arg)" returns, the thread will be destroyed.
   * Returns an identifier for the thread that is created.
   */
  virtual ThreadId StartThread(void (*function)(void* arg),
                               void* arg,
                               const std::string& thread_name = "") = 0;

  /** Start a new thread, invoking an std::function. */
  virtual ThreadId StartThread(std::function<void()> f,
                               const std::string& thread_name = "") = 0;

  /** Waits for the specified thread to exit */
  virtual void WaitForJoin(ThreadId tid) = 0;

  /** Gets the thread ID for the current thread. */
  virtual ThreadId GetCurrentThreadId() const = 0;

  /** Sets a thread name using the native thread handle if supported. */
  virtual void SetCurrentThreadName(const std::string& name);

  /** Gets a thread name for current thread. */
  virtual const std::string& GetCurrentThreadName();

  /**
   * Returns the number of micro-seconds since some fixed point in time.
   * Only useful for computing deltas of time.
   */
  virtual uint64_t NowMicros() = 0;

  /**
   * Returns the number of nano-seconds since some fixed point in time.
   * Only useful for computing deltas of time.
   */
  virtual uint64_t NowNanos() = 0;

  /** Get the current host name. */
  virtual Status GetHostName(char* name, uint64_t len) = 0;

  /**
   * Create a brand new sequentially-readable file with the specified name.
   * On success, stores a pointer to the new file in *result and returns OK.
   * On failure stores nullptr in *result and returns non-OK.  If the file does
   * not exist, returns a non-OK status.
   * The returned file will only be accessed by one thread at a time.
   */
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options);

 private:
  // No copying allowed
  BaseEnv(const BaseEnv&);
  void operator=(const BaseEnv&);
};

/** A file abstraction for reading sequentially through a file */
class SequentialFile {
 public:
  SequentialFile() {}

  virtual ~SequentialFile() {};

  /**
   * Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
   * written by this routine.  Sets "*result" to the data that was
   * read (including if fewer than "n" bytes were successfully read).
   * May set "*result" to point at data in "scratch[0..n-1]", so
   * "scratch[0..n-1]" must be live when "*result" is used.
   * If an error was encountered, returns a non-OK status.
   * REQUIRES: External synchronization
   */
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  /**
   * Skip "n" bytes from the file. This is guaranteed to be no
   * slower that reading the same data, but may be faster.
   * If end of file is reached, skipping will stop at the end of the
   * file, and Skip will return OK.
   * REQUIRES: External synchronization
   */
  virtual Status Skip(uint64_t n) = 0;

  /**
   * Remove any kind of caching of data from the offset to offset+length
   * of this file. If the length is 0, then it refers to the end of file.
   * If the system is not caching the file contents, then this is a noop.
   */
  virtual Status InvalidateCache(size_t offset, size_t length) {
    return Status::NotSupported("InvalidateCache not supported.");
  }
};

}  // namespace rocketspeed
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
