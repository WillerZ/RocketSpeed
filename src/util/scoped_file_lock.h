// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>
#include "src/port/Env.h"

namespace rocketspeed {

// Obtains a FileLock then closes it on destruction.
class ScopedFileLock {
 public:
  /**
   * Locks a file until destruction.
   *
   * @param env The Env object to use.
   * @param fname File name to lock.
   * @param waitForLock If true, will repeatedly try to lock file until
   *        successful. If false, will try once and may fail.
   */
  ScopedFileLock(Env* env, const std::string& fname, bool waitForLock);

  /**
   * Releases the file lock if possible.
   */
  ~ScopedFileLock();

  /**
   * @return True iff the constructor managed to lock the file.
   */
  bool HaveLock() const { return lock_ != nullptr; }

 private:
  Env* env_;
  FileLock* lock_;
};

}  // namespace rocketspeed
