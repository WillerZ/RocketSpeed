//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/scoped_file_lock.h"

#include <string>
#include "src/port/Env.h"

namespace rocketspeed {

ScopedFileLock::ScopedFileLock(Env* env,
                               const std::string& fname,
                               bool waitForLock)
: env_(env) {
  do {
    env_->LockFile(fname, &lock_);
  } while (waitForLock && !lock_);
}

ScopedFileLock::~ScopedFileLock() {
  if (lock_) {
    env_->UnlockFile(lock_);
  }
}

}  // namespace rocketspeed
