// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "external/logdevice/include/debug.h"

namespace facebook {
namespace logdevice {
namespace dbg {

Level currentLevel = Level::INFO;

int useFD(int fd) {
  (void)fd;
  // Mock implementation doesn't log separately, so ignore this.
  return -1;
}

}  // namespace dbg
}  // namespace logdevice
}  // namespace facebook
