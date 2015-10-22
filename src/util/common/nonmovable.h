//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

namespace rocketspeed {

class NonMovable {
 protected:
  NonMovable() = default;
  ~NonMovable() = default;

  NonMovable(const NonMovable&) = delete;
  const NonMovable& operator=(const NonMovable&) = delete;
};

}  // namespace rocketspeed
