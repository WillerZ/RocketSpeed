//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <endian.h>

#if defined(OS_ANDROID)
// std::to_string(...) is missing in <string> on Android:
// https://code.google.com/p/android/issues/detail?id=53460
#include <sstream>
namespace std {
template <typename T>
string to_string(const T& t) {
  ostringstream os;
  os << t;
  return os.str();
}
} // namespace std
#endif

#define CACHE_LINE_SIZE 64U

namespace rocketspeed {
namespace port {

static const bool kLittleEndian = __BYTE_ORDER == __LITTLE_ENDIAN;

}  // namespace port
}  // namespace rocketspeed
