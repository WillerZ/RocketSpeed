//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <endian.h>
#include <string>
#include <cstdio>

namespace std {

// std::to_string(...) is missing in <string> on Android:
// https://code.google.com/p/android/issues/detail?id=53460

extern string to_string(int value);
extern string to_string(unsigned int value);
extern string to_string(long value);
extern string to_string(unsigned long value);
extern string to_string(long long value);
extern string to_string(unsigned long long value);
extern string to_string(float value);
extern string to_string(double value);
extern string to_string(long double value);
} // namespace std

#define CACHE_LINE_SIZE 64U

namespace rocketspeed {
namespace port {

static const bool kLittleEndian = __BYTE_ORDER == __LITTLE_ENDIAN;

}  // namespace port
}  // namespace rocketspeed
