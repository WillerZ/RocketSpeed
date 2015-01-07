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

#define TO_STRING_HELPER(format, value, MAX_LEN) {      \
  char buf[MAX_LEN];                                    \
  snprintf(buf, MAX_LEN, format, value);                \
  return string(buf);                                   \
}

string to_string(int value) {
  TO_STRING_HELPER("%d", value, 40);
}

string to_string(unsigned value) {
  TO_STRING_HELPER("%u", value, 40);
}

string to_string(long value) {
  TO_STRING_HELPER("%ld", value, 40);
}

string to_string(unsigned long value) {
  TO_STRING_HELPER("%lu", value, 40);
}

string to_string(long long value) {
  TO_STRING_HELPER("%lld", value, 40);
}

string to_string(unsigned long long value) {
  TO_STRING_HELPER("%llu", value, 40);
}

string to_string(float value) {
  TO_STRING_HELPER("%f", value, 40);
}

string to_string(double value) {
  TO_STRING_HELPER("%f", value, 40);
}

string to_string(long double value) {
  TO_STRING_HELPER("%Lf", value, 40);
}

#undef TO_STRING_HELPER

} // namespace std

#define CACHE_LINE_SIZE 64U

namespace rocketspeed {
namespace port {

static const bool kLittleEndian = __BYTE_ORDER == __LITTLE_ENDIAN;

}  // namespace port
}  // namespace rocketspeed
