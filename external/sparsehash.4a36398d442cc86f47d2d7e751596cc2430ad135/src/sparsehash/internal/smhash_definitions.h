// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once
#include <xxhash/xxhash.h>

namespace rocketspeed {

namespace hdetails {

template<typename Arg>
struct SMHashBase {
  using result_type = size_t;
  using argument_type = Arg;
};

template <typename Tp>
size_t hashImpl(Tp val) noexcept {
  return rocketspeed::XXH64(&val, sizeof(val), 0xFACEBEAC);
}

} // namespace hdetails

template <typename Tp>
struct SMHash;

#define xxhash_default_impl(Tp)\
template <>\
struct SMHash<Tp> : public hdetails::SMHashBase<Tp>{\
  size_t operator()(Tp val) const noexcept {\
    return hdetails::hashImpl(val);\
  }\
};

xxhash_default_impl(bool)
xxhash_default_impl(char)
xxhash_default_impl(signed char)
xxhash_default_impl(unsigned char)
xxhash_default_impl(wchar_t)
xxhash_default_impl(char16_t)
xxhash_default_impl(char32_t)
xxhash_default_impl(short)
xxhash_default_impl(int)
xxhash_default_impl(long)
xxhash_default_impl(long long)
xxhash_default_impl(unsigned short)
xxhash_default_impl(unsigned int)
xxhash_default_impl(unsigned long)
xxhash_default_impl(unsigned long long)

template <>
struct SMHash<float> : public hdetails::SMHashBase<float> {
  size_t operator()(float val) const noexcept {
    // hash 0 and -0 to the same value
    return val != 0.0f ? hdetails::hashImpl(val) : hdetails::hashImpl(0.0f);
  }
};

template <>
struct SMHash<double> : public hdetails::SMHashBase<double> {
  size_t operator()(double val) const noexcept {
    // hash 0 and -0 to the same value
    return val != 0.0 ? hdetails::hashImpl(val) : hdetails::hashImpl(0.0);
  }
};

#undef xxhash_default_impl

} // namespace rocketspeed
