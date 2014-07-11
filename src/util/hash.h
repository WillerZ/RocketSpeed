// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <stdint.h>
#include <string>

namespace rocketspeed {

/**
 * 64-bit MurmurHash2 implementation. MurmurHash2 is a fast, non-cryptographic
 * hash function with excellent key distribution.
 */
template <typename T>
struct MurmurHash2;

template <>
struct MurmurHash2<size_t> {
  size_t operator()(size_t x) const {
    const size_t m = 0xc6a4a7935bd1e995;
    const unsigned int r = 47;
    size_t h = 0xfacec0defacec0de;
    x *= m;
    x ^= x >> r;
    x *= m;
    h ^= x;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
  }
};

template <>
struct MurmurHash2<std::string> {
  size_t operator()(const std::string& x) const {
    const uint64_t* data = reinterpret_cast<const uint64_t*>(x.data());
    const size_t len = x.size();
    const uint64_t* end = data + (len / 8);
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;

    uint64_t h = 0xfacec0defacec0de ^ (len * m);

    while (data != end) {
      uint64_t k = *data++;
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
    }

    const unsigned char* data2 = reinterpret_cast<const unsigned char*>(data);
    switch (len & 7) {
    case 7: h ^= uint64_t(data2[6]) << 48;
    case 6: h ^= uint64_t(data2[5]) << 40;
    case 5: h ^= uint64_t(data2[4]) << 32;
    case 4: h ^= uint64_t(data2[3]) << 24;
    case 3: h ^= uint64_t(data2[2]) << 16;
    case 2: h ^= uint64_t(data2[1]) << 8;
    case 1: h ^= uint64_t(data2[0]);
            h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
  }
};

}  // namespace rocketspeed
