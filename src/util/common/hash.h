// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <stdint.h>
#include <string>
#include "include/Slice.h"

namespace rocketspeed {

/**
 * 64-bit MurmurHash2 implementation. MurmurHash2 is a fast, non-cryptographic
 * hash function with excellent key distribution.
 */
template <typename... T>
struct MurmurHash2;

// Hash combining for multiple objects.
template <typename T, typename... Ts>
struct MurmurHash2<T, Ts...> {
  inline size_t operator()(const T& x, const Ts&... xs) const {
    // Based on boost::hash_combine
    // http://www.boost.org/doc/libs/1_56_0/boost/functional/hash/hash.hpp
    size_t hash = MurmurHash2<Ts...>()(xs...);
    hash ^= MurmurHash2<T>()(x) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    return hash;
  }
};

template <>
struct MurmurHash2<unsigned long long> {
  size_t operator()(unsigned long long xa) const {
    uint64_t x = xa;
    const uint64_t m = 0xc6a4a7935bd1e995;
    const unsigned int r = 47;
    uint64_t h = 0xfacec0defacec0de;
    x *= m;
    x ^= x >> r;
    x *= m;
    h ^= x;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return static_cast<size_t>(h);
  }
};

template <>
struct MurmurHash2<size_t> {
  size_t operator()(size_t xa) const {
    uint64_t x = xa;
    const uint64_t m = 0xc6a4a7935bd1e995;
    const unsigned int r = 47;
    uint64_t h = 0xfacec0defacec0de;
    x *= m;
    x ^= x >> r;
    x *= m;
    h ^= x;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return static_cast<size_t>(h);
  }
};

template <>
struct MurmurHash2<Slice> {
  size_t operator()(Slice x) const {
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

template <>
struct MurmurHash2<std::string> {
  size_t operator()(const std::string& x) const {
    return MurmurHash2<Slice>()(Slice(x));
  }
};


/**
 * 32-bit MurmurHash2 implementation.
 * Based on:
 * https://github.com/lemire/StronglyUniversalStringHashing/blob/master/smhasherpackage/MurmurHash2.cpp
 */
template <typename... T>
struct MurmurHash2_32;

template <>
struct MurmurHash2_32<size_t> {
  size_t operator()(size_t xa) const {
    uint32_t x = static_cast<uint32_t>(xa);
    const uint32_t m = 0x5bd1e995;
    const int r = 24;
    uint32_t h = 0xfacec0de;
    x *= m;
    x ^= x >> r;
    x *= m;
    h ^= x;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return static_cast<size_t>(h);
  }
};


template <>
struct MurmurHash2_32<Slice> {
  size_t operator()(Slice x) const {
    size_t len = x.size();
    const uint32_t seed = 0xfacec0de;

    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    // Initialize the hash to a 'random' value
    uint32_t h = static_cast<uint32_t>(seed ^ len);

    // Mix 4 bytes at a time into the hash
    const uint32_t* data = reinterpret_cast<const uint32_t*>(x.data());

    while (len >= 4) {
      uint32_t k = *data++;
      k *= m;
      k ^= k >> r;
      k *= m;

      h *= m;
      h ^= k;

      len -= 4;
    }

    // Handle the last few bytes of the input array

    const unsigned char* data2 = reinterpret_cast<const unsigned char*>(data);
    switch (len) {
      case 3: h ^= uint32_t(data2[2]) << 16;
      case 2: h ^= uint32_t(data2[1]) << 8;
      case 1: h ^= uint32_t(data2[0]);
        h *= m;
    }

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
  }
};

}  // namespace rocketspeed
