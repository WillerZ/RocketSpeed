//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "guid_generator.h"

#include <array>
#include <algorithm>

#include "src/util/common/coding.h"
#include "src/util/common/thread_local.h"

namespace {
void free_thread_local(void* ptr) {
  rocketspeed::GUIDGenerator *guid =
    static_cast<rocketspeed::GUIDGenerator *>(ptr);
  delete guid;
}
// A thread-local pointer that caches a thread-specific generator
rocketspeed::ThreadLocalPtr tgenerator_ =
    rocketspeed::ThreadLocalPtr(free_thread_local);

// Returns the generator for this thread
void* get_thread_generator() {
  void* ptr = static_cast<void *>(tgenerator_.Get());
  if (ptr == nullptr) {
    ptr = new rocketspeed::GUIDGenerator();
    tgenerator_.Reset(ptr);
  }
  return ptr;
}
}

namespace rocketspeed {

GUIDGenerator::GUIDGenerator() {
  static_assert(std::is_same<decltype(rng_()), uint64_t>::value,
    "std::mt19937_64 must return uint64_t");
  RS_ASSERT(rng_.min() == 0 &&
         rng_.max() == static_cast<uint64_t>(0) - static_cast<uint64_t>(1));

  // 2 = 64 bits / 32 bits; state_size is in 64-bit words
  std::array<int, 2 * std::mt19937_64::state_size> seed_data;
  std::random_device r;
  std::generate(seed_data.begin(), seed_data.end(), std::ref(r));
  std::seed_seq seq(seed_data.begin(), seed_data.end());
  rng_.seed(seq);
}

GUID GUIDGenerator::Generate() {
  GUID guid;
  guid.hi = rng_();
  guid.lo = rng_();
  return guid;
}

// generates a 16 byte guid string
std::string GUIDGenerator::GenerateString() {
  std::string buf;
  PutFixed64(&buf, rng_());
  PutFixed64(&buf, rng_());
  return buf;
}

// Returns a thread-safe GUID Generator
GUIDGenerator* GUIDGenerator::ThreadLocalGUIDGenerator() {
  return static_cast<GUIDGenerator *>(get_thread_generator());
}
}
