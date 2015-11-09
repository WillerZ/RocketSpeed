//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "src/util/filter_policy.h"
#include "src/util/common/coding.h"

#include "include/Slice.h"

namespace rocketspeed {

namespace {

class BloomFilterPolicy : public FilterPolicy {
 private:
  size_t bits_per_key_;
  size_t num_probes_;
  uint32_t (*hash_func_)(const Slice& key);

  void initialize() {
    // We intentionally round down to reduce probing cost a little bit
    double d = static_cast<double>(bits_per_key_) * 0.69; // 0.69 =~ ln(2)
    num_probes_ = (size_t)(d);
    if (num_probes_ < 1) num_probes_ = 1;
    if (num_probes_ > 30) num_probes_ = 30;
  }

  // Similar to murmur hash but lower cpu cost
  static uint32_t BloomHash(const Slice& key) {
    const uint32_t seed = 0xbc9f1d34;
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char* data = key.data();
    const size_t n = key.size();
    const char* limit = data + n;
    uint32_t h = static_cast<uint32_t>(seed ^ (n * m));

    // Pick up four bytes at a time
    while (data + 4 <= limit) {
      uint32_t w = DecodeFixed32(data);
      data += 4;
      h += w;
      h *= m;
      h ^= (h >> 16);
    }

    // Pick up remaining bytes
    switch (limit - data) {
      case 3:
        h += data[2] << 16;
        // fall through
      case 2:
        h += data[1] << 8;
        // fall through
      case 1:
        h += data[0];
        h *= m;
        h ^= (h >> r);
        break;
    }
    return h;
  }

 public:
  explicit BloomFilterPolicy(int bits_per_key,
                             uint32_t (*hash_func)(const Slice& key))
      : bits_per_key_(bits_per_key), hash_func_(hash_func) {
    initialize();
  }
  explicit BloomFilterPolicy(int bits_per_key)
      : bits_per_key_(bits_per_key) {
    hash_func_ = BloomHash;
    initialize();
  }

  virtual const char* Name() const {
    return "rocketspeed.BuiltinBloomFilter";
  }

  virtual ~BloomFilterPolicy() {
  }

  virtual void CreateFilter(const std::vector<Slice>& keys,
                            std::string* dst) const {
    // Compute bloom filter size (in both bits and bytes)
    size_t n = keys.size();
    size_t bits = n * bits_per_key_;

    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);
    dst->push_back(static_cast<char>(num_probes_));  // Remember # of probes in filter
    char* array = &(*dst)[init_size];
    for (size_t i = 0; i < n; i++) {
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      uint32_t h = hash_func_(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
      for (size_t j = 0; j < num_probes_; j++) {
        const uint32_t bitpos = static_cast<uint32_t>(h % bits);
        array[bitpos/8] |= static_cast<char>((1 << (bitpos % 8)));
        h += delta;
      }
    }
  }

  virtual bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len-1];
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint32_t h = hash_func_(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = static_cast<uint32_t>(h % bits);
      if ((array[bitpos/8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }
};
}

FilterPolicy* FilterPolicy::NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace rocketspeed