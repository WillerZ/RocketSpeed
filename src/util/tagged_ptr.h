// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include "include/Assert.h"

namespace rocketspeed {

/**
 * 64 bits pointer with additional data associated with it (tag).
 * The implementation uses knowledge of x86_64 virtual address format.
 * See http://en.wikipedia.org/wiki/X86-64#Canonical_form_addresses
 * TODO(dyniusz): If we drop the default ctor and use clear() instead
 *      we could allocate and array of these with malloc instead of new[]
 *      and save C++ size data overhead (if there's any for default
 *      destructable type)
 * NOTE: implementation is based on casts between pointer and uintptr_t
 *       which is OK
 *       as long the uintptr_t has the very same value when casting back
 */
template <typename T>
class TaggedPtr {
 public:
  using Tag = uint16_t;

  TaggedPtr() { Reset(); }

  // acts like a raw pointer in terms of copy & move
  TaggedPtr(const TaggedPtr&) = default;
  TaggedPtr(TaggedPtr&&) = default;
  TaggedPtr& operator=(const TaggedPtr&) = default;
  TaggedPtr& operator=(TaggedPtr&&) = default;

  bool operator==(const TaggedPtr& other) const { return impl_ == other.impl_; }

  void Reset() { impl_ = 0; }

  // gets raw pointer value
  T* GetPtr() const {
    // check if 47th bit is set, turn on high bits if that's the case
    const uintptr_t highBits = ~(((kBit47 & impl_) << 1U) - 1U);
    const uintptr_t lowBits = impl_ & kLowBitsMask;
    return reinterpret_cast<T*>(highBits | lowBits);
  }

  // sets raw pointer value, doesn't touch the tag part
  void SetPtr(const T* ptr) {
    const uintptr_t highBits = impl_ & ~kLowBitsMask;
    const uintptr_t lowBits = reinterpret_cast<uintptr_t>(ptr) & kLowBitsMask;
    impl_ = reinterpret_cast<uintptr_t>(highBits | lowBits);
  }

  // sets tag data, doesn't touch the pointer part
  void SetTag(Tag tag) {
    const uintptr_t highBits = uintptr_t(tag) << kNumLowBits;
    const uintptr_t lowBits = impl_ & kLowBitsMask;
    impl_ = highBits | lowBits;
  }

  // gets tag data
  Tag GetTag() const { return static_cast<Tag>(impl_ >> kNumLowBits); }

  void SetInvalidValue() {
    RS_ASSERT(0 == impl_);
    impl_ = kInvalidValue;
  }

  bool IsInvalidValue() const {
    return kInvalidValue == impl_;
  }

 private:
  static constexpr uint8_t kNumLowBits = 48;
  static constexpr uintptr_t kBit47 = 1ULL << 47;;
  static constexpr uintptr_t kLowBitsMask = 0xffffffffffff;
  // Violates 47bit rule
  static constexpr uintptr_t kInvalidValue = 0xfaceb00cfeed;
  uintptr_t impl_;
};

#if !defined(__x86_64__)
static_assert(false, "TaggedPtr is not supported on your arch");
#endif
static_assert(sizeof(TaggedPtr<void>) == 8, "Should be 64 bits");

}  // namespace rocketspeed
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
