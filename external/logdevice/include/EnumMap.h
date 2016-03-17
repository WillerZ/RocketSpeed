/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <array>
#include <cassert>

namespace facebook { namespace logdevice {

/**
 * A template for defining array-based maps from the set of values of
 * an enum class to arbitrary objects. It's currently used to define a
 * map from E:: error codes into their string representations and
 * descriptions, and a map from MessageType:: values into their string
 * names and deserializers. See ErrorStrings.h, MessageTypes.h.
 */

template<typename Enum, typename Val,
         Enum InvalidEnum = Enum::INVALID,
         int Size = static_cast<int>(Enum::MAX)>
class EnumMap {
public:
  EnumMap() : map_() { map_.fill(invalidValue()); setValues(); }

  const Val& operator[](int n) {
    if (n >= 0 && n < Size) {
      return map_[n];
    } else {
      return invalidValue();
    }
  }

  const Val& operator[](Enum n) {
    return (*this)[static_cast<int>(n)];
  }

  Enum reverseLookup(const Val& search_val) {
    if (search_val == invalidValue()) {
      return InvalidEnum;
    }

    int idx = 0;
    for (auto& val : map_) {
      if (search_val == val) {
        return static_cast<Enum>(idx);
      }
      ++idx;
    }
    return invalidEnum();
  }

  // set() is only used during setValues() and in tests.
  // In production maps are initialized at startup and never changed.
  template<typename ValT>
  void set(Enum n, ValT&& val) {
    assert(static_cast<int>(n) < Size);
    map_[static_cast<int>(n)] = std::forward<ValT>(val);
  }

  static constexpr Enum invalidEnum() {
    return InvalidEnum;
  }

  // Must be specialized.
  static const Val& invalidValue();

private:

  // sets map values. This function gets specialized in each class
  void setValues();

  std::array<Val, Size> map_;
};


}} // namespace
