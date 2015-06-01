/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <array>

namespace facebook { namespace logdevice {

/**
 * A template for defining array-based maps from the set of values of
 * an enum class to arbitrary objects. It's currently used to define a
 * map from E:: error codes into their string representations and
 * descriptions, and a map from MessageType:: values into their string
 * names and deserializers. See ErrorStrings.h, MessageTypes.h.
 */

template<typename Enum, typename Val>
class EnumMap {
public:
  EnumMap() : map_() { setValues(); }

  const Val& operator[](int n) {
    if (n >= 0 && n < static_cast<int>(Enum::MAX)) {
      return map_[n];
    } else {
      return invalidValue();
    }
  }

  const Val& operator[](Enum n) {
    return (*this)[static_cast<int>(n)];
  }

  // Only used in tests. In production maps are initialized at startup and never
  // changed.
  void set(Enum n, Val val) {
    map_[static_cast<int>(n)] = val;
  }

private:

  // sets map values. This function gets specialized in each class
  void setValues();

  // Can be specialized.
  static const Val& invalidValue() {
    return Val::invalid;
  }

  std::array<Val, static_cast<int>(Enum::MAX)> map_;
};


}} // namespace
