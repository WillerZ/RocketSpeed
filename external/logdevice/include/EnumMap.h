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
  EnumMap() : map_({}) { setValues(); }

  const Val& operator[](int n) {
    if (n >= 0 && n < static_cast<int>(Enum::MAX) && map_[n].valid()) {
      return map_[n];
    } else {
      return Val::invalid;
    }
  }

  const Val& operator[](Enum n) {
    return (*this)[static_cast<int>(n)];
  }

private:

  // sets map values. This function gets specialized in each class
  void setValues();

  std::array<Val, static_cast<int>(Enum::MAX)> map_;
};


}} // namespace
