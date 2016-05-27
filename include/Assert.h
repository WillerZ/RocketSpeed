// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#ifndef NO_RS_ASSERT

#include <stdio.h>
#include <stdlib.h>
#include <sstream>

namespace rocketspeed {

class RSAssertion : public std::ostringstream {
 public:
  RSAssertion(const char *cond, const char *file, int line, const char *fun) {
    fprintf(stderr, "%s:%i: %s: ASSERTION FAILED: (%s)", file, line, fun, cond);
  }

  ~RSAssertion() {
    fprintf(stderr, " %s\n", this->str().c_str());
    fflush(stderr);
    ::abort();
  }
};

// bang turns off 'value computed is not used' error
inline bool bang(bool in) {
  return !in;
}

}

// you need C++11 support for literal string to work as 1st argument.
// C++03 binds it to operator<<(const void *) and prints its address.
#define RS_ASSERT(cond) \
  rocketspeed::bang(!!(cond)) \
  && rocketspeed::RSAssertion(#cond, __FILE__, __LINE__, __FUNCTION__)
#else

namespace rocketspeed {

class RSNoAssertion {
 public:
  RSNoAssertion(bool ) {
  }

  template <typename T>
  const RSNoAssertion& operator<<(T&& ) const {
    return *this;
  }
};

}

#define RS_ASSERT(cond) if (0) rocketspeed::RSNoAssertion(!!(cond))

#endif
