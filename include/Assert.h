// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

// If NO_RS_ASSERT_DBG isn't set by compiler then automatically set it if we
// are not in debug mode.
#ifndef NO_RS_ASSERT_DBG
# ifdef NDEBUG
#  define NO_RS_ASSERT_DBG
# endif
#endif

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

class RSNoAssertion {
 public:
  RSNoAssertion(bool ) {
  }

  template <typename T>
  const RSNoAssertion& operator<<(T&& ) const {
    return *this;
  }
};

// bang turns off 'value computed is not used' error
inline bool bang(bool in) {
  return !in;
}

}

// you need C++11 support for literal string to work as 1st argument.
// C++03 binds it to operator<<(const void *) and prints its address.
#ifndef NO_RS_ASSERT
# define RS_ASSERT(cond) \
   rocketspeed::bang(!!(cond)) \
   && rocketspeed::RSAssertion(#cond, __FILE__, __LINE__, __FUNCTION__)
#else
# define RS_ASSERT(cond) if (0) rocketspeed::RSNoAssertion(!!(cond))
#endif


#ifndef NO_RS_ASSERT_DBG
# define RS_ASSERT_DBG(cond) \
   rocketspeed::bang(!!(cond)) \
   && rocketspeed::RSAssertion(#cond, __FILE__, __LINE__, __FUNCTION__)
#else
# define RS_ASSERT_DBG(cond) if (0) rocketspeed::RSNoAssertion(!!(cond))
#endif