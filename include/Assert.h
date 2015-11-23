// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <stdlib.h>
#include <stdio.h>

/**
 * This assertion exists independent of presence of NDEBUG, unlike assert().
 * but can be disabled by defining NO_RS_ASSERT
 */
#ifndef NO_RS_ASSERT
# define RS_ASSERT(expr) \
  do { if (!(expr)) { \
    fprintf(stderr, "%s:%u: %s: Assertion failed: %s\n", \
      __FILE__, __LINE__, __FUNCTION__, #expr); \
    abort(); \
  } } while (0)
#else
# define RS_ASSERT(expr)
#endif
