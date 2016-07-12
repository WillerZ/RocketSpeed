// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>
#include "include/Slice.h"
#include "include/Env.h"
#include "src/port/stack_trace.h"
#include "src/util/random.h"

// ostream output for enums.
template <typename T>
inline typename std::enable_if<std::is_enum<T>::value, std::ostream>::type&
operator<<(std::ostream& os, T enum_value) {
  return os << static_cast<typename std::underlying_type<T>::type>(enum_value);
}

/**
 * Generic pretty-printing. Uses operator<<.
 */
template <typename T>
inline std::string PrettyPrint(const T& value) {
  std::ostringstream ss;
  ss << value;
  return ss.str();
}

template <typename Rep, typename Period>
inline std::string PrettyPrint(const std::chrono::duration<Rep, Period>& dur) {
  using Seconds = std::chrono::duration<double, std::chrono::seconds::period>;
  return PrettyPrint(std::chrono::duration_cast<Seconds>(dur).count()) + "s";
}

/**
 * Pretty printing for vectors.
 */
template <typename T>
inline std::string PrettyPrint(const std::vector<T>& container) {
  std::ostringstream ss;
  ss << "{";
  for (auto it = container.begin(); it != container.end(); ++it) {
    if (it != container.begin()) {
      ss << ", ";
    }
    ss << PrettyPrint(*it);
  }
  ss << "}";
  return ss.str();
}

namespace rocketspeed {
namespace test {

// Return the directory to use for temporary storage.
extern std::string TmpDir();

// Create a logger for the test.
extern Status CreateLogger(Env* env,
                           const std::string& dir,
                           std::shared_ptr<Logger>* logger);

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
extern int RandomSeed();

extern int RunAllTests(int argc, char** argv);

template <typename P>
bool WaitUntil(
    P&& p,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
  using clock = std::chrono::steady_clock;
  auto start = clock::now();
  std::chrono::milliseconds wait(1);
  while (!p()) {
    if (clock::now() - start > timeout) {
      return false;
    }
    /* sleep override */
    std::this_thread::sleep_for(wait);
    wait *= 2;  // exp backoff
  }
  return true;
}

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s);

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(rocketspeed::test::AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(rocketspeed::test::AssertStatus, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).ok())

#define ASSERT_EVENTUALLY_TRUE(p) do { \
    ASSERT_TRUE(::rocketspeed::test::WaitUntil([&](){ return (p); })); \
  } while(0)

}  // namespace test
}  // namespace rocketspeed
