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

// Run some of the tests registered by the TEST() macro.  If the
// environment variable "ROCKETSPEED_TESTS" is not set, runs all tests.
// Otherwise, runs only the tests whose name contains the value of
// "ROCKETSPEED_TESTS" as a substring.  E.g., suppose the tests are:
//    TEST(Foo, Hello) { ... }
//    TEST(Foo, World) { ... }
// ROCKETSPEED_TESTS=Hello will run the first test
// ROCKETSPEED_TESTS=o     will run both tests
// ROCKETSPEED_TESTS=Junk  will run no tests
//
// Returns 0 if all tests pass.
// Dies or returns a non-zero value if some test fails.
extern int RunAllTests();

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

// An instance of Tester is allocated to hold temporary state during
// the execution of an assertion.
class Tester {
 private:
  const char* fname_;
  int line_;
  std::stringstream ss_;

 public:
  Tester(const char* f, int l)
      : fname_(f), line_(l) {
  }

  void Fail() {
    fprintf(stderr, "%s:%d:%s\n", fname_, line_, ss_.str().c_str());
    port::PrintStack(2);
    throw std::logic_error("test failed");
  }

  Tester& Is(bool b, const char* msg) {
    if (!b) {
      ss_ << " Assertion failure " << msg;
      Fail();
    }
    return *this;
  }

  Tester& IsOk(const Status& s) {
    if (!s.ok()) {
      ss_ << " " << s.ToString();
      Fail();
    }
    return *this;
  }

#define BINARY_OP(name, op)                              \
  template <class X, class Y>                           \
  Tester& name(const X& x, const Y& y) {                \
    if (!(x op y)) {                                   \
      ss_ << " failed: " << PrettyPrint(x) << (" " #op " ") << PrettyPrint(y); \
      Fail();                                      \
    }                                                   \
    return *this;                                       \
  }

  BINARY_OP(IsEq, ==)
  BINARY_OP(IsNe, !=)
  BINARY_OP(IsGe, >=)
  BINARY_OP(IsGt, >)
  BINARY_OP(IsLe, <=)
  BINARY_OP(IsLt, <)
#undef BINARY_OP
};

#define ASSERT_TRUE(c) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .Is(!!(c), #c)
#define ASSERT_OK(s) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .IsOk((s))
#define ASSERT_EQ(a, b) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .IsEq((a), (b))
#define ASSERT_NE(a, b) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .IsNe((a), (b))
#define ASSERT_GE(a, b) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .IsGe((a), (b))
#define ASSERT_GT(a, b) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .IsGt((a), (b))
#define ASSERT_LE(a, b) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .IsLe((a), (b))
#define ASSERT_LT(a, b) ::rocketspeed::test::Tester(__FILE__, __LINE__) \
  .IsLt((a), (b))

#define TCONCAT(a, b) TCONCAT1(a, b)
#define TCONCAT1(a, b) a##b

#define TEST(base, name)                                                \
class TCONCAT(_Test_, name) : public base {                             \
  public:                                                               \
    void _Run();                                                        \
    static void _RunIt() {                                              \
      TCONCAT(_Test_, name) t;                                          \
      t._Run();                                                         \
    }                                                                   \
};                                                                      \
bool TCONCAT(_Test_ignored_, name) =                                    \
  ::rocketspeed::test::RegisterTest(                                    \
    #base, #name, &TCONCAT(_Test_, name)::_RunIt);                      \
void TCONCAT(_Test_, name)::_Run()

// Register the specified test.  Typically not used directly, but
// invoked via the macro expansion of TEST.
extern bool RegisterTest(const char* base, const char* name, void (*func)());


}  // namespace test
}  // namespace rocketspeed
