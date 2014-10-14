//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/testharness.h"
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <chrono>
#include <string>
#include "src/port/stack_trace.h"

namespace rocketspeed {
namespace test {

namespace {
struct Test {
  const char* base;
  const char* name;
  void (*func)();
};
std::vector<Test>* tests;
}

bool RegisterTest(const char* base, const char* name, void (*func)()) {
  if (tests == nullptr) {
    tests = new std::vector<Test>;
  }
  Test t;
  t.base = base;
  t.name = name;
  t.func = func;
  tests->push_back(t);
  return true;
}

int RunAllTests() {
  port::InstallStackTraceHandler();

  const char* matcher = getenv("ROCKETSPEED_TESTS");

  using clock = std::chrono::steady_clock;

  int num = 0;
  if (tests != nullptr) {
    FILE* times_file = fopen("test_times", "a");
    for (unsigned int i = 0; i < tests->size(); i++) {
      const Test& t = (*tests)[i];
      if (matcher != nullptr) {
        std::string name = t.base;
        name.push_back('.');
        name.append(t.name);
        if (strstr(name.c_str(), matcher) == nullptr) {
          continue;
        }
      }
      fprintf(stderr, "==== Test %s.%s\n", t.base, t.name);
      auto start = clock::now();
      (*t.func)();
      auto end = clock::now();
      if (times_file) {
        fprintf(times_file, "%7lums %s.%s\n",
                std::chrono::duration_cast<std::chrono::milliseconds>(
                  end - start).count(),
                t.base, t.name);
      }
      ++num;
    }
    fclose(times_file);
  }
  fprintf(stderr, "==== PASSED %d tests\n", num);
  return 0;
}

std::string TmpDir() {
  std::string dir;
  Status s = Env::Default()->GetTestDirectory(&dir);
  ASSERT_TRUE(s.ok()) << s.ToString();
  return dir;
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != nullptr ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

}  // namespace test
}  // namespace rocketspeed
