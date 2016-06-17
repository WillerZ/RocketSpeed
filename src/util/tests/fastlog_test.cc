//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <cmath>

#include "src/util/testharness.h"
#include "src/util/testutil.h"

#include "external/fastlog/fastlog.h"

namespace rocketspeed {

class FastlogTest : public ::testing::Test { };

TEST_F(FastlogTest, Basic) {
  const float ACCEPTABLE_DELTA = 0.05f;
  for (float i = 1; i < 2000000; i += 0.3f) {
    ASSERT_LT(std::abs(log(i) - fastlog(i)), ACCEPTABLE_DELTA);
  }
}

} // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
