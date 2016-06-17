//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <cmath>
#include <string>

#include "src/util/testharness.h"
#include "src/port/port.h"

namespace rocketspeed {

class ToStringTest : public ::testing::Test {
public:
  static const float FLOAT_EPSILON;
  static const double DOUBLE_EPSILON;
  static const long double LONG_DOUBLE_EPSILON;
};

const float ToStringTest::FLOAT_EPSILON = 0.000001f;
const double ToStringTest::DOUBLE_EPSILON = 0.000001;
const long double ToStringTest::LONG_DOUBLE_EPSILON = 0.000001;

TEST_F(ToStringTest, Simple) {
  ASSERT_EQ(std::to_string(0), "0");

  ASSERT_EQ(std::to_string(752338274), "752338274");
  ASSERT_EQ(std::to_string(-752338274), "-752338274");
  ASSERT_EQ(std::to_string(4294967295U), "4294967295");

  ASSERT_EQ(std::to_string(752338274L), "752338274");
  ASSERT_EQ(std::to_string(-752338274L), "-752338274");
  ASSERT_EQ(std::to_string(4294967295UL), "4294967295");

  ASSERT_EQ(std::to_string(752338274752338274LL), "752338274752338274");
  ASSERT_EQ(std::to_string(-752338274752338274LL), "-752338274752338274");
  ASSERT_EQ(std::to_string(18446744073709551614ULL), "18446744073709551614");

  float floatVal = 22.00001f;
  ASSERT_TRUE(fabs(std::stof(std::to_string(floatVal)) - floatVal)
    < ToStringTest::FLOAT_EPSILON);

  floatVal = -22487.124f;
  ASSERT_TRUE(fabs(std::stof(std::to_string(floatVal)) - floatVal)
    < ToStringTest::FLOAT_EPSILON);

  double doubleVal = 22.00001;
  ASSERT_TRUE(fabs(std::stod(std::to_string(doubleVal)) - doubleVal)
    < ToStringTest::DOUBLE_EPSILON);

  doubleVal = -22487.124;;
  ASSERT_TRUE(fabs(std::stod(std::to_string(doubleVal)) - doubleVal)
    < ToStringTest::DOUBLE_EPSILON);

  long double lDoubleVal = 22.00001L;
  ASSERT_TRUE(std::fabs(std::stold(std::to_string(lDoubleVal)) - lDoubleVal)
    < ToStringTest::LONG_DOUBLE_EPSILON);

  lDoubleVal = -22.487263856643L;
  ASSERT_TRUE(std::fabs(std::stold(std::to_string(lDoubleVal)) - lDoubleVal)
    < ToStringTest::LONG_DOUBLE_EPSILON);
}

}  // namespace rocketspeed

int main(int argc, char** argv) { return rocketspeed::test::RunAllTests(argc, argv); }
