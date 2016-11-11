// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/set_operations.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class SetOperationsTest : public ::testing::Test {};

using Set = std::vector<int>;

static void Check(Set from, Set to, Set added, Set removed) {
  using Pair = std::pair<Set, Set>;
  ASSERT_TRUE(GetDeltas(from, to) == Pair(added, removed));
  ApplyDeltas(added, removed, &from);
  ASSERT_EQ(from, to);
}

TEST_F(SetOperationsTest, Test) {
  Set empty;
  Set one = { 1 };
  Set one_two = { 1, 2 };
  Set one_two_three = { 1, 2, 3 };
  Set one_three = { 1, 3 };
  Set two = { 2 };

  Check(empty, empty, empty, empty);
  Check(empty, one, one, empty);
  Check(one, empty, empty, one);
  Check(empty, one_two, one_two, empty);
  Check(one_two, empty, empty, one_two);
  Check(one, one_two, two, empty);
  Check(one_two, one, empty, two);
  Check(one, two, two, one);
  Check(two, one, one, two);
  Check(one_two_three, two, empty, one_three);
  Check(two, one_two_three, one_three, empty);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
