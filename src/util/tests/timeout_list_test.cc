//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/timeout_list.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

#include <iostream>
#include <string>
#include <vector>

namespace rocketspeed {

class TimeoutListTest {};

TEST(TimeoutListTest, Count) {
  TimeoutList<std::string> tlist;
  tlist.Add("Red");
  tlist.Add("Green");
  tlist.Add("blue");

  ASSERT_EQ(tlist.Size(), 3);
}

TEST(TimeoutListTest, SimpleExpiry) {
  TimeoutList<std::string> tlist;
  tlist.Add("Red");
  tlist.Add("Green");
  tlist.Add("blue");
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  std::vector<std::string> expired;
  tlist.GetExpired(std::chrono::milliseconds(100),
                   back_inserter(expired));
  ASSERT_EQ(expired.size(), 3);
  ASSERT_EQ(tlist.Size(), 0);
}

TEST(TimeoutListTest, UpdatedExpiry) {
  TimeoutList<std::string> tlist;
  tlist.Add("Red");
  tlist.Add("Green");
  tlist.Add("blue");
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // update the expiry time for "Red"
  tlist.Add("Red");
  // check that the list still has 3 items
  ASSERT_EQ(tlist.Size(), 3);
  // expire some items
  std::vector<std::string> expired;
  tlist.GetExpired(std::chrono::milliseconds(200),
                   back_inserter(expired));

  ASSERT_EQ(expired.size(), 2);
  ASSERT_EQ(tlist.Size(), 1);
}

TEST(TimeoutListTest, CallbackExpiry) {
  TimeoutList<std::string> tlist;
  tlist.Add("Red");
  tlist.Add("Green");
  tlist.Add("blue");
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // expire some items
  std::vector<std::string> expired;
  tlist.ProcessExpired(std::chrono::milliseconds(100),
                    [&](std::string colour) {
                      expired.emplace_back(std::move(colour));
                    });

  ASSERT_EQ(expired.size(), 3);
  ASSERT_EQ(tlist.Size(), 0);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
