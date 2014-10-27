//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <iostream>
#include "src/util/control_tower_router.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class ControlTowerRouterTest { };

std::vector<HostId> MakeControlTowers(int num) {
  std::vector<HostId> control_towers;
  for (int i = 0; i < num; ++i) {
    control_towers.emplace_back(std::to_string(i), i);
  }
  return control_towers;
}

TEST(ControlTowerRouterTest, ConsistencyTest) {
  // Test that log mapping changes minimally when increasing number of CTs.
  const int numCTs = 1000;
  const size_t numCopies = 3;
  ControlTowerRouter router1(MakeControlTowers(numCTs), 100, numCopies);
  ControlTowerRouter router2(
    MakeControlTowers(numCTs * 105 / 100), 100, numCopies);

  // Count number of changes for 100k logs.
  int numRelocations = 0;
  const int numLogs = 100000;
  for (int i = 0; i < numLogs; ++i) {
    std::vector<HostId const*> hosts1;
    std::vector<HostId const*> hosts2;
    ASSERT_TRUE(router1.GetControlTowers(i, &hosts1).ok());
    ASSERT_TRUE(router2.GetControlTowers(i, &hosts2).ok());
    ASSERT_EQ(hosts1.size(), numCopies);
    ASSERT_EQ(hosts2.size(), numCopies);

    auto host_less = [](HostId const* lhs, HostId const* rhs) {
      return *lhs < *rhs;
    };
    std::sort(hosts1.begin(), hosts1.end(), host_less);
    std::sort(hosts2.begin(), hosts2.end(), host_less);
    std::vector<HostId const*> intersection(numCopies);
    auto intersection_end = std::set_intersection(
      hosts1.begin(), hosts1.end(),
      hosts2.begin(), hosts2.end(),
      intersection.begin(),
      host_less);
    numRelocations += intersection.end() - intersection_end;
  }

  // Ideally ~5% should change, but allow for up to 2-8% margin of error.
  ASSERT_LT(numRelocations, (numLogs * numCopies) * 8 / 100);
  ASSERT_GT(numRelocations, (numLogs * numCopies) * 2 / 100);
}

TEST(ControlTowerRouterTest, LogDistribution) {
  // Test that logs are well distributed among control towers
  int numControlTowers = 1000;
  ControlTowerRouter router(MakeControlTowers(numControlTowers), 100, 1);
  std::vector<int> logCount(numControlTowers, 0);

  // Count number of changed for 100k logs.
  int numLogs = 100000;
  for (int i = 0; i < numLogs; ++i) {
    std::vector<HostId const*> hosts;
    ASSERT_TRUE(router.GetControlTowers(i, &hosts).ok());
    logCount[hosts[0]->port]++;
  }

  // Find the minimum and maximum logs per control tower.
  auto minmax = std::minmax_element(logCount.begin(), logCount.end());
  int expected = numLogs / numControlTowers;  // perfect distribution
  ASSERT_GT(*minmax.first, expected * 0.5);  // allow +/-50% error worst case
  ASSERT_LT(*minmax.second, expected * 1.5);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
