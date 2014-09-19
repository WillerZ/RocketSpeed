//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <map>
#include <set>
#include <string>
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
  int numCTs = 1000;
  ControlTowerRouter router1(MakeControlTowers(numCTs), 100);
  ControlTowerRouter router2(MakeControlTowers(numCTs * 105 / 100), 100);

  // Count number of changes for 1 million logs.
  int numChanged = 0;
  int numLogs = 1000000;
  for (int i = 0; i < numLogs; ++i) {
    const HostId* host1;
    const HostId* host2;
    ASSERT_TRUE(router1.GetControlTower(i, &host1).ok());
    ASSERT_TRUE(router2.GetControlTower(i, &host2).ok());
    if (!(*host1 == *host2)) {
      ++numChanged;
    }
  }

  // Ideally ~5% should change, but allow for up to 2-8% margin of error.
  ASSERT_LT(numChanged, numLogs * 8 / 100);
  ASSERT_GT(numChanged, numLogs * 2 / 100);
}

TEST(ControlTowerRouterTest, LogDistribution) {
  // Test that logs are well distributed among control towers
  int numControlTowers = 1000;
  ControlTowerRouter router(MakeControlTowers(numControlTowers), 100);
  std::vector<int> logCount(numControlTowers, 0);

  // Count number of changed for 1 million logs.
  int numLogs = 1000000;
  for (int i = 0; i < numLogs; ++i) {
    const HostId* host;
    ASSERT_TRUE(router.GetControlTower(i, &host).ok());
    logCount[host->port]++;
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
