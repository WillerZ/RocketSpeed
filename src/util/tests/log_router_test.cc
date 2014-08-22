//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include "src/util/log_router.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

using std::string;

namespace rocketspeed {

class LogRouterTest { };

TEST(LogRouterTest, ConsistencyTest) {
  // Test that topic mapping changes minimally when increasing number of logs.
  int numLogs = 1000000;
  LogRouter router1(numLogs);
  LogRouter router2(numLogs * 105 / 100);  // 5% more

  // Count number of changed for 1 million topics.
  int numChanged = 0;
  for (int i = 0; i < 1000000; ++i) {
    Topic topic = std::to_string(i);
    Retention retention = static_cast<Retention>(i % Retention::Total + 1);
    LogID logID1;
    LogID logID2;
    ASSERT_TRUE(router1.GetLogID(topic, retention, &logID1).ok());
    ASSERT_TRUE(router2.GetLogID(topic, retention, &logID2).ok());
    if (logID1 != logID2) {
      ++numChanged;
    }
  }

  // Ideally ~5% should change, but allow for up to 2-8% margin of error.
  ASSERT_LT(numChanged, numLogs * 8 / 100);
  ASSERT_GT(numChanged, numLogs * 2 / 100);
}

TEST(LogRouterTest, RetentionPartitioning) {
  // Test that topics with different retentions go to different logs
  int numLogs = 1000;
  LogRouter router(numLogs);
  std::map<LogID, std::set<Retention>> logRetentions;

  // Count number of changed for 1 million topics.
  for (int i = 0; i < 1000000; ++i) {
    Topic topic = std::to_string(i);
    Retention retention = static_cast<Retention>(i % Retention::Total + 1);
    LogID logID;
    ASSERT_TRUE(router.GetLogID(topic, retention, &logID).ok());
    logRetentions[logID].insert(retention);
    ASSERT_EQ(logRetentions[logID].size(), 1);  // 1 retention per log
  }
}

TEST(LogRouterTest, LogDistribution) {
  // Test that topics are well distributed among logs
  int numLogs = 1000 * static_cast<int>(Retention::Total);
  LogRouter router(numLogs);
  std::vector<int> topicCount(numLogs, 0);

  // Count number of changed for 1 million topics.
  int numTopics = 1000000;
  for (int i = 0; i < numTopics; ++i) {
    Topic topic = std::to_string(i);
    Retention retention = static_cast<Retention>(i % Retention::Total + 1);
    LogID logID;
    ASSERT_TRUE(router.GetLogID(topic, retention, &logID).ok());
    topicCount[logID - 1]++;  // LogIDs start at 1, not 0.
  }

  // Find the minimum and maximum topics per log.
  auto minmax = std::minmax_element(topicCount.begin(), topicCount.end());
  int expected = numTopics / numLogs;  // perfect distribution
  ASSERT_GT(*minmax.first, expected * 0.7);  // allow +/-30% error worst case
  ASSERT_LT(*minmax.second, expected * 1.3);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
