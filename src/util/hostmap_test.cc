//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/testharness.h"
#include "src/util/hostmap.h"

namespace rocketspeed {

class HostMapTest {};

TEST(HostMapTest, Empty) {
  HostMap map1(10);
  HostId dummyid;
  ASSERT_EQ(map1.Lookup(dummyid), -1);
}

TEST(HostMapTest, Simple) {
  HostId id1("name1", 0);
  HostId id1alias("name1", 0);
  HostId id2("name1", 1);
  HostId id3("name2", 1);
  HostId id4("name2", 2);

  // insert id1
  HostMap map(100);
  map.Insert(id1);
  ASSERT_NE(map.Lookup(id1), -1);

  // insert an alias of id1
  map.Insert(id1alias);
  ASSERT_NE(map.Lookup(id1), -1);
  ASSERT_EQ(map.Lookup(id1), map.Lookup(id1alias));

  // insert id2
  map.Insert(id2);
  ASSERT_NE(map.Lookup(id1), -1);
  ASSERT_NE(map.Lookup(id2), -1);
  ASSERT_NE(map.Lookup(id1), map.Lookup(id2));

  // insert id3
  map.Insert(id3);
  ASSERT_NE(map.Lookup(id1), -1);
  ASSERT_NE(map.Lookup(id2), -1);
  ASSERT_NE(map.Lookup(id3), -1);
  ASSERT_NE(map.Lookup(id3), map.Lookup(id2));

  // insert id4
  map.Insert(id4);
  ASSERT_NE(map.Lookup(id1), -1);
  ASSERT_NE(map.Lookup(id2), -1);
  ASSERT_NE(map.Lookup(id3), -1);
  ASSERT_NE(map.Lookup(id4), -1);
  ASSERT_NE(map.Lookup(id3), map.Lookup(id4));
}

}  // namespace rocketspeed

int main(int argc, char** argv) { return rocketspeed::test::RunAllTests(); }
