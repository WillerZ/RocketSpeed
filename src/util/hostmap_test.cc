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
  ClientID dummyid;
  ASSERT_EQ(map1.Lookup(dummyid), -1);
}

TEST(HostMapTest, Simple) {
  HostId id1("name1", 0);
  HostId id2("name1", 1);
  HostId id3("name2", 1);
  HostId id4("name2", 2);

  // insert id1
  HostMap map(100);
  map.Insert(id1.ToClientId());
  ASSERT_NE(map.Lookup(id1.ToClientId()), -1);

  // insert id2
  map.Insert(id2.ToClientId());
  ASSERT_NE(map.Lookup(id1.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id2.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id1.ToClientId()), map.Lookup(id2.ToClientId()));

  // insert id3
  map.Insert(id3.ToClientId());
  ASSERT_NE(map.Lookup(id1.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id2.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id3.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id3.ToClientId()), map.Lookup(id2.ToClientId()));

  // insert id4
  map.Insert(id4.ToClientId());
  ASSERT_NE(map.Lookup(id1.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id2.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id3.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id4.ToClientId()), -1);
  ASSERT_NE(map.Lookup(id3.ToClientId()), map.Lookup(id4.ToClientId()));
}

}  // namespace rocketspeed

int main(int argc, char** argv) { return rocketspeed::test::RunAllTests(); }
