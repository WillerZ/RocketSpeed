//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <cstdint>
#include <algorithm>
#include <iterator>
#include <limits>
#include <set>
#include <vector>

#include "src/util/id_allocator.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class IDAllocatorTest : public ::testing::Test {
 protected:
  using TestID = uint16_t;

  class TestAllocator : public IDAllocator<TestID, TestAllocator> {
    using Base = IDAllocator<TestID, TestAllocator>;

   public:
    using Base::Base;
  };

  void VerifyMinNumberOfUniqueIDs(TestAllocator alloc, size_t min_ids) {
    std::set<TestID> set;
    TestID id;
    do {
      id = alloc.Next();
    } while (set.insert(id).second);
    ASSERT_GE(set.size(), min_ids);
  }

  void VerifyMutuallyDisjoint(std::vector<TestAllocator> allocs) {
    ASSERT_GT(allocs.size(), 1);
    std::vector<std::set<TestID>> sets;
    for (auto& alloc : allocs) {
      std::set<TestID> set;
      TestID id;
      do {
        id = alloc.Next();
      } while (set.insert(id).second);
      ASSERT_GT(set.size(), 10);
      sets.emplace_back(std::move(set));
    }
    ASSERT_EQ(sets.size(), allocs.size());
    for (size_t i = 0; i < sets.size(); ++i) {
      for (size_t j = 0; j < i; ++j) {
        std::vector<TestID> intersection;
        std::set_intersection(sets[i].begin(),
                              sets[i].end(),
                              sets[j].begin(),
                              sets[j].end(),
                              std::back_inserter(intersection));
        ASSERT_TRUE(intersection.empty());
      }
    }
  }

  void VerifyDivisionMapping(TestAllocator alloc,
                             const std::vector<TestAllocator>& allocs,
                             TestAllocator::DivisionMapping mapping) {
    TestID first = alloc.Next(), next;
    ASSERT_TRUE(alloc.IsSourceOf(first));
    do {
      next = alloc.Next();
      ASSERT_TRUE(alloc.IsSourceOf(next));
      auto index = mapping(next);
      ASSERT_LT(index, allocs.size());
      ASSERT_TRUE(allocs[index].IsSourceOf(next));
    } while (first != next);
  }
};

TEST_F(IDAllocatorTest, DivisionDisjoint2) {
  VerifyMutuallyDisjoint(TestAllocator().Divide(2));
  VerifyMinNumberOfUniqueIDs(std::move(TestAllocator().Divide(2).front()),
                             30000);
}

TEST_F(IDAllocatorTest, DivisionDisjoint3) {
  VerifyMutuallyDisjoint(TestAllocator().Divide(3));
  VerifyMinNumberOfUniqueIDs(std::move(TestAllocator().Divide(2).front()),
                             15000);
}

TEST_F(IDAllocatorTest, DivisionDisjoint8) {
  VerifyMutuallyDisjoint(TestAllocator().Divide(8));
  VerifyMinNumberOfUniqueIDs(std::move(TestAllocator().Divide(2).front()),
                             7500);
}

TEST_F(IDAllocatorTest, DivisionDisjoint10) {
  VerifyMutuallyDisjoint(TestAllocator().Divide(10));
  VerifyMinNumberOfUniqueIDs(std::move(TestAllocator().Divide(2).front()),
                             3000);
}

TEST_F(IDAllocatorTest, DivisionDisjoint3then7) {
  VerifyMutuallyDisjoint(TestAllocator().Divide(3).front().Divide(7));
}

TEST_F(IDAllocatorTest, SmallDivisionMapping) {
  TestAllocator alloc;
  TestAllocator::DivisionMapping mapping;
  auto allocs = alloc.Divide(7, &mapping);
  VerifyDivisionMapping(std::move(alloc), allocs, mapping);
}

TEST_F(IDAllocatorTest, LargeDivisionMapping) {
  TestAllocator alloc;
  TestAllocator::DivisionMapping mapping;
  size_t total_size = std::numeric_limits<TestID>::max(),
         num_pieces = total_size / 7;
  ASSERT_NE(total_size, 7 * num_pieces);
  auto allocs = alloc.Divide(num_pieces, &mapping);
  VerifyDivisionMapping(std::move(alloc), allocs, mapping);
}

TEST_F(IDAllocatorTest, DivisionMappingAfterUse) {
  TestAllocator alloc;
  alloc.Next();
  alloc.Next();
  alloc.Next();
  TestAllocator::DivisionMapping mapping;
  auto allocs = alloc.Divide(7, &mapping);
  VerifyDivisionMapping(std::move(alloc), allocs, mapping);
}

TEST_F(IDAllocatorTest, DivisionMappingComposability) {
  TestAllocator alloc = std::move(TestAllocator().Divide(7).front());
  TestAllocator::DivisionMapping mapping;
  auto allocs = alloc.Divide(4, &mapping);
  VerifyDivisionMapping(std::move(alloc), allocs, mapping);
}
}

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
