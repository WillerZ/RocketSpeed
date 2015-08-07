//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/testharness.h"
#include "src/util/testutil.h"
#include "src/util/unsafe_shared_ptr.h"

namespace rocketspeed {

class UnsafeSharedPtrTest {};

TEST(UnsafeSharedPtrTest, NullTest) {
  UnsafeSharedPtr<int> shared;
  ASSERT_TRUE(!shared.get());
  ASSERT_TRUE(!shared.unique());
}

TEST(UnsafeSharedPtrTest, TakeOwnershipTest) {
  int* ptr = new int(1);
  auto shared = UnsafeSharedPtr<int>(ptr);
  ASSERT_EQ(shared.get(), ptr);
  ASSERT_TRUE(shared.unique());
}

TEST(UnsafeSharedPtrTest, CopyConstructorTest) {
  int* ptr = new int(1);
  auto shared = UnsafeSharedPtr<int>(ptr);
  ASSERT_TRUE(shared.unique());

  {
    auto sharedCopy = UnsafeSharedPtr<int>(shared);
    ASSERT_EQ(sharedCopy.get(), shared.get());
    ASSERT_TRUE(!shared.unique());
    ASSERT_TRUE(!sharedCopy.unique());
  }

  ASSERT_TRUE(shared.unique());
}

TEST(UnsafeSharedPtrTest, AssignCopyTest) {
  int* ptr = new int(1);
  auto shared = UnsafeSharedPtr<int>(ptr);
  ASSERT_TRUE(shared.unique());

  {
    UnsafeSharedPtr<int> sharedCopy;
    ASSERT_TRUE(!sharedCopy.get());
    ASSERT_TRUE(!sharedCopy.unique());

    sharedCopy = shared;
    ASSERT_TRUE(sharedCopy.get());
    ASSERT_EQ(sharedCopy.get(), shared.get());

    ASSERT_TRUE(!sharedCopy.unique());
    ASSERT_TRUE(!shared.unique());
  }

  ASSERT_TRUE(shared.unique());
}

TEST(UnsafeSharedPtrTest, ResetTest) {
  int* ptr = new int(1);
  auto shared = UnsafeSharedPtr<int>(ptr);
  ASSERT_EQ(shared.get(), ptr);
  ASSERT_TRUE(shared.unique());

  int* ptr2 = new int(5);
  shared.reset(ptr2);
  ASSERT_EQ(shared.get(), ptr2);
  ASSERT_TRUE(shared.unique());

  shared.reset();
  ASSERT_TRUE(!shared.get());
  ASSERT_TRUE(!shared.unique());
}

TEST(UnsafeSharedPtrTest, MakeTest) {
  auto shared = MakeUnsafeSharedPtr<int>(5);
  ASSERT_TRUE(shared.get());
  ASSERT_EQ(*shared, 5);
  ASSERT_TRUE(shared.unique());
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
