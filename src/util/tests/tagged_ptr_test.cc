//  Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocketspeed tools\n");
  return 1;
}
#else


#include <gflags/gflags.h>
#include <memory>

#include "include/TaggedPtr.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

using GFLAGS::ParseCommandLineFlags;

namespace rocketspeed {

const std::string sentence("All work and no play makes Jack a dull boy");
template <typename T>
void RunPtrChecks(const T* ptr1, const T* ptr2, bool dereference) {
  TaggedPtr<T> ptr;
  using TagType = typename TaggedPtr<T>::Tag;

  auto maxTag = std::numeric_limits<TagType>::max();
  for (TagType tag = 0; tag < maxTag; ++tag) {
    ptr.SetPtr(ptr1);
    ptr.SetTag(tag);
    ASSERT_EQ(ptr.GetTag(), tag);
    ASSERT_EQ(ptr.GetPtr(), ptr1);

    ptr.SetTag(tag + 1);
    ASSERT_EQ(ptr.GetTag(), tag + 1);
    ASSERT_EQ(ptr.GetPtr(), ptr1);
    if (dereference) {
      ASSERT_EQ(*ptr.GetPtr(), *ptr1);
    }
    ptr.SetPtr(ptr2);
    ASSERT_EQ(ptr.GetPtr(), ptr2);
    ASSERT_EQ(ptr.GetTag(), tag + 1);
    if (dereference) {
      ASSERT_EQ(*ptr.GetPtr(), *ptr2);
    }
  }
  TaggedPtr<T> copied = ptr;
  ASSERT_TRUE(copied == ptr);
  TaggedPtr<T> moved = std::move(copied);  // non destructive move
  ASSERT_TRUE(moved == copied);
}

class TaggedPtrTest : public ::testing::Test {};

TEST_F(TaggedPtrTest, HeapPointers) {
  std::unique_ptr<std::string> str1(new std::string(sentence));
  std::unique_ptr<std::string> str2(new std::string(sentence));
  const bool dereference = true;
  RunPtrChecks(str1.get(), str2.get(), dereference);
}

TEST_F(TaggedPtrTest, StackPointers) {
  auto str1(sentence);
  auto str2(sentence);
  const bool dereference = true;
  RunPtrChecks(&str1, &str2, dereference);
}

TEST_F(TaggedPtrTest, MixedPointers) {
  std::unique_ptr<uint32_t> int1(
      new uint32_t(std::numeric_limits<uint32_t>::max()));
  auto int2 = std::numeric_limits<uint32_t>::max();
  const bool dereference = true;
  RunPtrChecks(int1.get(), &int2, dereference);
}

TEST_F(TaggedPtrTest, artificialPointer) {
  uintptr_t ptr1 = 0xffff800000000000;
  uintptr_t ptr2 = 0xffffffffffffffff;
  const bool dereference = false;
  RunPtrChecks(reinterpret_cast<char*>(ptr1), reinterpret_cast<char*>(ptr2),
               dereference);

  ptr1 = 0x00007fffffffffff;
  ptr1 = 0xffff812481248124;
  RunPtrChecks(reinterpret_cast<char*>(ptr1), reinterpret_cast<char*>(ptr2),
               dereference);

  RunPtrChecks(reinterpret_cast<char*>(ptr1), reinterpret_cast<char*>(ptr2),
               dereference);

  ptr2 = 0xffffffffdeadbeef;
  ptr2 = 0xfffffff0faceb00c;
  RunPtrChecks(reinterpret_cast<char*>(ptr1), reinterpret_cast<char*>(ptr2),
               dereference);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  return rocketspeed::test::RunAllTests(argc, argv);
}

#endif  // GFLAGS
