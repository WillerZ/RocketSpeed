//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/autovector.h"
#include "src/util/random.h"
#include "src/util/testharness.h"
#include <string>
#include <random>
#include <vector>

namespace rocketspeed {

class AutoVectorTest {};

template <class AutoVector>
inline bool IsOnStack(const AutoVector& autoVector)
{
  const void *begin = &autoVector;
  const void *end = &autoVector + 1;
  return begin <= autoVector.data() && autoVector.data() < end;
}

TEST(AutoVectorTest, DefaultCapacity) {
  ASSERT_EQ(autovector<int>().capacity(), 8);
}

TEST(AutoVectorTest, DefaultConstructor) {
  autovector<int, 3> vec;
  ASSERT_TRUE(IsOnStack(vec));
  ASSERT_EQ(vec.size(), 0);
  ASSERT_EQ(vec.capacity(), 3);
}

TEST(AutoVectorTest, ConstructorWithSize) {
  { // on stack
    autovector<int, 3> vec(2);
    ASSERT_TRUE(IsOnStack(vec));
    ASSERT_EQ(vec.size(), 2);
    ASSERT_EQ(vec.capacity(), 3);
    ASSERT_EQ(vec[0], 0);
    ASSERT_EQ(vec[1], 0);
  }
  { // in heap
    autovector<int, 3> vec(4);
    ASSERT_TRUE(!IsOnStack(vec));
    ASSERT_EQ(vec.size(), 4);
    ASSERT_GE(vec.capacity(), 4);
    ASSERT_EQ(vec[0], 0);
    ASSERT_EQ(vec[1], 0);
    ASSERT_EQ(vec[2], 0);
    ASSERT_EQ(vec[3], 0);
  }
}

TEST(AutoVectorTest, ConstructorWithSizeAndValue) {
  { // on stack
    autovector<int, 3> vec(2, -1);
    ASSERT_TRUE(IsOnStack(vec));
    ASSERT_EQ(vec.size(), 2);
    ASSERT_EQ(vec.capacity(), 3);
    ASSERT_EQ(vec[0], -1);
    ASSERT_EQ(vec[1], -1);
  }
  { // in heap
    autovector<int, 3> vec(4, -1);
    ASSERT_TRUE(!IsOnStack(vec));
    ASSERT_EQ(vec.size(), 4);
    ASSERT_GE(vec.capacity(), 4);
    ASSERT_EQ(vec[0], -1);
    ASSERT_EQ(vec[1], -1);
    ASSERT_EQ(vec[2], -1);
    ASSERT_EQ(vec[3], -1);
  }
}

TEST(AutoVectorTest, CopyConstructor) {
  { // on stack
    const autovector<int, 3> vec = { 0, 1 };
    auto copy = vec;
    ASSERT_TRUE(IsOnStack(copy));
    ASSERT_EQ(copy.size(), 2);
    ASSERT_EQ(copy.capacity(), 3);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[1], 1);
  }
  { // in heap
    const autovector<int, 3> vec = { 0, 1, 2, 3 };
    auto copy = vec;
    ASSERT_TRUE(!IsOnStack(copy));
    ASSERT_EQ(copy.size(), 4);
    ASSERT_EQ(copy.capacity(), 4);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[3], 3);
  }
}

TEST(AutoVectorTest, MoveConstructor) {
  { // on stack
    autovector<int, 3> vec = { 0, 1 };
    auto copy = std::move(vec);
    ASSERT_TRUE(IsOnStack(copy));
    ASSERT_EQ(vec.size(), 2);
    ASSERT_EQ(copy.size(), 2);
    ASSERT_EQ(copy.capacity(), 3);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[1], 1);
  }
  { // in heap
    autovector<int, 3> vec = { 0, 1, 2, 3 };
    auto copy = std::move(vec);
    ASSERT_TRUE(!IsOnStack(copy));
    ASSERT_EQ(vec.size(), 0); // Elements were stolen.
    ASSERT_EQ(copy.size(), 4);
    ASSERT_EQ(copy.capacity(), 4);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[3], 3);
  }
}

TEST(AutoVectorTest, ConstructorWithInitializerList) {
  { // on stack
    autovector<int, 3> vec = { 0, 1 };
    ASSERT_TRUE(IsOnStack(vec));
    ASSERT_EQ(vec.size(), 2);
    ASSERT_EQ(vec.capacity(), 3);
    ASSERT_EQ(vec[0], 0);
    ASSERT_EQ(vec[1], 1);
  }
  { // in heap
    autovector<int, 3> vec = { 0, 1, 2, 3 };
    ASSERT_TRUE(!IsOnStack(vec));
    ASSERT_EQ(vec.size(), 4);
    ASSERT_EQ(vec.capacity(), 4);
    ASSERT_EQ(vec[0], 0);
    ASSERT_EQ(vec[3], 3);
  }
}

TEST(AutoVectorTest, CopyOperator) {
  using AutoVector = autovector<int, 3>;
  { // stack to stack
    AutoVector vec = { 0, 1 };
    AutoVector copy = { 2 };
    copy = std::move(vec);
    ASSERT_TRUE(IsOnStack(copy));
    ASSERT_EQ(copy.size(), 2);
    ASSERT_EQ(copy.capacity(), 3);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[1], 1);
  }
  { // stack to heap
    AutoVector vec = { 0, 1 };
    AutoVector copy = { 2, 3, 4, 5 };
    copy = std::move(vec);
    ASSERT_TRUE(!IsOnStack(copy));
    ASSERT_EQ(copy.size(), 2);
    ASSERT_EQ(copy.capacity(), 4);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[1], 1);
  }
  { // heap to stack
    AutoVector vec = { 0, 1, 2, 3 };
    AutoVector copy = { 2 };
    copy = std::move(vec);
    ASSERT_TRUE(!IsOnStack(copy));
    ASSERT_EQ(copy.size(), 4);
    ASSERT_EQ(copy.capacity(), 4);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[3], 3);
  }
  { // heap to heap
    AutoVector vec = { 0, 1, 2, 3 };
    AutoVector copy = { 2, 3, 4, 5, 6 };
    copy = std::move(vec);
    ASSERT_TRUE(!IsOnStack(copy));
    ASSERT_EQ(vec.size(), 5); // Vectors were swaped.
    ASSERT_EQ(vec.capacity(), 5);
    ASSERT_EQ(copy.size(), 4);
    ASSERT_EQ(copy.capacity(), 4);
    ASSERT_EQ(copy[0], 0);
    ASSERT_EQ(copy[3], 3);
  }
}

namespace {

template <class RandomGenerator>
std::string RandomString(RandomGenerator& random_generator) {
  std::uniform_int_distribution<size_t> size(0, 64);
  std::uniform_int_distribution<char> chr('A', 'Z');
  std::string result(size(random_generator), '\0');
  for (char& c : result) {
    c = chr(random_generator);
  }
  return result;
}

template <class StringVector, class RandomGenerator>
StringVector RandomStringVector(
    RandomGenerator& random_generator,
    const size_t max_size
) {
  std::bernoulli_distribution should_erase(0.4);
  StringVector result;
  for (size_t i = 0; i < 100 * max_size; ++i) {
    std::uniform_int_distribution<size_t> position(0, result.size());

    if ((!result.empty() && should_erase(random_generator)) ||
        result.size() == max_size)
    {
      result.erase(result.begin() + position(random_generator));
    } else {
      result.insert(result.begin() + position(random_generator),
                    RandomString(random_generator));
    }
  }
  return result;
}

} // namespace


TEST(AutoVectorTest, SmallRandomTest) {
  using Vector = std::vector<std::string>;
  using AutoVector = autovector<std::string, 8>;

  const int seed = 5757;
  const size_t max_size = 8;

  std::mt19937 vector_generator(seed);
  const auto vector =
    RandomStringVector<Vector>(vector_generator, max_size);

  std::mt19937 autovector_generator(seed);
  const auto autovector =
    RandomStringVector<AutoVector>(autovector_generator, max_size);

  ASSERT_TRUE(IsOnStack(autovector));
  ASSERT_TRUE(vector == Vector(autovector.begin(), autovector.end()));
}

TEST(AutoVectorTest, BigRandomTest) {
  using Vector = std::vector<std::string>;
  using AutoVector = autovector<std::string, 8>;

  const int seed = 5757;
  const size_t max_size = 512;

  std::mt19937 vector_generator(seed);
  const auto vector =
    RandomStringVector<Vector>(vector_generator, max_size);

  std::mt19937 autovector_generator(seed);
  const auto autovector =
    RandomStringVector<AutoVector>(autovector_generator, max_size);

  ASSERT_TRUE(!IsOnStack(autovector));
  ASSERT_TRUE(vector == Vector(autovector.begin(), autovector.end()));
}

}  // namespace rocketspeed

int main(int argc, char** argv) { return rocketspeed::test::RunAllTests(); }
