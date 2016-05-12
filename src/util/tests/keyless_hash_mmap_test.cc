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
#include <iostream>
#include <memory>
#include <unordered_set>

#include "include/KeylessHashMMap.h"
#include "google/sparse_hash_set"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

#include <set>

using GFLAGS::ParseCommandLineFlags;

namespace rocketspeed {

namespace {

// period is used to make keys repeat (and go to the same small array)
constexpr size_t kUniquePeriod = std::numeric_limits<size_t>::max();

template <typename Key, size_t period>
class NonUniqueHashValue {
 public:
  // default ctor to be used for Hashing construction purpose
  NonUniqueHashValue(Key k = std::numeric_limits<Key>::max())
      : key_(k % period) {}

  NonUniqueHashValue(NonUniqueHashValue&&) = default;
  NonUniqueHashValue& operator=(NonUniqueHashValue&&) = default;
  NonUniqueHashValue(const NonUniqueHashValue&) = default;
  NonUniqueHashValue& operator=(const NonUniqueHashValue&) = default;

  // Hash from Value, object specific
  const Key& ExtractKey(const NonUniqueHashValue* ptr) const {
    return ptr->key_;
  }

  // Hash from Key
  size_t Hash(const Key& someKey) const { return someKey; }

  bool Equals(const Key& key1, const Key& key2) const { return key1 == key2; }

  Key key_;
};

template <typename Key>
using UniqueHashValue = NonUniqueHashValue<Key, kUniquePeriod>;

template <size_t period, template <typename...> class Impl>
void RunMapSearchTestImpl() {
  using Key = size_t;
  using Value = NonUniqueHashValue<Key, period>;
  std::unordered_set<std::unique_ptr<Value> > values;
  KeylessHashMMap<Key, Value*, Value, Impl> mapa;
  Random random(static_cast<uint32_t>(time(nullptr)));
  auto num_values = random.Next() % (mapa.MaxElementsPerKey() + 1);

  for (size_t i = 0; i < num_values; ++i) {
    auto it = values.emplace(new Value(i)).first;
    ASSERT_TRUE(mapa.Insert(it->get()));
    ASSERT_TRUE(mapa.Size() == i + 1);
    ASSERT_TRUE(!mapa.Empty());
  }
  ASSERT_TRUE(mapa.Size() == values.size());

  while (!values.empty()) {
    auto it = values.begin();
    auto mapa_it = mapa.Find((*it)->key_);
    ASSERT_TRUE(mapa_it != mapa.End());
    bool found = false;
    while (mapa_it != mapa.End()) {
      if (*mapa_it == it->get()) {
        ASSERT_TRUE(mapa.Erase(*mapa_it));
        found = true;
        break;
      }
      ++mapa_it;
    }
    ASSERT_TRUE(found);
    values.erase(it);
  }
  ASSERT_EQ(values.size(), 0);
  ASSERT_EQ(mapa.Size(), 0);
}

template <size_t period>
void RunMapSearchTest() {
  RunMapSearchTestImpl<period, std::unordered_set>();
  RunMapSearchTestImpl<period, google::sparse_hash_set>();
}

template <size_t period, template <typename...> class Impl>
void TestIteratorUsageImpl() {
  using Key = size_t;
  using Value = NonUniqueHashValue<Key, period>;
  std::vector<std::unique_ptr<Value> > values_owner;
  std::unordered_set<Value*> values;
  KeylessHashMMap<Key, Value*, Value, Impl> mapa;
  Random random(static_cast<uint32_t>(time(nullptr)));
  auto num_values = random.Next() % (mapa.MaxElementsPerKey() + 1);

  for (size_t i = 0; i < num_values; ++i) {
    std::unique_ptr<Value> value(new Value(i));
    values_owner.emplace_back(std::move(value));
    values.emplace(values_owner[i].get());
    ASSERT_TRUE(mapa.Insert(values_owner[i].get()));
    ASSERT_TRUE(mapa.Size() == i + 1);
    ASSERT_TRUE(!mapa.Empty());
  }
  ASSERT_TRUE(mapa.Size() == values.size());

  for (auto it = mapa.Begin(); it != mapa.End(); ++it) {
    auto map_it = values.find(*it);
    ASSERT_TRUE((*it)->key_ != std::numeric_limits<Key>::max());
    ASSERT_TRUE(map_it != values.end());
    values.erase(map_it);
  }
  ASSERT_EQ(values.size(), 0);
}

template <size_t period>
void TestIteratorUsage() {
  TestIteratorUsageImpl<period, std::unordered_set>();
  TestIteratorUsageImpl<period, google::sparse_hash_set>();
}

template <typename KeyT, typename ValueT, template <typename...> class Impl>
void RunInsertionDeletionChecksImpl() {
  KeylessHashMMap<KeyT, ValueT*, ValueT, Impl> mapa;
  Random random(static_cast<uint32_t>(time(nullptr)));
  auto num_values = random.Next() % (mapa.MaxElementsPerKey() + 1);
  std::vector<std::unique_ptr<ValueT> > values;
  values.reserve(num_values);

  for (size_t i = 0; i < num_values; ++i) {
    values.emplace_back(new ValueT(i));
    ASSERT_TRUE(mapa.Insert(values[i].get()));
    ASSERT_TRUE(mapa.Size() == i + 1);
    ASSERT_TRUE(!mapa.Empty());
  }
  ASSERT_TRUE(mapa.Size() == values.size());

  while (!values.empty()) {
    auto idx = random.Next() % values.size();
    ASSERT_TRUE(mapa.Size() == values.size());
    ASSERT_TRUE(mapa.Erase(values[idx].get()));
    values.erase(values.begin() + idx);
  }
  ASSERT_TRUE(mapa.Empty());
  ASSERT_EQ(mapa.Size(), 0);
}

template <typename KeyT, typename ValueT>
void RunInsertionDeletionChecks() {
  RunInsertionDeletionChecksImpl<KeyT, ValueT, std::unordered_set>();
  RunInsertionDeletionChecksImpl<KeyT, ValueT, google::sparse_hash_set>();
}
};

struct TestValue {
  explicit TestValue(size_t i) : idx(i) {}
  size_t idx;
};

class SmallIntArrayTest {};

TEST(SmallIntArrayTest, InsertionDelition) {
  SmallIntArray<TestValue*> array;
  std::vector<std::unique_ptr<TestValue> > values;
  const size_t num_values = array.MaxSize();
  values.reserve(num_values);

  for (size_t i = 0; i < num_values; ++i) {
    values.emplace_back(new TestValue(i));
    ASSERT_TRUE(array.PushBack(values[i].get()));
    ASSERT_EQ(array.Size(), i + 1);
    ASSERT_TRUE(!array.Empty());
  }

  // enough is enough, we inserted MaxSize() already
  ASSERT_TRUE(!array.PushBack(values[0].get()));

  Random random(static_cast<uint32_t>(time(nullptr)));
  while (!values.empty()) {
    auto idx = random.Next() % values.size();
    auto ptr = values[idx].get();
    ASSERT_EQ(array.Size(), values.size());
    ASSERT_TRUE(array.Erase(ptr));
    values.erase(values.begin() + idx);
  }
  ASSERT_EQ(array.Size(), 0);
  ASSERT_TRUE(array.Empty());
}

TEST(SmallIntArrayTest, BracketsOperator) {
  Random random(static_cast<uint32_t>(time(nullptr)));
  SmallIntArray<TestValue*> array;
  const size_t num_values = random.Next() % (array.MaxSize() + 1);
  std::vector<std::unique_ptr<TestValue> > values;
  values.reserve(num_values);

  for (size_t i = 0; i < num_values; ++i) {
    values.emplace_back(new TestValue(i));
    ASSERT_TRUE(array.PushBack(values[i].get()));
    ASSERT_EQ(array[i], values[i].get());
  }

  for (size_t i = 0; i < num_values; ++i) {
    ASSERT_EQ(array[i], values[i].get());
  }
  array.Clear();
  ASSERT_EQ(array.Size(), 0);
}

TEST(SmallIntArrayTest, Moving) {
  Random random(static_cast<uint32_t>(time(nullptr)));
  SmallIntArray<TestValue*> array;
  const size_t num_values = random.Next() % (array.MaxSize() + 1);
  std::vector<std::unique_ptr<TestValue> > values;
  values.reserve(num_values);

  for (size_t i = 0; i < num_values; ++i) {
    values.emplace_back(new TestValue(i));
    ASSERT_TRUE(array.PushBack(values[i].get()));
  }

  auto moved = std::move(array);
  ASSERT_EQ(array.Size(), 0);
  ASSERT_TRUE(array.Empty());
  ASSERT_EQ(moved.Size(), values.size());

  for (size_t i = 0; i < num_values; ++i) {
    auto ptr = values[i].get();
    ASSERT_EQ(moved[i], ptr);
  }
}

class KeylessHashMMapTest {};

TEST(KeylessHashMMapTest, InsertionDeletion) {
  using Key = size_t;
  RunInsertionDeletionChecks<Key, UniqueHashValue<Key> >();
  RunInsertionDeletionChecks<Key, NonUniqueHashValue<Key, 2> >();
  RunInsertionDeletionChecks<Key, NonUniqueHashValue<Key, 3> >();
  RunInsertionDeletionChecks<Key, NonUniqueHashValue<Key, 17> >();
}

TEST(KeylessHashMMapTest, IteratorUsage) {
  TestIteratorUsage<kUniquePeriod>();
  TestIteratorUsage<1>();
  TestIteratorUsage<2>();
  TestIteratorUsage<13>();
}

TEST(KeylessHashMMapTest, Searching) {
  RunMapSearchTest<kUniquePeriod>();
  RunMapSearchTest<1>();
  RunMapSearchTest<2>();
  RunMapSearchTest<16>();
  RunMapSearchTest<128>();
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  return rocketspeed::test::RunAllTests();
}

#endif  // GFLAGS