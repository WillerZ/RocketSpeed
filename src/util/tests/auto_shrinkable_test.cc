//  Copyright (c) 2017, Facebook, Inc.  All rights reserved.
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

#include <random>
#include <unordered_map>
#include <vector>

#include "include/AutoShrinkable.h"
#include "src/util/testharness.h"

namespace rocketspeed {

namespace {
using default_threshold = std::ratio<1, 4>;

constexpr size_t kInsert = 100000;
constexpr size_t kRemain = 32;

// Hash testing helpers
template <class Threshold = default_threshold>
using testing_hash_type = auto_shrinkable_hash<
    std::unordered_map<uint64_t, std::string>,
    hash_default_shrink_policy<Threshold>>;

auto defaultHashRemover = [](auto&& c) {
  if (c.empty()) {
    return 0;
  }
  c.erase(begin(c));
  return 1;
};

template <class Container, class Remover, class Checker>
void testHashImpl(Container&& c, Remover remover, Checker checker) {
  // Insert random unique keys.
  std::default_random_engine randEngine{std::random_device{}()};
  std::uniform_int_distribution<uint64_t> dist;
  for (auto i = 0; i < kInsert; ++i) {
    auto key = dist(randEngine);
    while (c.find(key) != c.end()) {
      key = dist(randEngine);
    }
    c[key] = std::to_string(key);
  }
  EXPECT_EQ(kInsert, c.size());
  const auto maxBucketCount = c.bucket_count();

  // Remove items until kRemain or less items are left.
  float minLoadFactor = c.load_factor();
  float maxLoadFactor = c.load_factor();
  while (!c.empty() && c.size() > kRemain) {
    size_t oldSize = c.size();
    size_t removed = remover(std::forward<decltype(c)>(c));
    if (c.size() != (oldSize - removed)) {
      FAIL() << "Mismatch: rm:" << removed << ", oldSz:"
        << oldSize << ", newSz:" << c.size();
      break;
    }
    minLoadFactor = std::min(minLoadFactor, c.load_factor());
    maxLoadFactor = std::max(maxLoadFactor, c.load_factor());
  }
  EXPECT_GE(kRemain, c.size());

  // Customized check depending on test scenario.
  checker(c, maxBucketCount, minLoadFactor, maxLoadFactor);
}

template <class Container>
void testHashNoShrink(Container&& container) {
  testHashImpl(
      std::forward<Container>(container),
      defaultHashRemover,
      [](auto&& cc,
         const auto& maxNBuckets,
         const auto& minLF,
         const auto& maxLF) {
        // No shrink
        ASSERT_EQ(maxNBuckets, cc.bucket_count());
        ASSERT_EQ(float(kRemain) / float(cc.bucket_count()), cc.load_factor());
        ASSERT_EQ(float(kRemain) / maxNBuckets, minLF);
        ASSERT_EQ(float(kInsert) / maxNBuckets, maxLF);
      });
}

template <
    class Threshold = default_threshold,
    class Remover = decltype(defaultHashRemover)>
void testHashShrinkRemove(Remover remover = defaultHashRemover) {
  testHashImpl(
      testing_hash_type<Threshold>{},
      remover,
      [](auto&& cnt,
         const auto& maxNBuckets,
         const auto& minLF,
         const auto& maxLF) {
        // Shrink must have been occurred.
        ASSERT_GT(maxNBuckets, cnt.bucket_count());
        // Cleared or erased all items. No more check needed.
        if (cnt.size() == 0) {
          return;
        }
        // A loose load factor check: the final load factor (after removals)
        // with shrinks must be greater than the load factor without shrink.
        ASSERT_LE(float(kRemain) / maxNBuckets, cnt.load_factor());
        // Tighter lower bound check with 10% tolerance
        const auto lowerBound = cnt.max_load_factor() * cnt.threshold();
        ASSERT_NEAR(lowerBound, minLF, lowerBound * 0.1f);
        // Load factor should not exceed the maximum.
        ASSERT_GE(cnt.max_load_factor(), maxLF);
      });
}

// Vector testing helpers
template <class Threshold = default_threshold>
using testing_vector_type = auto_shrinkable_vector<
    std::vector<uint64_t>,
    vector_default_shrink_policy<Threshold>>;

// Return the number of elements removed
auto defaultVectorRemover = [](auto&& c) {
  if (c.empty()) {
    return 0;
  }
  c.pop_back();
  return 1;
};

template <class Container, class Remover, class Checker>
void testVectorImpl(Container&& cont, Remover remover, Checker checker) {
  std::default_random_engine randEngine{std::random_device{}()};
  std::uniform_int_distribution<uint64_t> dist;
  for (auto i = 0; i < kInsert; ++i) {
    cont.push_back(dist(randEngine));
  }
  EXPECT_EQ(kInsert, cont.size());
  const auto maxCapacity = cont.capacity();

  while (!cont.empty() && cont.size() > kRemain) {
    size_t oldSize = cont.size();
    size_t rmed = remover(std::forward<decltype(cont)>(cont));
    if (cont.size() != (oldSize - rmed)) {
      FAIL() << "Mismatch: rm:" << rmed << ", oldSz:"
        << oldSize << ", newSz:" << cont.size();
      break;
    }
  }
  EXPECT_GE(kRemain, cont.size());

  checker(cont, maxCapacity);
}

template <class Container>
void testVectorNoShrink(Container&& container) {
  testVectorImpl(
      std::forward<Container>(container),
      defaultVectorRemover,
      [](auto&& c, const auto& maxCap) {
        ASSERT_EQ(maxCap, c.capacity()); // No shrink
      });
}

template <
    class Threshold = default_threshold,
    class Remover = decltype(defaultVectorRemover)>
void testVectorShrinkRemove(Remover remover = defaultVectorRemover) {
  testVectorImpl(
      testing_vector_type<Threshold>{},
      remover,
      [](auto&& c, const auto& maxCap) {
        // Shrink must have been occurred.
        ASSERT_GT(maxCap, c.capacity());
        // We reserve twice of the current size.
        ASSERT_LE(c.size() * 2, c.capacity());
      });
}
} // anonymous namespace

TEST(AutoShrinkableTest, ShouldNotShrinkStandardHash) {
  testHashNoShrink(std::unordered_map<uint64_t, std::string>{});
}

TEST(AutoShrinkableTest, ShouldNotShrinkZeroThresholdHash) {
  // auto_shrinkable_hash with threshold of 0 should not shrink.
  testHashNoShrink(testing_hash_type<std::ratio<0, 1>>{});
}

TEST(AutoShrinkableTest, InvalidHashThreshold) {
  // auto_shrinkable_hash accepts threshold value within [0, 0.5).
  // Invalid thresholds must throw static_assert.
  //
  // testing_hash_type<std::ratio<1, 2>> error1;  // Error: threshold >= 0.5
  // testing_hash_type<std::ratio<-1, 4>> error2; // Error: threshold < 0
}

TEST(AutoShrinkableTest, ShouldWorkAutoShrinkableHashRemove) {
  // Removing one item; assume that container is not empty.
  testHashShrinkRemove([](auto&& cc) {
    if (cc.empty()) {
      return 0;
    }
    cc.erase(begin(cc));
    return 1;
  });
  testHashShrinkRemove([](auto&& cc) {
    if (cc.empty()) {
      return 0;
    }
    cc.erase(begin(cc), ++begin(cc));
    return 1;
  });
  testHashShrinkRemove([](auto&& cc) {
    if (cc.empty()) {
      return 0;
    }
    cc.erase(cc.find(begin(cc)->first)->first);
    return 1;
  });

  // Removing at most 4 items at a time.
  testHashShrinkRemove([](auto&& cc) {
    auto endIt = begin(cc);
    const size_t distance = std::min(size_t(4), cc.size());
    std::advance(endIt, distance);
    const size_t oldCount = cc.size();
    cc.erase(begin(cc), endIt);
    EXPECT_EQ(oldCount, cc.size() + distance);
    return distance;
  });
}

TEST(AutoShrinkableTest, ShouldWorkAutoShrinkableHashClear) {
  testHashShrinkRemove([](auto&& cc) {
    size_t rmed = cc.size();
    cc.clear();
    return rmed;
  });
}

TEST(AutoShrinkableTest, NonDefaultThresholdAutoShrinkableHash) {
  testHashShrinkRemove<std::ratio<1, 3>>();
  testHashShrinkRemove<std::ratio<1, 5>>();
  testHashShrinkRemove<std::ratio<1, 7>>();
  testHashShrinkRemove<std::ratio<1, 8>>();
}

TEST(AutoShrinkableTest, ShouldNotShrinkStandardVector) {
  testVectorNoShrink(std::vector<uint64_t>{});
}

TEST(AutoShrinkableTest, ShouldNotShrinkZeroThresholdVector) {
  testVectorNoShrink(testing_vector_type<std::ratio<0, 1>>{});
}

TEST(AutoShrinkableTest, InvalidVectorThreshold) {
  // testing_vector_type<std::ratio<1, 2>> error1;  // Error: threshold >= 0.5
  // testing_vector_type<std::ratio<-1, 4>> error2; // Error: threshold < 0
}

TEST(AutoShrinkableTest, ShouldWorkAutoShrinkableVectorPopBack) {
  testVectorShrinkRemove();
}

TEST(AutoShrinkableTest, ShouldWorkAutoShrinkableVectorErase) {
  testVectorShrinkRemove([](auto&& cc) {
    if (cc.empty()) {
      return 0;
    }
    cc.erase(end(cc) - 1);
    return 1;
  });
  testVectorShrinkRemove([](auto&& cc) {
    if (cc.empty()) {
      return 0;
    }
    cc.erase(end(cc) - 1, end(cc));
    return 1;
  });

  // Removing at most 4 items at a time.
  testVectorShrinkRemove([](auto&& cc) {
    size_t removed = std::min(size_t(4), cc.size());
    cc.erase(end(cc) - std::min(size_t(4), cc.size()), end(cc));
    return removed;
  });
}

TEST(AutoShrinkableTest, ShouldWorkAutoShrinkableVectorResizeClear) {
  testVectorShrinkRemove([](auto&& cc) {
    size_t removed = cc.size() - cc.size() / 4;
    cc.resize(cc.size() / 4);
    return removed;
  });
  testVectorShrinkRemove([](auto&& cc) {
    size_t removed = cc.size();
    cc.clear();
    return removed;
  });
}

TEST(AutoShrinkableTest, NonDefaultThresholdAutoShrinkableVector) {
  testVectorShrinkRemove<std::ratio<1, 3>>();
  testVectorShrinkRemove<std::ratio<1, 5>>();
  testVectorShrinkRemove<std::ratio<1, 7>>();
  testVectorShrinkRemove<std::ratio<1, 8>>();
}
} // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}

#endif  // GFLAGS
