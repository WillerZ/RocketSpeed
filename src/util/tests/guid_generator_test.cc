//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <future>
#include <vector>
#include <algorithm>

#include "src/util/common/guid_generator.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class GUIDGeneratorTest { };

TEST(GUIDGeneratorTest, Uniqueness) {
  const int num_threads = 100;
  const int guids_per_thread = 10;

  std::vector<std::future<std::vector<GUID>>> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.push_back(std::async([guids_per_thread]() -> std::vector<GUID>{
      GUIDGenerator gen;
      std::vector<GUID> res;
      for (int k = 0; k < guids_per_thread; ++k) {
        res.push_back(gen.Generate());
      }
      return res;
    }));
  }

  std::vector<GUID> all_guids;
  for (size_t i = 0; i < threads.size(); ++i) {
    auto guids = threads[i].get();
    all_guids.insert(all_guids.end(), guids.begin(), guids.end());
  }

  ASSERT_EQ(all_guids.size(), num_threads * guids_per_thread);

  std::sort(all_guids.begin(), all_guids.end());
  size_t uniques = std::unique(all_guids.begin(), all_guids.end())
                 - all_guids.begin();
  ASSERT_EQ(uniques, all_guids.size());
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
