//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <vector>
#include <folly/Benchmark.h>
#include <folly/Foreach.h>
#include "common/init/Init.h"
#include "src/util/consistent_hash.h"

using namespace std;
using namespace folly;
using namespace rocketspeed;

// this var is global, which is not very clean, but should be fine
// for a read-only benchmark regardless of how it's run?
namespace bench {
  size_t counter;
};

void ConsistentHashGet(uint n, size_t initialSize) {
  ConsistentHash<uint64_t, size_t> ch;
  BENCHMARK_SUSPEND {
    for (int i = 0; i < initialSize; ++i) {
      ch.Add(i, 20);
    }
  }

  size_t a = 0;

  FOR_EACH_RANGE(i, 0, n) {
    a += ch.Get(++bench::counter % ch.SlotCount());
  }
  doNotOptimizeAway(a);
}

BENCHMARK_PARAM(ConsistentHashGet, 10)
BENCHMARK_PARAM(ConsistentHashGet, 20)
BENCHMARK_PARAM(ConsistentHashGet, 50)
BENCHMARK_PARAM(ConsistentHashGet, 100)
BENCHMARK_PARAM(ConsistentHashGet, 200)
BENCHMARK_PARAM(ConsistentHashGet, 500)
BENCHMARK_PARAM(ConsistentHashGet, 1000)
BENCHMARK_PARAM(ConsistentHashGet, 2000)

int main(int argc, char** argv) {
  facebook::initFacebook(&argc, &argv);

  bench::counter = 0;

  runBenchmarks();

  return 0;
}
