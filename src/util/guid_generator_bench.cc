//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include <folly/Benchmark.h>
#include <folly/Foreach.h>

#include "common/init/Init.h"
#include "src/util/common/guid_generator.h"

using namespace std;
using namespace folly;
using namespace rocketspeed;

// Spoiler: it's 78.11M iters/s
BENCHMARK(guidGeneration, n) {
  BenchmarkSuspender braces;
  GUIDGenerator gen;
  braces.dismiss();

  FOR_EACH_RANGE (i, 0, n) {
    auto guid = gen.Generate();
    doNotOptimizeAway(guid);
  }
}

int main(int argc, char** argv) {
  facebook::initFacebook(&argc, &argv);

  runBenchmarks();

  return 0;
}
