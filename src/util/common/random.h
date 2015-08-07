// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <random>

namespace rocketspeed {

/**
 * Returns a thread local Mersenne Twister PRNG, seeded using
 * std::random_device.
 */
std::mt19937_64& ThreadLocalPRNG();

}  // namespace rocketspeed
