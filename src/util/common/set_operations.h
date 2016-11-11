// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <algorithm>
#include <iterator>
#include <utility>
#include "include/Assert.h"

namespace rocketspeed {

/**
 * Takes the next set state as input, and produces the set of added and
 * removed elements.
 */
template <typename Set>
std::pair<Set, Set> GetDeltas(const Set& prev, const Set& next) {
  std::pair<Set, Set> result;
  Set& added = result.first;
  Set& removed = result.second;
  std::set_difference(
      next.begin(), next.end(),
      prev.begin(), prev.end(),
      std::back_inserter(added));
  std::set_difference(
      prev.begin(), prev.end(),
      next.begin(), next.end(),
      std::back_inserter(removed));
  RS_ASSERT(prev.size() + added.size() - removed.size() == next.size());
  return result;
}

/**
 * Takes a set of added and removed deltas and integrates these into the
 * current state.
 */
template <typename Set>
void ApplyDeltas(Set added, const Set& removed, Set* state) {
  if (added.empty() && removed.empty()) {
    return;
  }
  Set retained;  // *state - removed
  RS_ASSERT(state->size() >= removed.size());
  retained.reserve(state->size() - removed.size());
  std::set_difference(
      state->begin(), state->end(),
      removed.begin(), removed.end(),
      std::back_inserter(retained));
  state->clear();
  state->reserve(retained.size() + added.size());
  std::set_union(
      retained.begin(), retained.end(),
      added.begin(), added.end(),
      std::back_inserter(*state));
}

}
