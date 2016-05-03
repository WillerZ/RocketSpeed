// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <unordered_set>

#include "src/util/common/observable_container.h"

namespace rocketspeed {

/// Set of keys that allows event loops to register for changes.
///
/// Multiple appends of the same key will be merged, such that, effectively,
/// only the last write is going to be processed.  I.e. if key is added multiple
/// times and deleted before callback has been invoked, it won't be processed at
/// all.
///
/// Set is tolerant to changes to itself from within the callback, but is not
/// thread-safe.
template <typename T>
class ObservableSet : public ObservableContainer<std::unordered_set<T>> {
  using Base = ObservableContainer<std::unordered_set<T>>;

 public:
  using KeyType = T;

  explicit ObservableSet(EventLoop* event_loop) : Base(event_loop) {}

  bool Empty() const { return Base::Read().empty(); }

  void Add(const KeyType& v) {
    Base::Modify(
        [&](std::unordered_set<T>& set) { set.emplace(std::move(v)); });
  }

  void Remove(const KeyType& v) {
    auto it = Base::Read().find(v);
    if (it != Base::Read().end()) {
      Base::Modify([&](std::unordered_set<T>& set) { set.erase(it); });
    }
  }

  void Clear() {
    if (!Base::Read().empty()) {
      Base::Modify([&](std::unordered_set<T>& set) { set.clear(); });
    }
  }
};

}  // namespace rocketspeed
