// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <utility>
#include "src/port/port.h"
#include "src/messages/event_loop.h"
#include "src/util/common/flow.h"
#include "src/util/common/linked_map.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * Map of keys to values that allows event loops to register for changes to
 * the map. If two writes to a key k with values x and y happen before x is read
 * the then map will merge the writes, and reader will only see (k, y). If the
 * reader drains the map between the writes then it will see (k, x), followed
 * by (k, y). It is up to the user to ensure that both sequences of events
 * preserve application invariants.
 *
 * Elements will be read in insertion order (not update order). e.g. the writes
 * (a, 1), (b, 2), (a, 3), (c, 4) will be read (a, 3), (b, 2), (c, 4). This is
 * to ensure fairness when only few elements can be read at once.
 */
template <typename Key, typename Value>
class ObservableMap : public Source<std::pair<Key, Value>> {
 public:
  using KeyValue = std::pair<Key, Value>;

  ObservableMap()
  : read_ready_fd_(true, true) {
  }

  void Write(Key key, Value value) {
    thread_check_.Check();
    auto it = map_.find(key);
    if (it == map_.end()) {
      if (map_.empty()) {
        // First insertion, so trigger read readiness.
        if (read_ready_fd_.write_event(1)) {
          // Error.
        }
      }
      // Didn't find the entry, insert the new key value.
      map_.emplace_back(std::move(key), std::move(value));
    } else {
      // Existing entry for this key, update value.
      it->second = std::move(value);
    }
  }

  void Remove(Key key) {
    if (map_.erase(key) && map_.empty()) {
      // Read the eventfd to clear read-readiness.
      eventfd_t value;
      read_ready_fd_.read_event(&value);
      RS_ASSERT(value > 0);
    }
  }

  void RegisterReadEvent(EventLoop* event_loop) final override {
    event_loop->RegisterFdReadEvent(
      read_ready_fd_.readfd(),
      [this] () { this->Drain(); });
  }

  void SetReadEnabled(EventLoop* event_loop, bool enabled) final override {
    event_loop->SetFdReadEnabled(read_ready_fd_.readfd(), enabled);
  }

 private:
  void Drain() {
    // Drainining the Observable map simply means drain each key-value entry
    // in the map.
    thread_check_.Check();

    if (map_.empty()) {
      // If map is already empty then do nothing.
      // This is to avoid the read_event assertion below, which would fail
      // since there never was anything in the map.
      return;
    }

    for (auto it = map_.begin(); it != map_.end(); ) {
      KeyValue item(it->first, std::move(it->second));
      it = map_.erase(it);
      if (!this->DrainOne(std::move(item))) {
        break;
      }
    }
    if (map_.empty()) {
      // Read the eventfd to clear read-readiness.
      eventfd_t value;
      read_ready_fd_.read_event(&value);
      RS_ASSERT(value > 0);
    }
  }

  ThreadCheck thread_check_;
  rocketspeed::port::Eventfd read_ready_fd_;
  LinkedMap<Key, Value> map_;
};

}  // namespace rocketspeed
