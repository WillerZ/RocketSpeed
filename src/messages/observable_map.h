// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <mutex>
#include <utility>
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
class ObservableMap : public Source<std::pair<Key, Value>>,
                      public Sink<std::pair<Key, Value>> {
 public:
  using KeyValue = std::pair<Key, Value>;

  ObservableMap()
  : read_ready_fd_(true, true) {
  }

  bool Write(KeyValue& kv, bool check_thread = true) final override {
    if (check_thread) {
      write_check_.Check();
    }

    std::lock_guard<std::mutex> lock(map_mutex_);
    auto it = map_.find(kv.first);
    if (it == map_.end()) {
      if (map_.empty()) {
        // First insertion, so trigger read readiness.
        if (read_ready_fd_.write_event(1)) {
          // Error.
        }
      }
      // Didn't find the entry, insert the new key value.
      map_.emplace_back(std::move(kv));
    } else {
      // Existing entry for this key, update value.
      it->second = kv.second;
    }
    return true;
  }

  bool FlushPending(bool check_thread) final override {
    (void)check_thread;
    return true;
  }

  void RegisterReadEvent(EventLoop* event_loop) final override {
    event_loop->RegisterFdReadEvent(
      read_ready_fd_.readfd(),
      [this] () { this->Drain(); });
  }

  void SetReadEnabled(EventLoop* event_loop, bool enabled) final override {
    event_loop->SetFdReadEnabled(read_ready_fd_.readfd(), enabled);
  }

  std::unique_ptr<EventCallback>
  CreateWriteCallback(EventLoop* event_loop,
                      std::function<void()> callback) final override {
    // ObservableMap is always writable.
    // There should never be a need to create a write event.
    assert(false);
    return nullptr;
  }

 private:
  void Drain() {
    // Drainining the Observable map simply means drain each key-value entry
    // in the map.
    std::lock_guard<std::mutex> lock(map_mutex_);
    for (auto it = map_.begin(); it != map_.end(); ) {
      bool ok = this->DrainOne(KeyValue(it->first, std::move(it->second)));
      it = map_.erase(it);
      if (!ok) {
        break;
      }
    }
    if (map_.empty()) {
      // Read the eventfd to clear read-readiness.
      eventfd_t value;
      read_ready_fd_.read_event(&value);
      assert(value > 0);
    }
  }

  ThreadCheck write_check_;
  rocketspeed::port::Eventfd read_ready_fd_;
  std::mutex map_mutex_;
  LinkedMap<Key, Value> map_;
};

}  // namespace rocketspeed
