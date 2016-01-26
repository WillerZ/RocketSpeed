// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "src/port/port.h"
#include "src/messages/event_loop.h"
#include "src/util/common/flow.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * Set of keys that allows event loops to register for changes.
 * Multiple appends of the same key will be merged, such that,
 * effectively, only the last write is going to be processed.
 * I.e. if key is added multiple times and deleted before
 * callback has been invoked, it won't be processed at all.
 *
 * Set is tolerant to changes to itself from within the callback.
 */
template <typename T>
class ObservableSet : public Source<T> {
 public:
  using KeyType = T;

  ObservableSet()
  : read_ready_fd_(true /* nonblock */, true /* close_on_exec */)
  , changed_(false)
  , ready_(false) {
    // empty
  }

  bool Empty() const {
    thread_check_.Check();
    return set_.empty();
  }

  void Add(KeyType v) {
    thread_check_.Check();
    changed_ = true;
    set_.emplace(std::move(v));
    SetReadiness(true);
  }

  void Remove(KeyType v) {
    thread_check_.Check();

    if (set_.erase(v)) {
      changed_ = true;
      if (set_.empty()) {
        SetReadiness(false);
      }
    }
  }

  void Clear() {
    thread_check_.Check();
    changed_ = true;
    set_.clear();
    SetReadiness(false);
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
    thread_check_.Check();
    changed_ = false; // Reset flag before draining events, if it was set

    for (auto it = set_.begin(); !changed_ && it != set_.end(); ) {
      T t(std::move(*it));
      it = set_.erase(it);
      if (!this->DrainOne(std::move(t))) {
        break;
      }
    }
    changed_ = false; // It is safe to operate on the set again

    if (set_.empty()) {
      SetReadiness(false);
    }
  }

  void SetReadiness(bool value) {
    if (ready_ == value) {
      return;
    }

    ready_ = value;
    if (value) {
      read_ready_fd_.write_event(1);
    } else {
      eventfd_t v = 0;
      read_ready_fd_.read_event(&v);
      RS_ASSERT(v > 0);
    }
  }

  ThreadCheck thread_check_;
  rocketspeed::port::Eventfd read_ready_fd_;
  std::unordered_set<T> set_;
  bool changed_;
  bool ready_;
};

} // namespace rocketspeed
