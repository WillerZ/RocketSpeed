// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Assert.h"
#include "src/messages/event_callback.h"
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

  explicit ObservableSet(EventLoop* event_loop)
  : event_loop_(event_loop)
  , read_ready_(event_loop->CreateEventTrigger())
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
    RS_ASSERT(event_loop == event_loop_);
    read_callback_ = event_loop->CreateEventCallback(
        [this]() { this->Drain(); }, read_ready_);
  }

  void SetReadEnabled(EventLoop* event_loop, bool enabled) final override {
    RS_ASSERT(event_loop == event_loop_);
    if (enabled) {
      read_callback_->Enable();
    } else {
      read_callback_->Disable();
    }
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
      event_loop_->Notify(read_ready_);
    } else {
      event_loop_->Unnotify(read_ready_);
    }
  }

  ThreadCheck thread_check_;
  EventLoop* event_loop_;
  EventTrigger read_ready_;
  std::unique_ptr<EventCallback> read_callback_;
  std::unordered_set<T> set_;
  bool changed_;
  bool ready_;
};

} // namespace rocketspeed
