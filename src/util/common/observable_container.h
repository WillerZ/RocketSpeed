/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <functional>
#include <memory>

#include "include/Assert.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/util/common/flow.h"

namespace rocketspeed {

/// A wrapper on top a STL container that reads (moves) elements from it and
/// implements Source interface.
///
/// The class tolerates modifications of the underlying container from the read
/// callback even if the underlying container does not have stable iterators.
/// The class is not thread-safe.
template <typename C, typename T = typename C::value_type>
class ObservableContainer : public Source<T> {
 public:
  using ContainerType = C;

  explicit ObservableContainer(EventLoop* event_loop)
  : ObservableContainer(event_loop, C()) {}

  explicit ObservableContainer(EventLoop* event_loop, C container)
  : event_loop_(event_loop)
  , container_(std::move(container))
  , read_ready_(event_loop->CreateEventTrigger())
  , changed_(false) {}

  const C* operator->() const { return &Read(); }

  /// Provides a const reference to the underlying container.
  ///
  /// Invoking this method does affect readability of the Source.
  const C& Read() const { return container_; }

  /// Provides a mutable reference to the underlying container to the functor.
  ///
  /// Readability of the Source is adjusted automatically after the opration,
  /// which may mutate the container in an arbitrary way, completes.
  template <typename Func>
  void Modify(Func&& func) {
    changed_ = true;
    func(container_);
    AdjustReadability();
  }

  void RegisterReadEvent(EventLoop* event_loop) final override {
    RS_ASSERT(event_loop == event_loop_);
    read_callback_ = event_loop->CreateEventCallback(
        std::bind(&ObservableContainer::Drain, this), read_ready_);
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
  EventLoop* const event_loop_;
  C container_;
  EventTrigger read_ready_;
  std::unique_ptr<EventCallback> read_callback_;
  bool changed_;

  void Drain() {
    bool changed_orig = changed_;
    changed_ = false;
    for (auto it = container_.begin(); !changed_ && it != container_.end();) {
      T t(std::move(*it));
      it = container_.erase(it);
      if (!this->DrainOne(std::move(t))) {
        break;
      }
    }
    // Restore the original value of the changed_ flag when unwinding the stack.
    changed_ = changed_orig;
    AdjustReadability();
  }

  void AdjustReadability() {
    if (container_.empty()) {
      event_loop_->Unnotify(read_ready_);
    } else {
      event_loop_->Notify(read_ready_);
    }
  }
};

}  // namespace rocketspeed
