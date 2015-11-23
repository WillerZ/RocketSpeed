// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <functional>
#include <memory>

#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class EventLoop;

/** An ID of a trigger, unique within an EventLoop. */
using TriggerID = uint64_t;

/**
 * A utility to notify arbitrary number of EventCallbacks on the same EventLoop
 * as the notifying thread. It is safe for registered EventCallback or Trigger,
 * that has any number of registered events to be destroyed at any time.
 */
class EventTrigger {
 public:
  explicit EventTrigger(TriggerID id) : id_(id) {}

  TriggerID id_;
};

class EventCallback : public NonMovable, public NonCopyable {
 public:
  /**
   * Creates an EventCallback that will be invoked when fd becomes readable.
   * cb will be invoked on the event_loop thread.
   *
   * @param event_loop The EventLoop to add the event to.
   * @param fd File descriptor to listen for reads.
   * @param cb Callback to invoke when fd is readable.
   * @return EventCallback object.
   */
  static std::unique_ptr<EventCallback> CreateFdReadCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb);

  /**
   * Creates an EventCallback that will be invoked when fd becomes writable.
   * cb will be invoked on the event_loop thread.
   *
   * @param event_loop The EventLoop to add the event to.
   * @param fd File descriptor to listen for writes.
   * @param cb Callback to invoke when fd is writable.
   * @return EventCallback object.
   */
  static std::unique_ptr<EventCallback> CreateFdWriteCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb);

  virtual ~EventCallback() {};

  /**
   * Enables the event.
   * Can be called from the thread that would execute the callback only.
   */
  virtual void Enable() = 0;

  /**
   * Disables the event.
   * Can be called from the thread that would execute the callback only.
   */
  virtual void Disable() = 0;
};

}  // namespace rocketspeed
