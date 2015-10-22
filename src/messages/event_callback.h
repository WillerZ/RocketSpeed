// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>

struct event;

namespace rocketspeed {

class EventLoop;

class EventCallback {
 public:
  ~EventCallback();

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

  // non-copyable, non-moveable
  EventCallback(const EventCallback&) = delete;
  EventCallback(EventCallback&&) = delete;
  EventCallback& operator=(const EventCallback&) = delete;
  EventCallback& operator=(EventCallback&&) = delete;

  /** Invokes the callback */
  void Invoke();

  /** Enables the event */
  void Enable();

  /** Disables the event */
  void Disable();

  /** @return true iff currently enabled. */
  bool IsEnabled() const {
    return enabled_;
  }

 private:
  explicit EventCallback(EventLoop* event_loop, std::function<void()> cb);

  EventLoop* event_loop_;
  event* event_;
  std::function<void()> cb_;
  bool enabled_;
};

}  // namespace rocketspeed
