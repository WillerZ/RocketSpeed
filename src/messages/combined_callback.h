// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <functional>

#include "src/messages/event_callback.h"

namespace rocketspeed {

class EventLoop;

typedef std::function<std::unique_ptr<EventCallback>(std::function<void()>)>
  CreateEvent;

/**
 * A combination of two EventCallbacks.
 * The callback only gets invoked if both the callbacks set the event_state
 * to true.
 *
 * Note: The "event_state" is coupled with the class using CombinedCallback
 * to let CombinedCallback know which callback(s) to enable and what is the
 * current state of the callback.
 *
 * event_state->first = true; means the first callback is in an ok state to
 * invoke the callback, similarly for event_state->second;
 *
 * event_state->first = false; means that first callback has not yet completed
 * and is in an active state, similarly for event_state->second;
 *
 * The custom callbacks are responsible for setting the event_state to true
 * when completed and invoking the callback.
 */
class CombinedCallback : public EventCallback {
 public:

  /**
   * CombinedCallback Constructor.
   *
   * @param event_loop the event_loop to run the callback on.
   * @param cb the callback to be invoked.
   * @param event_state the state of the callbacks
   * @param create_event_first function to create the first_event
   *        which takes custom callback as a parameter
   * @param create_event_second function to create the second_event
   *        which takes custom callback as a parameter
   */
  CombinedCallback(
    EventLoop* event_loop, std::function<void()> cb,
    std::shared_ptr<std::pair<bool, bool>> event_state,
    CreateEvent create_event_first, CreateEvent create_event_second);

  ~CombinedCallback() {};

  void Enable() final override;

  void Disable() final override;

  void Invoke() {
    // Invoke the callback only when both the event callbacks sets
    // event_state to true.
    if (event_state_->first && event_state_->second) {
      cb_();
    }
  }

 private:
  EventLoop* event_loop_;
  std::function<void()> cb_;
  std::shared_ptr<std::pair<bool, bool>> event_state_;
  std::unique_ptr<EventCallback> first_event_;
  std::unique_ptr<EventCallback> second_event_;
};

} // namespace rocketspeed
