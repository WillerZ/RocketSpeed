//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>

#include "src/messages/event_callback.h"

namespace rocketspeed {

class EventLoop;

class TriggerableCallback : public EventCallback {
 public:
  TriggerableCallback(EventLoop* event_loop,
                      TriggerID trigger_id,
                      std::function<void()> cb)
  : event_loop_(event_loop)
  , trigger_id_(trigger_id)
  , cb_(std::move(cb))
  , enabled_(false) {}

  ~TriggerableCallback();

  void Enable() final override;

  void Disable() final override;

  void Invoke() const {
    assert(enabled_);
    cb_();
  }

  bool IsEnabled() const { return enabled_; }

  TriggerID GetTriggerID() const { return trigger_id_; }

 private:
  EventLoop* const event_loop_;
  const TriggerID trigger_id_;
  const std::function<void()> cb_;
  bool enabled_;
};

}  // namespace rocketspeed
