//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/triggerable_callback.h"

#include "src/messages/event_loop.h"

namespace rocketspeed {

TriggerableCallback::~TriggerableCallback() {
  event_loop_->TriggerableCallbackClose(access::EventLoop(), this);
}

void TriggerableCallback::Enable() {
  enabled_ = true;
  event_loop_->TriggerableCallbackEnable(access::EventLoop(), this);
}

void TriggerableCallback::Disable() {
  enabled_ = false;
  event_loop_->TriggerableCallbackDisable(access::EventLoop(), this);
}

}  // namespace rocketspeed
