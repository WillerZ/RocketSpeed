//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "event_callback.h"

#include "src/messages/event2_version.h"
#include <event2/event.h>

#include "src/messages/event_loop.h"

namespace rocketspeed {

static void EventShim(int fd, short what, void* event) {
  assert(event);
  if (what & (EV_READ|EV_WRITE)) {
    static_cast<EventCallback*>(event)->Invoke();
  }
}

EventCallback::EventCallback(EventLoop* event_loop, std::function<void()> cb)
: event_loop_(event_loop)
, cb_(std::move(cb))
, enabled_(false) {
}

std::unique_ptr<EventCallback> EventCallback::CreateFdReadCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb) {
  std::unique_ptr<EventCallback> callback(
    new EventCallback(event_loop, std::move(cb)));
  callback->event_ =
    event_loop->CreateFdReadEvent(fd, &EventShim, callback.get());
  return callback;
}

std::unique_ptr<EventCallback> EventCallback::CreateFdWriteCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb) {
  std::unique_ptr<EventCallback> callback(
    new EventCallback(event_loop, std::move(cb)));
  callback->event_ =
    event_loop->CreateFdWriteEvent(fd, &EventShim, callback.get());
  return callback;
}

EventCallback::~EventCallback() {
  if (event_) {
    event_free(event_);
  }
}

void EventCallback::Invoke() {
  event_loop_->ThreadCheck();
  cb_();
}

void EventCallback::Enable() {
  event_loop_->ThreadCheck();
  if (!enabled_) {
    if (event_add(event_, nullptr)) {
      exit(137);
    }
    enabled_ = true;
  }
}

void EventCallback::Disable() {
  event_loop_->ThreadCheck();
  if (enabled_) {
    if (event_del(event_)) {
      exit(137);
    }
    enabled_ = false;
  }
}

}  // namespace
