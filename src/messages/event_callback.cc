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

namespace {

class FdCallback : public EventCallback {
 public:
  FdCallback(EventLoop* event_loop, std::function<void()> cb)
  : event_loop_(event_loop), cb_(std::move(cb)), enabled_(false) {}

  ~FdCallback() {
    if (event_) {
      event_free(event_);
    }
  }

  void Enable() final override {
    event_loop_->ThreadCheck();
    if (!enabled_) {
      if (event_add(event_, nullptr)) {
        exit(137);
      }
      enabled_ = true;
    }
  }

  void Disable() final override {
    event_loop_->ThreadCheck();
    if (enabled_) {
      if (event_del(event_)) {
        exit(137);
      }
      enabled_ = false;
    }
  }

 private:
  friend class EventCallback;

  static void Invoke(int fd, short what, void* event) {
    assert(event);
    if (what & (EV_READ | EV_WRITE)) {
      auto fd_event = static_cast<FdCallback*>(event);
      fd_event->event_loop_->ThreadCheck();
      fd_event->cb_();
    }
  }

  EventLoop* event_loop_;
  event* event_;
  std::function<void()> cb_;
  bool enabled_;
};

}  // namespace

std::unique_ptr<EventCallback> EventCallback::CreateFdReadCallback(
    EventLoop* event_loop, int fd, std::function<void()> cb) {
  auto callback = new FdCallback(event_loop, std::move(cb));
  callback->event_ =
      event_loop->CreateFdReadEvent(fd, &FdCallback::Invoke, callback);
  return std::unique_ptr<FdCallback>(callback);
}

std::unique_ptr<EventCallback> EventCallback::CreateFdWriteCallback(
    EventLoop* event_loop, int fd, std::function<void()> cb) {
  auto callback = new FdCallback(event_loop, std::move(cb));
  callback->event_ =
      event_loop->CreateFdWriteEvent(fd, &FdCallback::Invoke, callback);
  return std::unique_ptr<FdCallback>(callback);
}

}  // namespace rocketspeed
