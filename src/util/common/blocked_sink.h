/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <functional>
#include <memory>

#include "src/messages/event_callback.h"
#include "src/util/common/flow.h"

namespace rocketspeed {

class EventLoop;

/// A Sink that is never writable.
///
/// Useful for blocking a Source of events temporarily on something, when the
/// actual Sink could not be determined yet.
template <typename T>
class BlockedSink : public Sink<T> {
  bool Write(T&) final override { return false; }

  bool FlushPending() final override { return false; }

  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) final override {
    class DeadCallback : public EventCallback {
      void Enable() final override {}
      void Disable() final override {}
    };
    return std::make_unique<DeadCallback>();
  }
};

}  // namespace rocketspeed
