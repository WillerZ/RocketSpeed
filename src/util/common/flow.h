// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <vector>

#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"

namespace rocketspeed {

class EventCallback;
class EventLoop;

/**
 * Represents a source of any data.
 */
class AbstractSource : public NonMovable, public NonCopyable {
 public:
  virtual ~AbstractSource() {}

  /**
   * Enable or disable the read callback.
   *
   * @param event_loop The EventLoop to enable/disable on.
   * @param enabled Should callback be enabled or disabled.
   */
  virtual void SetReadEnabled(EventLoop* event_loop, bool enabled) = 0;
};

/**
 * Represents a sink for data.
 */
class AbstractSink : public NonMovable, public NonCopyable {
 public:
  virtual ~AbstractSink() {}
};

/** A source of messages of type T */
template <typename T>
class Source : public AbstractSource {
 public:
  virtual ~Source() {}

  /**
   * Registers a callback to invoke on read availability.
   *
   * @param event_loop The EventLoop to invoke the callback on.
   * @param read_callback The callback to invoke when reads are available.
   *                      Should return false when unable to process more.
   */
  void RegisterReadCallback(EventLoop* event_loop,
                            std::function<bool(T)> read_callback) {
    RS_ASSERT(!read_callback_);  // cannot register more than one.
    read_callback_ = std::move(read_callback);
    RegisterReadEvent(event_loop);
  }

 protected:
  virtual void RegisterReadEvent(EventLoop* event_loop) = 0;

  bool DrainOne(T item) {
    return read_callback_(std::move(item));
  }

 private:
  std::function<bool(T)> read_callback_;
};

/** A sink for messages of type T. */
template <typename T>
class Sink : public AbstractSink {
 public:
  virtual ~Sink() {}

  /**
   * Writes to the sink. When returning false, the value was successfully
   * written, but the writer should avoid writing more data otherwise the
   * pending data will grow without bound. Using FlowControl will avoid this.
   *
   * @param value The value to write (will be moved).
   * @return true iff more data can be written.
   */
  virtual bool Write(T& value) = 0;

  /**
   * Attempts to flush pending writes.
   *
   * @return true iff all pending writes were flushed.
   */
  virtual bool FlushPending() = 0;

  /**
   * Creates an EventCallback on event_loop that will invoke callback when
   * the sink is available for writing. The EventCallback is initially
   * disabled.
   *
   * @param event_loop The EventLoop to install the callback on.
   * @param callback The callback to invoke.
   * @return The EventCallback.
   */
  virtual std::unique_ptr<EventCallback> CreateWriteCallback(
    EventLoop* event_loop,
    std::function<void()> callback) = 0;
};

/**
 * A sink which automatically handles overflow.
 *
 * Writes that are not accepted by TryWrite method are queued in a std::deque
 * and flushed opportunistically.
 */
template <typename T>
class SinkWithOverflow : public Sink<T> {
 public:
  virtual ~SinkWithOverflow() = default;

  bool Write(T& value) final override {
    if (FlushPending() && TryWrite(value)) {
      return true;
    }
    overflow_.emplace_back(std::move(value));
    return false;
  }

  bool FlushPending() final override {
    for (; !overflow_.empty(); overflow_.pop_front()) {
      if (!TryWrite(overflow_.front())) {
        return false;
      }
    }
    return true;
  }

 protected:
  /**
   * Attempt to write to the sink.
   *
   * @param value The value to write (will be moved iff written).
   * @return true iff written.
   */
  virtual bool TryWrite(T& value) = 0;

 private:
  std::deque<T> overflow_;
};

} // namespace rocketspeed
