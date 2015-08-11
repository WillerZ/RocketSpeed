// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <vector>

namespace rocketspeed {

class EventCallback;
class EventLoop;

/**
 * Represents a source of any data.
 */
class AbstractSource {
 public:
  virtual ~AbstractSource() {}

  /**
   * Enable or disable the read callback.
   *
   * @param enabled Should callback be enabled or disabled.
   */
  virtual void SetReadEnabled(bool enabled) const = 0;
};

/**
 * Represents a sink for data.
 */
class AbstractSink {
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
    assert(!read_callback_);  // cannot register more than one.
    read_callback_ = std::move(read_callback);
    RegisterReadEvent(event_loop);
  }

  /**
   * Reads from the source, and invokes read callback until it returns false.
   */
  virtual void Drain() = 0;

 protected:
  virtual void RegisterReadEvent(EventLoop* event_loop) = 0;

  bool DrainOne(T item) {
    return read_callback_(std::move(item));
  }

 private:
  std::function<bool(T)> read_callback_;
};

// A sink for messages of type T
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
   * @param check_thread Whether to do thead safety checks.
   * @return true iff more data can be written.
   */
  bool Write(T&& value, bool check_thread = true) {
    if (FlushPending(check_thread) && TryWrite(value, check_thread)) {
      return true;
    }
    overflow_.emplace_back(std::move(value));
    return false;
  }

  bool Write(T& value, bool check_thread = true) {
    return Write(std::move(value), check_thread);
  }

  /**
   * Attempt to write to the sink.
   *
   * @param value The value to write (will be moved iff written).
   * @param check_thread Whether to do thead safety checks.
   * @return true iff written.
   */
  virtual bool TryWrite(T& value, bool check_thread) = 0;

  /**
   * Attempts to flush pending writes.
   *
   * @return true iff all pending writes were flushed.
   */
  bool FlushPending(bool check_thread) {
    for (; !overflow_.empty(); overflow_.pop_front()) {
      if (!TryWrite(overflow_.front(), check_thread)) {
        return false;
      }
    }
    return true;
  }

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

 private:
  std::deque<T> overflow_;
};

}
