// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include "include/Assert.h"
#include "src/messages/event_loop.h"
#include "src/util/common/flow.h"
#include "src/util/memory.h"

namespace rocketspeed {

class FlowControl;

/**
 * Interface to the flow of message. To enable flow control, writes to sinks
 * from a source must be written using this interface.
 */
class Flow {
 public:
  /**
   * Writes value to sink. If the sink requests back-off then backpressure
   * will be applied from sink to source of the flow. The write never fails,
   * but may be delayed until the sink has space.
   *
   * @param sink The sink to write the value to.
   * @param value The value to write to the sink.
   * @return false iff the write was delayed due to backpressure.
   */
  template <typename T>
  bool Write(Sink<T>* sink, T& value);

  /** @return true iff a write has failed */
  bool WriteHasFailed() const {
    return write_failed_;
  }

 protected:
  friend class FlowControl;

  Flow(FlowControl* flow_control, AbstractSource* source)
  : flow_control_(flow_control)
  , source_(source)
  , write_failed_(false) {}

  FlowControl* const flow_control_;
  AbstractSource* const source_;
  bool write_failed_;
};

/**
 * Constructs a sourceless flow of messages. This can be used when the flow
 * interface is needed but backpressure will be handled by other means.
 */
class SourcelessFlow : public Flow {
 public:
  explicit SourcelessFlow(FlowControl* flow_control)
  : Flow(flow_control, nullptr) {}
};

class FlowControl {
 public:
  /**
   * Constructs FlowControl over an EventLoop.
   *
   * @param stats_prefix Prefix for flow control statistics.
   * @param event_loop The EventLoop to register processors with.
   */
  explicit FlowControl(const std::string& stats_prefix, EventLoop* event_loop)
  : event_loop_(event_loop)
  , stats_(stats_prefix) {
  }

  /**
   * Registers a callback to be invoked when a source is ready for reading.
   * The callback may be disabled when backpressure is applied to the source
   * from any blocked sinks.
   *
   * @param source The source to read from.
   * @param on_read Callback to be invoked when source has data to read.
   */
  template <typename T>
  void Register(Source<T>* source, std::function<void(Flow*, T)> on_read) {
    thread_check_.Check();
    auto read_cb = [this, on_read, source](T item) {
      // When source is read available, drain it into on_read until backpressure
      // is applied.
      Flow flow{this, source};
      on_read(&flow, std::move(item));
      return !flow.WriteHasFailed();
    };
    // Register the read callback for this source, and enable it.
    source->RegisterReadCallback(event_loop_, std::move(read_cb));
    source->SetReadEnabled(event_loop_, true);
  }

  template <typename T>
  void Unregister(Source<T>* source) {
    thread_check_.Check();
    sources_.erase(source);
    // Pointers to the source stored in SinkState will be removed lazily, when
    // the backpressure from respective sink is removed.
  }

  const Statistics& GetStatistics() const {
    return stats_.all;
  }

 private:
  friend class Flow;

  /**
   * Writes value to sink. If the sink requests back-off then backpressure
   * will be applied from sink to source. The write never fails, but may be
   * delayed until the sink has space.
   * If provided source is null, the Sink is automatically flushed after it
   * becomes writable.
   *
   * @param source The source of the value.
   * @param sink The sink to write the value to.
   * @param value The value to write to the sink.
   * @return false iff the write was delayed due to backpressure.
   */
  template <typename T>
  bool Write(AbstractSource* source, Sink<T>* sink, T& value) {
    event_loop_->ThreadCheck();
    if (!sink->Write(value)) {
      // If the sink was blocked by some source, then apply backpressure to the
      // source to prevent additional writes along this path.
      if (source) {
        ApplyBackpressure(sink, source);
      }
      // Enable an event to notify us when the sink is available again for
      // writes.
      GetSinkWriteEvent(sink)->Enable();
      return false;
    }
    return true;
  }

  /**
   * Lazily creates and returns the write available EventCallback for a sink.
   */
  template <typename T>
  std::unique_ptr<EventCallback>& GetSinkWriteEvent(Sink<T>* sink) {
    event_loop_->ThreadCheck();

    SinkState& sink_state = sinks_[sink];
    if (!sink_state.write_event) {
      // Install write event.
      sink_state.write_event =
        sink->CreateWriteCallback(
          event_loop_,
          [this, sink] () mutable {
            // Invoked when sink is ready to write again.
            // First, write any pending writes.
            if (sink->FlushPending()) {
              // Pending writes all written, so we can open up all the sources
              // that caused the backpressure.
              RemoveBackpressure(sink);
            }
          });
    }
    return sink_state.write_event;
  }

  /**
   * Applies backpressure from sink to source. The EventLoop will stop
   * processing source until sink is unblocked.
   *
   * @param sink The sink that is requesting backpressure.
   * @param source The source that needs to back off.
   */
  template <typename T>
  void ApplyBackpressure(Sink<T>* sink, AbstractSource* source) {
    RS_ASSERT(source);
    event_loop_->ThreadCheck();

    // Backpressure is achieved by first disabling read events from the source.
    // This relieves pressure on the down stream sinks.
    // Next, we add an event to detect when the sink has room to accept more
    // writes. When this event fires, we re-anble all source that were
    // blocked by the sink (assuming the sink actually has room).
    SinkState& sink_state = sinks_[sink];
    SourceState& source_state = sources_[source];

    // Disable events from the source that caused the write.
    source->SetReadEnabled(event_loop_, false);

    // Add this source as one that will be re-enabled on the sink write event.
    auto result = sink_state.backpressure.emplace(source);

    // If this is a new source for this sink, increase the blocker count.
    // The same sink may be blocked by the same source in fan-out cases where
    // a single read from a source causes multiple writes to a single sink.
    if (result.second) {
      stats_.backpressure_applied->Add(1);
      source_state.blockers++;
    }
  }

  /**
   * Does the opposite of ApplyBackpressure. Removes this sink as a blocker
   * of all sources, and re-enables the source if there are no more blockers.
   *
   * @param sink The sink to remove backpressure of.
   */
  void RemoveBackpressure(AbstractSink* sink);

  struct SinkState {
    // Set of sources that the sink is blocking with backpressure.
    std::unordered_set<AbstractSource*> backpressure;

    // EventCallback handle triggered when the sink has space available.
    std::unique_ptr<EventCallback> write_event;
  };

  struct SourceState {
    // Number of sinks blocking this source.
    uint32_t blockers = 0;
  };

  EventLoop* event_loop_;
  ThreadCheck thread_check_;
  std::unordered_map<AbstractSink*, SinkState> sinks_;
  std::unordered_map<AbstractSource*, SourceState> sources_;

  struct Stats {
    explicit Stats(std::string prefix) {
      prefix += ".flow_control.";
      backpressure_applied =
        all.AddCounter(prefix + "backpressure_applied");
      backpressure_lifted =
        all.AddCounter(prefix + "backpressure_lifted");
    }

    Statistics all;
    Counter* backpressure_applied;
    Counter* backpressure_lifted;
  } stats_;
};

template <typename T>
bool Flow::Write(Sink<T>* sink, T& value) {
  RS_ASSERT(flow_control_);
  write_failed_ |= !flow_control_->Write(source_, sink, value);
  return !write_failed_;
}

}
