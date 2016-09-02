// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/flow_control.h"
#include "src/messages/event_loop.h"

namespace rocketspeed {

void FlowControl::RemoveBackpressure(AbstractSink* sink) {
  event_loop_->ThreadCheck();
  SinkState& sink_state = sinks_[sink];
  for (auto disabled_source : sink_state.backpressure) {
    auto source_it = sources_.find(disabled_source);
    // If the source was unregistered, while a backpressure was applied to it,
    // the list of sources might contain dangling pointer to it.
    if (source_it == sources_.end()) {
      continue;
    }
    SourceState& source_state = source_it->second;
    RS_ASSERT(source_state.blockers > 0);
    if (--source_state.blockers == 0) {
      // No more sinks blocking source, so re-enable.
      disabled_source->SetReadEnabled(event_loop_, true);
    }
  }
  stats_.backpressure_lifted->Add(sink_state.backpressure.size());
  sink_state.backpressure.clear();
}

void FlowControl::UnregisterSource(AbstractSource* source) {
  thread_check_.Check();
  source->SetReadEnabled(event_loop_, false);
  sources_.erase(source);
  // Pointers to the source stored in SinkState will be removed lazily, when
  // the backpressure from respective sink is removed.
}

void FlowControl::UnregisterSink(AbstractSink* sink) {
  thread_check_.Check();
  auto it = sinks_.find(sink);
  if (it != sinks_.end()) {
    RemoveBackpressure(sink);
    sinks_.erase(it);
  }
}

}  // namespace rocketspeed
