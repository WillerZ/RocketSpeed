// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/flow_control.h"
#include "src/messages/event_loop.h"

namespace rocketspeed {

FlowControl::FlowControl(const std::string& stats_prefix, EventLoop* event_loop)
: event_loop_(event_loop)
, info_log_(event_loop->GetLog())
, stats_(stats_prefix) {
}

void FlowControl::RemoveBackpressure(AbstractSink* sink) {
  event_loop_->ThreadCheck();
  SinkState& sink_state = sinks_[sink];
  for (auto disabled_source : sink_state.backpressure) {
    auto source_it = sources_.find(disabled_source);
    RS_ASSERT(source_it != sources_.end());

    SourceState& source_state = source_it->second;
    RS_ASSERT(!source_state.blockers.empty());
    auto r = source_state.blockers.erase(sink);
    RS_ASSERT(r) << "sink '" << sink->GetSinkName() << "' has backpressure on "
      "source '" << disabled_source->GetSourceName() << "' not blocked by sink";
    if (source_state.blockers.empty()) {
      // No more sinks blocking source, so re-enable.
      disabled_source->SetReadEnabled(event_loop_, true);
    }
  }
  stats_.backpressure_lifted->Add(sink_state.backpressure.size());
  sink_state.backpressure.clear();

  LOG_WARN(info_log_, "Backpressure removed from sink '%s'",
    sink->GetSinkName().c_str());
}

void FlowControl::UnregisterSource(AbstractSource* source) {
  event_loop_->ThreadCheck();
  source->SetReadEnabled(event_loop_, false);
  auto it = sources_.find(source);
  if (it != sources_.end()) {
    // Source is going away, so remove from sink maps.
    for (AbstractSink* sink : it->second.blockers) {
      auto sink_it = sinks_.find(sink);
      RS_ASSERT(sink_it != sinks_.end());
      if (sink_it != sinks_.end()) {
        auto r = sink_it->second.backpressure.erase(source);
        RS_ASSERT(r) << "source '" << source->GetSourceName() << "' blocked by "
          "sink '" << sink->GetSinkName() << "' without backpressure on source";
      }
    }
    sources_.erase(it);
  }
}

void FlowControl::UnregisterSink(AbstractSink* sink) {
  event_loop_->ThreadCheck();
  auto it = sinks_.find(sink);
  if (it != sinks_.end()) {
    RemoveBackpressure(sink);
    sinks_.erase(it);
  }
}

}  // namespace rocketspeed
