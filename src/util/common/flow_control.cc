// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/flow_control.h"
#include "src/messages/event_loop.h"

namespace rocketspeed {

void FlowControl::RemoveBackpressure(AbstractSink* sink) {
  SinkState& sink_state = sinks_[sink];
  for (auto disabled_source : sink_state.backpressure) {
    SourceState& source_state = sources_[disabled_source];
    assert(source_state.blockers > 0);
    if (--source_state.blockers == 0) {
      // No more sinks blocking source, so re-enable.
      disabled_source->SetReadEnabled(event_loop_, true);
    }
  }
  sink_state.backpressure.clear();
  sink_state.write_event->Disable();
}

}
