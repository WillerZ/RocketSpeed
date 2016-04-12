/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

#include "src/proxy2/abstract_worker.h"

namespace rocketspeed {

class EventLoop;
class ProxyServerOptions;

/// Accepts inbound streams and routes them to appropriate upstream worker.
///
/// The layer of DownstreamWorkers is sharded by a subscriber that connects to
/// the proxy. From the perspective of the proxy's operation this layer does
/// not exist. It is only needed to improve topic locality. No important
/// proxy-related logic happens here.
class DownstreamWorker : public AbstractWorker, public StreamReceiver {
 public:
  DownstreamWorker(const ProxyServerOptions& options, EventLoop* event_loop);

  void ReceiveFromQueue(Flow* flow,
                        size_t inbound_id,
                        MessageAndStream elem) override;

  /// a.k.a. ReceiveFromStream
  void operator()(StreamReceiveArg<Message> arg) override;

 private:
  /// Maps a stream to an index of an upstream worker that should handle it.
  std::unordered_map<StreamID, size_t> stream_to_upstream_worker_;

  void CleanupState(StreamID stream_id);
};

}  // namespace rocketspeed
