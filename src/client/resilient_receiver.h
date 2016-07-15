/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include "include/RocketSpeed.h"
#include "src/messages/event_loop.h"
#include "src/messages/types.h"

namespace rocketspeed {

/**
 * A decorating stream receiver that takes responsibility for
 * supervising the connection. It constructs a stream and
 * automatically backs off and tries to construct a new one on
 * disconnect.
 *
 * This decorates another stream receiver forwarding all message
 * events. It also notifies the delegate of (dis)connection events. On
 * creating a new connection, it passes ownership of the stream to the
 * delegate. The delegate should not reassign the receiver on the
 * stream!
 */
class ResilientStreamReceiver final : public StreamReceiver {
 public:
  ResilientStreamReceiver(EventLoop* event_loop,
                          ConnectionAwareReceiver* receiver,
                          BackOffStrategy backoff_strategy);

  /// Establish communication to the provided host. This must be
  /// called at least once. Calling this at any point is allowed and
  /// will force a reconnectioh.
  void ConnectTo(const HostId& host);

 private:
  EventLoop* const event_loop_;
  ConnectionAwareReceiver* const receiver_;
  const BackOffStrategy backoff_strategy_;

  HostId current_host_;
  std::unique_ptr<EventCallback> backoff_timer_;
  size_t connection_failures_{0};

  virtual void operator()(StreamReceiveArg<Message> arg) override;
  void Reconnect();
};
}
