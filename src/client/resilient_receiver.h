/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include "include/RocketSpeed.h"
#include "src/messages/event_loop.h"
#include "src/messages/types.h"

#include <chrono>

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
 *
 * We also take a connection status callback. This is used to advise
 * the client when a connection is unhealthy. We make
 * max_silent_reconnects before advising the client that the
 * connection is bad. The callback is invoked with a healthy status on
 * every healthy message that is delivered.
 */
class ResilientStreamReceiver final : public StreamReceiver {
 public:
  using HealthStatusCb = std::function<void(bool isHealthy)>;

  /**
   * Constructor.
   *
   * @param event_loop Event loop to use.
   * @param receiver Decorated delegate.
   * @param health_status_cb Notify this when the connection is
   * detected as unhealthy or on each healthy message received.
   * @param backoff_strategy Determines the backoff period.
   * @param max_silent_reconnects Reconnect this many times before
   * @param shard_id shard id for the stream receiver
   * @param stream_descriptor Properties descriptor of the stream
   * notifying the connection_status_cb.
   */
  ResilientStreamReceiver(
      EventLoop* event_loop,
      ConnectionAwareReceiver* receiver,
      HealthStatusCb health_status_cb,
      BackOffStrategy backoff_strategy,
      size_t max_silent_reconnects,
      size_t shard_id,
      std::shared_ptr<const IntroParameters> intro_parameters =
          std::make_shared<const IntroParameters>());

  /// Establish communication to the provided host. This must be
  /// called at least once. Calling this at any point is allowed and
  /// will force a reconnection.
  void ConnectTo(const HostId& host);

  /// Returns host that we're connecting or connected to.
  /// Returns HostId() before ConnectTo() is called.
  const HostId& GetCurrentHost() const {
    return current_host_;
  }

 private:
  EventLoop* const event_loop_;
  ConnectionAwareReceiver* const receiver_;
  const HealthStatusCb health_status_cb_;
  const BackOffStrategy backoff_strategy_;
  const size_t max_silent_reconnects_;
  const size_t shard_id_;
  std::shared_ptr<const IntroParameters> intro_parameters_;

  HostId current_host_;
  std::unique_ptr<EventCallback> backoff_timer_;
  size_t sequential_conn_failures_{0};

  virtual void operator()(StreamReceiveArg<Message> arg) override;
  virtual void NotifyHealthy(bool isHealthy) override {
    receiver_->NotifyHealthy(isHealthy); // just forward
    NotifyConnectionHealthy(isHealthy);
  }

  void CreateConnection(const HostId& host);
  void Reconnect(size_t conn_failures, HostId host);
  size_t UpdateConnectionState(bool isNowHealthy);

  void NotifyConnectionHealthy(bool currentState) {
    health_status_cb_(currentState);
  }
};
}
