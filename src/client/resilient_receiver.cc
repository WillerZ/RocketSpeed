/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.

#include "src/client/resilient_receiver.h"
#include "src/messages/stream.h"
#include "src/util/common/random.h"

namespace rocketspeed {

ResilientStreamReceiver::ResilientStreamReceiver(
    EventLoop* event_loop,
    ConnectionAwareReceiver* receiver,
    HealthStatusCb health_status_cb,
    BackOffStrategy backoff_strategy,
    size_t max_silent_reconnects)
: event_loop_(event_loop)
, receiver_(receiver)
, health_status_cb_(std::move(health_status_cb))
, backoff_strategy_(std::move(backoff_strategy))
, max_silent_reconnects_(max_silent_reconnects) {}

void ResilientStreamReceiver::ConnectTo(const HostId& host) {
  if (current_host_ == host) {
    return;
  }

  Logger* logger = event_loop_->GetLog().get();
  LOG_INFO(logger, "ReconnectTo(%s)", host.ToString().c_str());

  current_host_ = host;
  Reconnect(0, host);
}

void ResilientStreamReceiver::operator()(StreamReceiveArg<Message> arg) {
  auto sequential_failures = UpdateConnectionState(
      arg.message->GetMessageType() != MessageType::mGoodbye);

  if (sequential_failures > 0) {
    Reconnect(sequential_failures, current_host_);
  }

  // forward the message to delegate
  (*receiver_)(std::move(arg));
}

size_t ResilientStreamReceiver::UpdateConnectionState(bool isNowHealthy) {
  if (isNowHealthy) {
    NotifyConnectionHealthy(isNowHealthy);
  }

  sequential_conn_failures_ = isNowHealthy ? 0 : sequential_conn_failures_ + 1;

  size_t notify_after = max_silent_reconnects_ + 1;
  if (sequential_conn_failures_ == notify_after) {
    RS_ASSERT(!isNowHealthy);
    NotifyConnectionHealthy(isNowHealthy);
  }

  return sequential_conn_failures_;
}

void ResilientStreamReceiver::Reconnect(size_t conn_failures, HostId host) {
  // Think about this method as something that can be called in almost arbitrary
  // state and should leave the receiver in a perfectly valid state.
  // Code over here should be rather defensive. Corner cases should be severely
  // underrepresented.
  receiver_->ConnectionDropped();

  Logger* logger = event_loop_->GetLog().get();

  // Kill any scheduled reconnection.
  backoff_timer_.reset();

  // If no remote host is known, we're done. The procedure will be repeated
  // after router update.
  if (!host) {
    LOG_INFO(logger, "Reconnect(): unknown host");
    return;
  }

  auto reconnect = [this, host, logger]() {
    RS_ASSERT(host);

    // Open the stream.
    auto stream = event_loop_->OpenStream(host);
    if (!stream) {
      auto failures = UpdateConnectionState(false);
      Reconnect(failures, host);
      return false;
    }
    stream->SetReceiver(this);
    LOG_INFO(logger,
             "Reconnect()::reconnect(%s, %lld)",
             host.ToString().c_str(),
             stream->GetLocalID());

    // Kill the backoff timer, if set.
    backoff_timer_.reset();

    receiver_->ConnectionCreated(std::move(stream));
    return true;
  };

  // We do not apply backoff if there have been no recorded connection failure
  // to this host.
  if (conn_failures == 0) {
    if (reconnect()) {
      return;
    }
  }

  RS_ASSERT(conn_failures > 0);
  // Figure out the backoff.
  auto backoff_duration =
      backoff_strategy_(&ThreadLocalPRNG(), conn_failures - 1);

  LOG_INFO(logger,
           "Reconnect()::backoff(%zu, %zd)",
           conn_failures,
           backoff_duration.count());

  // Schedule asynchronous creation of a new stream.
  RS_ASSERT(!backoff_timer_);
  backoff_timer_ =
      event_loop_->CreateTimedEventCallback(reconnect, backoff_duration);
  backoff_timer_->Enable();
}
}
