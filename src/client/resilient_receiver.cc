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
    ConnectionStatusCb connection_status_cb,
    BackOffStrategy backoff_strategy,
    size_t max_silent_reconnects)
: event_loop_(event_loop)
, receiver_(receiver)
, connection_status_cb_(std::move(connection_status_cb))
, backoff_strategy_(std::move(backoff_strategy))
, max_silent_reconnects_(max_silent_reconnects) {}

void ResilientStreamReceiver::ConnectTo(const HostId& host) {
  if (current_host_ == host) {
    return;
  }
  Logger* logger = event_loop_->GetLog().get();
  LOG_INFO(logger, "ReconnectTo(%s)", host.ToString().c_str());

  sequential_conn_failures_ = 0;
  current_host_ = host;
  Reconnect();
}

void ResilientStreamReceiver::operator()(StreamReceiveArg<Message> arg) {
  NotifyConnectionHealthy(arg.message->GetMessageType() !=
                          MessageType::mGoodbye);

  if (arg.message->GetMessageType() == MessageType::mGoodbye) {
    Reconnect();
  }

  // forward the message to delegate
  (*receiver_)(std::move(arg));
}

bool ResilientStreamReceiver::NotifyConnectionHealthy(bool isHealthy) {
  size_t notify_after = max_silent_reconnects_ + 1;
  bool previously_notified_failure = sequential_conn_failures_ >= notify_after;

  if (isHealthy && previously_notified_failure) {
    connection_status_cb_(isHealthy);
  }

  sequential_conn_failures_ = isHealthy ? 0 : sequential_conn_failures_ + 1;

  if (sequential_conn_failures_ == notify_after) {
    connection_status_cb_(isHealthy);
  }

  return isHealthy;
}

void ResilientStreamReceiver::Reconnect() {
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
  if (!current_host_) {
    LOG_INFO(logger, "Reconnect(): unknown host");
    return;
  }

  auto reconnect = [this, logger]() {
    RS_ASSERT(current_host_);

    // Open the stream.
    auto stream = event_loop_->OpenStream(current_host_);
    if (!stream) {
      return NotifyConnectionHealthy(false);
    }
    stream->SetReceiver(this);
    LOG_INFO(logger,
             "Reconnect()::reconnect(%s, %lld)",
             current_host_.ToString().c_str(),
             stream->GetLocalID());

    // Kill the backoff timer, if set.
    backoff_timer_.reset();

    receiver_->ConnectionEstablished(std::move(stream));
    return true;
  };

  // We do not apply backoff if there have been no recorded connection failure
  // to this host.
  if (sequential_conn_failures_ == 0) {
    if (reconnect()) {
      return;
    }
  }

  RS_ASSERT(sequential_conn_failures_ > 0);
  // Figure out the backoff.
  auto backoff_duration =
      backoff_strategy_(&ThreadLocalPRNG(), sequential_conn_failures_ - 1);

  LOG_INFO(logger,
           "Reconnect()::backoff(%zu, %zd)",
           sequential_conn_failures_,
           backoff_duration.count());

  // Schedule asynchronous creation of a new stream.
  RS_ASSERT(!backoff_timer_);
  backoff_timer_ =
      event_loop_->CreateTimedEventCallback(reconnect, backoff_duration);
  backoff_timer_->Enable();
}
}
