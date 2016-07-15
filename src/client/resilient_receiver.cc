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
    BackOffStrategy backoff_strategy)
: event_loop_(event_loop)
, receiver_(receiver)
, backoff_strategy_(backoff_strategy) {}

void ResilientStreamReceiver::ConnectTo(const HostId& host) {
  if (current_host_ == host) {
    return;
  }
  Logger* logger = event_loop_->GetLog().get();
  LOG_INFO(logger, "ReconnectTo(%s)", host.ToString().c_str());

  connection_failures_ = 0;
  current_host_ = host;
  Reconnect();
}

void ResilientStreamReceiver::operator()(StreamReceiveArg<Message> arg) {
  if (arg.message->GetMessageType() == MessageType::mGoodbye) {
    connection_failures_++;
    Reconnect();
  }

  // forward the message to delegate
  (*receiver_)(std::move(arg));
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
    stream->SetReceiver(this);
    LOG_INFO(logger,
             "Reconnect()::reconnect(%s, %lld)",
             current_host_.ToString().c_str(),
             stream->GetLocalID());

    // Kill the backoff timer, if set.
    backoff_timer_.reset();

    receiver_->ConnectionEstablished(std::move(stream));
  };

  // We do not apply backoff if there have been no recorded connection failure
  // to this host.
  if (connection_failures_ == 0) {
    reconnect();
  } else {
    RS_ASSERT(connection_failures_ > 0);
    // Figure out the backoff.
    auto backoff_duration =
        backoff_strategy_(&ThreadLocalPRNG(), connection_failures_ - 1);

    LOG_INFO(logger,
             "Reconnect()::backoff(%zu, %zd)",
             connection_failures_,
             backoff_duration.count());

    // Schedule asynchronous creation of a new stream.
    RS_ASSERT(!backoff_timer_);
    backoff_timer_ =
        event_loop_->CreateTimedEventCallback(reconnect, backoff_duration);
    backoff_timer_->Enable();
  }
}
}
