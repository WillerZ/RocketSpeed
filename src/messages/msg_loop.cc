//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/messages/msg_loop.h"
#include "src/messages/serializer.h"
#include "src/messages/messages.h"

namespace rocketspeed {

//
// This is registered with the event loop. The event loop invokes
// this call on every message received.
void
MsgLoop::EventCallback(EventCallbackContext ctx,
                       std::unique_ptr<Message> msg) {
  // find the MsgLoop that we are working for
  MsgLoop* msgloop = static_cast<MsgLoop*> (ctx);

  // what message have we received?
  MessageType type = msg->GetMessageType();

  // Search for a callback method corresponding to this msg type
  // Give up ownership of this message to the callback function
  std::map<MessageType, MsgCallbackType>::const_iterator iter =
    msgloop->msg_callbacks_.find(type);
  if (iter != msgloop->msg_callbacks_.end()) {
    MsgCallbackType cb = iter->second;
    cb(std::move(msg));
  } else {
    // If the user has not registered a message of this type,
    // then this msg will be droped silently.
    assert(0);
  }
}

/**
 * Constructor for a Message Loop
 */
MsgLoop::MsgLoop(int port_number,
                 const std::shared_ptr<Logger>& info_log,
                 const std::map<MessageType, MsgCallbackType>& callbacks):
  port_number_(port_number),
  info_log_(info_log),
  msg_callbacks_(callbacks),
  event_loop_(port_number, info_log, EventCallback) {
  // set the Event callback context
  event_loop_.SetEventCallbackContext(this);

  // log an informational message
  Log(InfoLogLevel::INFO_LEVEL, info_log,
      "Created a new Event Loop");
}

MsgLoop::~MsgLoop() {
}
}  // namespace rocketspeed
