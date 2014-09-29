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
  assert(msgloop);
  assert(msg);

  // what message have we received?
  MessageType type = msg->GetMessageType();
  Log(InfoLogLevel::INFO_LEVEL, msgloop->info_log_,
      "Received message %d at port %d", type, msgloop->hostid_.port);

  // Search for a callback method corresponding to this msg type
  // Give up ownership of this message to the callback function
  std::map<MessageType, MsgCallbackType>::const_iterator iter =
    msgloop->msg_callbacks_.find(type);
  if (iter != msgloop->msg_callbacks_.end()) {
    MsgCallbackType cb = iter->second;

    // special handling of ping message
    if (type == mPing) {
      cb(static_cast<ApplicationCallbackContext>(msgloop), std::move(msg));
    } else {
      cb(msgloop->application_context_, std::move(msg));
    }
  } else {
    // If the user has not registered a message of this type,
    // then this msg will be droped silently.
    Log(InfoLogLevel::WARN_LEVEL, msgloop->info_log_,
        "No registered msg callback for msg type %d", type);
    msgloop->info_log_->Flush();
    assert(0);
  }
}

/**
 * Constructor for a Message Loop
 */
MsgLoop::MsgLoop(const Env* env,
                 const EnvOptions& env_options,
                 const HostId& hostid,
                 const std::shared_ptr<Logger>& info_log,
                 const ApplicationCallbackContext application_context,
                 const std::map<MessageType, MsgCallbackType>& callbacks,
                 CommandCallbackType command_callback):
  env_(env),
  env_options_(env_options),
  hostid_(hostid),
  info_log_(info_log),
  application_context_(application_context),
  msg_callbacks_(SanitizeCallbacks(callbacks)),
  event_loop_(hostid.port, info_log, EventCallback, command_callback),
  client_(env, env_options, info_log) {
  // set the Event callback context
  event_loop_.SetEventCallbackContext(this);

  // log an informational message
  Log(InfoLogLevel::INFO_LEVEL, info_log,
      "Created a new Message Loop at port %d with %d callbacks",
      hostid_.port, msg_callbacks_.size());
}

void MsgLoop::Run() {
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "Starting Message Loop at port %d", hostid_.port);
  event_loop_.Run();
}

void MsgLoop::Stop() {
  event_loop_.Stop();
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
    "Stopped a Message Loop at port %d", hostid_.port);
  info_log_->Flush();
}

MsgLoop::~MsgLoop() {
  Stop();
}

//
// This is the system's handling of the ping message.
// Applications can override this behaviour if desired.
void
MsgLoop::ProcessPing(const ApplicationCallbackContext mloop,
                     std::unique_ptr<Message> msg) {
  // This was a system generated message, so the application
  // context is the msg loop itself.
  MsgLoop* msgloop = static_cast<MsgLoop*>(mloop);

  // get the ping request message
  MessagePing* request = static_cast<MessagePing*>(msg.get());
  const HostId origin = request->GetOrigin();

  // change it to a ping response message
  request->SetPingType(MessagePing::Response);

  // send reponse
  Status st = msgloop->GetClient().Send(origin, std::move(msg));

  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, msgloop->info_log_,
        "Unable to send ping response to %s:%d",
        origin.hostname.c_str(), origin.port);
  } else {
    Log(InfoLogLevel::INFO_LEVEL, msgloop->info_log_,
        "Send ping response to %s:%d",
        origin.hostname.c_str(), origin.port);
  }
  msgloop->info_log_->Flush();
}

//
// Create a new copy of the callback map.
// Add a ping handler to the callbacks if not already set
//
std::map<MessageType, MsgCallbackType>
MsgLoop::SanitizeCallbacks(const std::map<MessageType,
                           MsgCallbackType>& cb) {
  // make a copy
  std::map<MessageType, MsgCallbackType> newmap(cb);

  // add Ping handler
  if (newmap.find(MessageType::mPing) == newmap.end()) {
    newmap[MessageType::mPing] = MsgCallbackType(ProcessPing);
  }
  return newmap;
}

}  // namespace rocketspeed
