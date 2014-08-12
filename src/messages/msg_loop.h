// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <inttypes.h>
#include <sys/types.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/thread.h>
#include <event2/util.h>
#include <map>
#include "include/Env.h"
#include "src/messages/serializer.h"
#include "src/messages/messages.h"
#include "src/messages/event_loop.h"
#include "src/messages/msg_client.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/auto_roll_logger.h"

namespace rocketspeed {

// The first parameter of callbacks specified by the application are
// of this type. It allows an application to get back its own context
// when invoked as part of a callback.
typedef void* ApplicationCallbackContext;

// Application callback are invoked with messages of this type
typedef std::function<void(const ApplicationCallbackContext,
                           std::unique_ptr<Message> msg)> MsgCallbackType;

class MsgLoop {
 public:
  // Create a listener to receive messages on a specified port.
  // When a message arrives, invoke the specified callback.
  MsgLoop(const Env* env,
          const EnvOptions& env_options,
          const HostId& hostid,
          const std::shared_ptr<Logger>& info_log,
          const ApplicationCallbackContext application_context,
          const std::map<MessageType, MsgCallbackType>& callbacks);

  virtual ~MsgLoop();

  // Start this instance of the Event Loop
  void Run(void);

  // Is the MsgLoop up and running?
  bool IsRunning() const { return event_loop_.IsRunning(); }

  // returns a client that is used to send messages to remote hosts
  MsgClient& GetClient() { return client_; }

 private:
  // The Environment
  const Env* env_;

  // The Environment Options
  const EnvOptions env_options_;

  // the host/port number of this Msg Loop
  HostId hostid_;

  // debug message go here
  const std::shared_ptr<Logger> info_log_;

  // The application context that is passed back to app on every callback
  const ApplicationCallbackContext application_context_;

  // The callbacks specified by the application
  const std::map<MessageType, MsgCallbackType> msg_callbacks_;

  // The underlying Eventloop callback handler
  EventLoop event_loop_;

  // A client that is used to respond to system Ping messages
  MsgClient client_;

  // The static method registered with the EventLoop
  static void EventCallback(EventCallbackContext ctx,
                            std::unique_ptr<Message> msg);

  // static methods to provide default hadling of ping message
  static void ProcessPing(const ApplicationCallbackContext ctx,
                          std::unique_ptr<Message> msg);
  static std::map<MessageType, MsgCallbackType> SanitizeCallbacks(
                  const std::map<MessageType, MsgCallbackType>& cb);
};

}  // namespace rocketspeed
