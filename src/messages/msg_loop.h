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
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/auto_roll_logger.h"

namespace rocketspeed {

typedef std::function<void(std::unique_ptr<Message> msg)> MsgCallbackType;

class MsgLoop {
 public:
  // Create a listener to receive messages on a specified port.
  // When a message arrives, invoke the specified callback.
  MsgLoop(int port,
          const std::shared_ptr<Logger>& info_log,
          const std::map<MessageType, MsgCallbackType>& callbacks);

  virtual ~MsgLoop();

  // Start this instance of the Event Loop
  void Run(void) { event_loop_.Run(); }

  // Is the MsgLoop up and running?
  bool IsRunning() { return event_loop_.IsRunning(); }

 private:
  // the port nuber of
  int port_number_;

  // debug message go here
  const std::shared_ptr<Logger> info_log_;

  // The callbacks specified by the application
  const std::map<MessageType, MsgCallbackType> msg_callbacks_;

  // The underlying Eventloop callback handler
  EventLoop event_loop_;

  // The static method registered with the EventLoop
  static void EventCallback(EventCallbackContext ctx,
                            std::unique_ptr<Message> msg);
};

}  // namespace rocketspeed
