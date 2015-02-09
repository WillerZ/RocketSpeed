// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <atomic>
#include <map>
#include <memory>

#include "src/messages/commands.h"
#include "src/messages/serializer.h"
#include "src/messages/messages.h"
#include "src/messages/event_loop.h"
#include "src/util/common/base_env.h"
#include "src/util/common/statistics.h"
#include "src/port/Env.h"


namespace rocketspeed {

// Application callback are invoked with messages of this type
typedef std::function<void(std::unique_ptr<Message> msg)> MsgCallbackType;

//
class MsgLoopBase {
 public:

  MsgLoopBase(){
  };

  virtual ~MsgLoopBase() {
  };

  // Register callback for a command in all underlying EventLoops.
  virtual void RegisterCommandCallback(CommandType type,
                                       CommandCallbackType callback) = 0;

  // Registers callbacks for a number of message types.
  virtual void RegisterCallbacks(const std::map<MessageType,
                                 MsgCallbackType>& callbacks) = 0;

  // Start this instance of the Event Loop
  virtual void Run(void) = 0;

  // Is the MsgLoop up and running?
  virtual bool IsRunning() const = 0;

  // Stop the message loop.
  virtual void Stop() = 0;

  // The client ID of a specific event loop.
  virtual const ClientID& GetClientId(int worker_id) const = 0;

  // Send a command to a specific event loop for processing.
  // This call is thread-safe.
  virtual Status SendCommand(std::unique_ptr<Command> command,
                             int worker_id) = 0;

  virtual Statistics GetStatistics() const = 0;

  // Checks that we are running on any EventLoop thread.
  virtual void ThreadCheck() const = 0;

  // Retrieves the number of EventLoop threads.
  virtual int GetNumWorkers() const = 0;

  // Get the worker ID of the least busy event loop.
  virtual int LoadBalancedWorkerId() const = 0;

  // Retrieves the worker ID for the currently running thread.
  virtual int GetThreadWorkerIndex() const = 0;

  // Checks that the message origin matches this worker loop.
  virtual bool CheckMessageOrigin(const Message* msg) = 0;
};

}  // namespace rocketspeed
