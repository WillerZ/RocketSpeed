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
typedef std::function<void(std::unique_ptr<Message>)> MsgCallbackType;

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

  /**
   * Send a command to a specific event loop for processing.
   *
   * This call is thread-safe.
   *
   * @param command The command to send for processing.
   * @param worker_id The index of the worker thread.
   * @return OK if enqueued.
   *         NoBuffer if queue is full.
   */
  virtual Status SendCommand(std::unique_ptr<Command> command,
                             int worker_id) = 0;

  /**
   * Sends a request message to a recipient. The message will be sent to an
   * unspecified event loop to dispatch, so will not be ordered with respect to
   * other requests. If this is the first message sent to the recipient,
   * communication will be initiated.
   *
   * @param msg The message to send to the recipient.
   * @param recipient Client to send message to.
   * @return OK if enqueued.
   *         NoBuffer if queue is full.
   */
  Status SendRequest(const Message& msg, ClientID recipient) {
    return SendRequest(msg, std::move(recipient), LoadBalancedWorkerId());
  }

  /**
   * Sends a request message to a recipient via a specific event loop specified
   * by worker_id. Requests on the same worker_id from the same thread will
   * be ordered with respect to each other. If this is the first message sent
   * to the recipient, communication will be initiated.
   *
   * @param msg The message to send to the recipient.
   * @param recipient Client to send message to.
   * @param worker_id The index of the worker thread.
   * @return OK if enqueued.
   *         NoBuffer if queue is full.
   */
  Status SendRequest(const Message& msg, ClientID recipient, int worker_id) {
    return SendMessage(msg, std::move(recipient), worker_id, true);
  }

  /**
   * Sends a response message to a recipient via a specific event loop
   * specified by worker_id. Responses on the same worker_id from the same
   * thread will be ordered with respect to each other. If no messages have
   * been received from the recipient (or the recipient has sent a goodbye
   * message) then the response will asyncronously fail to send.
   *
   * @param msg The message to send to the recipient.
   * @param recipient Client to send message to.
   * @param worker_id The index of the worker thread.
   * @return OK if enqueued.
   *         NoBuffer if queue is full.
   */
  Status SendResponse(const Message& msg, ClientID recipient, int worker_id) {
    return SendMessage(msg, std::move(recipient), worker_id, false);
  }

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

  /**
   * Waits until the message loop has run, or failed to start. If the loop
   * started and has subsequently stopped, the status will still be OK().
   *
   * @param time Maximum time to wait.
   * @return OK if loop successfully started, error otherwise.
   */
  virtual Status WaitUntilRunning(std::chrono::seconds timeout =
                                    std::chrono::seconds(10)) = 0;

 private:
  // Sends msg to recipient on event loop worker_id.
  virtual Status SendMessage(const Message& msg,
                             ClientID recipient,
                             int worker_id,
                             bool is_new_request) = 0;
};

}  // namespace rocketspeed
