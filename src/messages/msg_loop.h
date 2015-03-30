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
#include "src/messages/msg_loop_base.h"
#include "src/messages/event_loop.h"
#include "src/util/common/base_env.h"
#include "src/util/common/thread_local.h"
#include "src/port/Env.h"

namespace rocketspeed {

class Logger;

class MsgLoop : public MsgLoopBase {
 public:
  // Create a listener to receive messages on a specified port.
  // When a message arrives, invoke the specified callback.
  MsgLoop(BaseEnv* env,
          const EnvOptions& env_options,
          int port,
          int num_workers,
          const std::shared_ptr<Logger>& info_log,
          std::string name,
          ClientID client_id = "");

  virtual ~MsgLoop();

  // Register callback for a command in all underlying EventLoops.
  void RegisterCommandCallback(CommandType type,
                               CommandCallbackType callback);

  // Registers callbacks for a number of message types.
  void
  RegisterCallbacks(const std::map<MessageType, MsgCallbackType>& callbacks);

  // Start this instance of the Event Loop
  void Run(void);

  // Is the MsgLoop up and running?
  bool IsRunning() const {
    for (const auto& event_loop : event_loops_) {
      if (!event_loop->IsRunning()) {
        return false;
      }
    }
    return true;
  }

  // Stop the message loop.
  void Stop();

  // Get the host ID of this message loop.
  const HostId& GetHostId() const { return hostid_; }

  // The client ID of a specific event loop.
  const ClientID& GetClientId(int worker_id) const {
    return worker_client_ids_[worker_id];
  }

  /**
   * Send a command to the event loop that the thread is currently running on.
   * Calling from non event loop thread has undefined behaviour.
   * This method might be implemented in such a way, that it processes command
   * inline, in which case it's rather easy to overflow stack if one wants to
   * call the method from execute command functor.
   *
   * @param command The command to send for processing.
   */
  void SendCommandToSelf(std::unique_ptr<Command> command);

  Status SendCommand(std::unique_ptr<Command> command, int worker_id) override;

  // TODO(stupaq) remove it once we get numeric sequence ids & allocators
  Status SendRequest(const Message& msg, ClientID recipient) {
    return SendRequest(msg, recipient, LoadBalancedWorkerId());
  }

  // TODO(stupaq) remove it once we get numeric sequence ids & allocators
  Status SendRequest(const Message& msg, ClientID recipient, int worker_id) {
    StreamSocket socket(recipient, GetClientId(worker_id) + '|' + recipient);
    return SendRequest(msg, &socket, worker_id);
  }

  Status SendRequest(const Message& msg,
                     StreamSocket* socket,
                     int worker_id) override;

  Status SendResponse(const Message& msg,
                      StreamID stream,
                      int worker_id) override;

  Statistics GetStatistics() const {
    Statistics stats;
    for (const auto& event_loop : event_loops_) {
      stats.Aggregate(event_loop->GetStatistics());
    }
    stats.Aggregate(stats_.all);
    return stats;
  }

  // Checks that we are running on any EventLoop thread.
  void ThreadCheck() const {
    GetThreadWorkerIndex();
  }

  // Retrieves the number of EventLoop threads.
  int GetNumWorkers() const {
    return static_cast<int>(event_loops_.size());
  }

  // Get the worker ID of the least busy event loop.
  int LoadBalancedWorkerId() const;

  // Retrieves the worker ID for the currently running thread.
  // Will assert if called from a non-EventLoop thread.
  int GetThreadWorkerIndex() const;

  // Checks that the message origin matches this worker loop.
  bool CheckMessageOrigin(const Message* msg);

  Status WaitUntilRunning(std::chrono::seconds timeout =
                            std::chrono::seconds(10));

  /**
   * Synchronously finds the total number of active clients on each event
   * loop. Will block until all event loops are able to asynchronously process
   * the request.
   *
   * @return The total number of clients.
   */
  int GetNumClientsSync();

 private:
  void SetThreadWorkerIndex(int worker_index);

  // Stores index of the worker for this thread.
  // Reading this is only valid within an EventLoop callback. It is used to
  // define affinities between workers and messages.
  ThreadLocalPtr worker_index_;

  // The Environment Options
  const EnvOptions env_options_;

  // the host/port number of this Msg Loop
  HostId hostid_;

  // debug message go here
  const std::shared_ptr<Logger> info_log_;

  // The callbacks specified by the application
  std::map<MessageType, MsgCallbackType> msg_callbacks_;

  // The underlying EventLoop callback handlers, and threads.
  std::vector<std::unique_ptr<EventLoop>> event_loops_;
  std::vector<Env::ThreadId> worker_threads_;

  // Name of the message loop.
  // Used for stats and thread naming.
  std::string name_;

  // Looping counter to distribute load on the message loop.
  mutable std::atomic<int> next_worker_id_;

  // Client ID per event loop.
  std::unique_ptr<ClientID[]> worker_client_ids_;

  struct Stats {
    explicit Stats(const std::string& prefix) {
      bad_origin = all.AddCounter(prefix + ".bad_origin");
    }

    Statistics all;
    Counter* bad_origin;  // number of messages with bad origin
  } stats_;

  // The EventLoop callback.
  void EventCallback(std::unique_ptr<Message> msg);

  // method to provide default handling of ping message
  void ProcessPing(std::unique_ptr<Message> msg);
  std::map<MessageType, MsgCallbackType> SanitizeCallbacks(
                  const std::map<MessageType, MsgCallbackType>& cb);
};

}  // namespace rocketspeed
