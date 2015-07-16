// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <map>
#include <memory>

#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop_base.h"
#include "src/port/Env.h"
#include "src/util/common/base_env.h"
#include "src/util/common/thread_local.h"

namespace rocketspeed {

typedef std::function<void()> TimerCallbackType;

class StreamAllocator;
class EventLoop;
class Logger;

class MsgLoop : public MsgLoopBase {
 public:
  /**
    * Options is a helper class used for passing the additional arguments to the
    * MsgLoop constructor.
    */
  class Options {
   public:
    // the options used for constructing the underlying event loop. will get
    // modified within the constructor.
    EventLoop::Options event_loop;
  };

  // Create a listener to receive messages on a specified port.
  // When a message arrives, invoke the specified callback.
  MsgLoop(BaseEnv* env,
          const EnvOptions& env_options,
          int port,
          int num_workers,
          const std::shared_ptr<Logger>& info_log,
          std::string name,
          Options options = Options());

  virtual ~MsgLoop();

  // Registers callbacks for a number of message types.
  void RegisterCallbacks(
    const std::map<MessageType, MsgCallbackType>& callbacks) override;

  // Register the timer callback at the givne period. Must be called after Init.
  Status RegisterTimerCallback(TimerCallbackType callback,
                             std::chrono::microseconds period) override;

  Status Initialize() override;

  // Start this instance of the Event Loop
  void Run() override;

  // Is the MsgLoop up and running?
  bool IsRunning() const override {
    for (const auto& event_loop : event_loops_) {
      if (!event_loop->IsRunning()) {
        return false;
      }
    }
    return true;
  }

  // Stop the message loop.
  void Stop() override;

  // Get the host ID of this message loop.
  const HostId& GetHostId() const { return hostid_; }

  // Get the name of this message loop.
  const std::string& GetName() const {
    return name_;
  }

  /**
   * Returns stream ID allocator used by given event loop to create outbound
   * streams.
   *
   * @param worker_id Index of the event loop.
   * @return Stream allocator which represents outbound stream ID space.
   */
  StreamAllocator* GetOutboundStreamAllocator(int worker_id);

  StreamSocket CreateOutboundStream(HostId destination,
                                    int worker_id) override;

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

  Status TrySendCommand(std::unique_ptr<Command>& command,
                        int worker_id) override;

  Status SendRequest(const Message& msg,
                     StreamSocket* socket,
                     int worker_id) override;

  Status SendResponse(const Message& msg,
                      StreamID stream,
                      int worker_id) override;

  std::unique_ptr<Command> RequestCommand(const Message& msg,
                                          StreamSocket* socket);

  std::unique_ptr<Command> ResponseCommand(const Message& msg,
                                           StreamID stream);

  Statistics GetStatisticsSync() override;

  // Checks that we are running on any EventLoop thread.
  void ThreadCheck() const override {
    GetThreadWorkerIndex();
  }

  // Retrieves the number of EventLoop threads.
  int GetNumWorkers() const override {
    return static_cast<int>(event_loops_.size());
  }

  // Get the worker ID of the least busy event loop.
  int LoadBalancedWorkerId() const override;

  // Retrieves the worker ID for the currently running thread.
  // Will assert if called from a non-EventLoop thread.
  int GetThreadWorkerIndex() const override;

  size_t GetQueueSize(int worker_id) const override;

  Status WaitUntilRunning(std::chrono::seconds timeout =
                            std::chrono::seconds(10)) override;

  /**
   * Synchronously finds the total number of active clients on each event
   * loop. Will block until all event loops are able to asynchronously process
   * the request.
   *
   * @return The total number of clients.
   */
  int GetNumClientsSync();

  /**
   * Creates a new queue that will be read by a worker loop.
   *
   * @param worker_id The worker to read from this queue.
   * @param size Size of the queue (number of commands). Defaults to whatever
   *             the EventLoop default command queue size is.
   * @return The created queue.
   */
  std::shared_ptr<CommandQueue> CreateCommandQueue(int worker_id,
                                                   size_t size = 0);

  /**
   * Creates a vector of command queues, one for each worker.
   *
   * @param size Size of the queue (number of commands). Defaults to whatever
   *             the EventLoop default command queue size is.
   * @return The created queue vector.
   */
  std::vector<std::shared_ptr<CommandQueue>>
    CreateWorkerQueues(size_t size = 0);

  /**
   * Creates a logical set of queues from each thread, to a particular worker.
   * The queues are created on demand for each thread.
   */
  std::unique_ptr<ThreadLocalCommandQueues>
    CreateThreadLocalQueues(int worker_id, size_t size = 0);

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

  /** External synchronisation for getting sockets. */
  std::mutex stream_allocation_mutex_;

  // Looping counter to distribute load on the message loop.
  mutable std::atomic<int> next_worker_id_;

  // The EventLoop callback.
  void EventCallback(std::unique_ptr<Message> msg, StreamID origin);

  // method to provide default handling of ping message
  void ProcessPing(std::unique_ptr<Message> msg, StreamID origin);
  std::map<MessageType, MsgCallbackType> SanitizeCallbacks(
                  const std::map<MessageType, MsgCallbackType>& cb);
};

}  // namespace rocketspeed
