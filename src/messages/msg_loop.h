// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <vector>

#include "include/BaseEnv.h"
#include "include/Env.h"
#include "include/HostId.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/queues.h"
#include "src/messages/stream_allocator.h"
#include "src/util/common/thread_local.h"

namespace rocketspeed {

typedef std::function<void()> TimerCallbackType;

/**
 * Type of an application callback that is invoked with meassage of appropriate
 * type (depending on what type the callback was registered for), and the
 * origin of the message.
 */
typedef std::function<void(Flow*, std::unique_ptr<Message>, StreamID)>
    MsgCallbackType;

class EventCallback;
class EventLoop;
class Flow;
class Logger;
class StreamAllocator;
class LoadBalancer;

class MsgLoop {
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
          std::string stats_prefix,
          Options options = Options());

  virtual ~MsgLoop();

  // Registers callbacks for a number of message types.
  void RegisterCallbacks(
    const std::map<MessageType, MsgCallbackType>& callbacks);

  // Register the timer callback at the givne period. Must be called after Init.
  Status RegisterTimerCallback(TimerCallbackType callback,
                               std::chrono::microseconds period);

  Status Initialize();

  // Start this instance of the message loop
  void Run();

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
  const HostId& GetHostId() const { return event_loops_[0]->GetHostId(); }

  // Get the stats prefix of this message loop.
  const std::string& GetStatsPrefix() const {
    return stats_prefix_;
  }

  /// Returns a function that maps stream IDs, both inbound and outbound, used
  /// by this loop, into worker IDs.
  const StreamAllocator::DivisionMapping& GetStreamMapping() {
    return stream_mapping_;
  }

  /// Returns stream ID allocator used by given event loop to create outbound
  /// streams.
  ///
  /// @param worker_id Index of the event loop.
  /// @return Stream allocator which represents outbound stream ID space.
  StreamAllocator* GetOutboundStreamAllocator(int worker_id);

  StreamSocket CreateOutboundStream(HostId destination,
                                    int worker_id);

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

  Status SendCommand(std::unique_ptr<Command> command,
                     int worker_id);

  void SendControlCommand(std::unique_ptr<Command> command,
                          int worker_id);

  template <typename TMsg>
  Status SendRequest(TMsg msg, StreamSocket* socket, int worker_id);

  template <typename TMsg>
  Status SendResponse(TMsg msg, StreamID stream, int worker_id);

  template <typename TMsg>
  Status SendRequest(TMsg msg, StreamSocket* socket);

  template <typename TMsg>
  Status SendResponse(TMsg msg, StreamID stream);

  template <typename TMsg>
  static std::unique_ptr<Command> RequestCommand(TMsg msg,
                                                 StreamSocket* socket);

  template <typename TMsg>
  static std::unique_ptr<Command> ResponseCommand(TMsg msg,
                                                  StreamID stream);

  Statistics GetStatisticsSync();

  // Checks that we are running on any EventLoop thread.
  void ThreadCheck() const {
    GetThreadWorkerIndex();
  }

  EventLoop* GetEventLoop(int worker_id) {
    RS_ASSERT(worker_id < GetNumWorkers());
    return event_loops_[worker_id].get();
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

  size_t GetQueueSize(int worker_id) const;

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

  using WorkerStatsProvider = std::function<Statistics(int)>;

  /**
   * Call to aggregate a set of statistics provided by the
   * worker stats provider using the gather pattern. Waits
   * until the gather call finishes and thus is expensive.
   */
  Statistics AggregateStatsSync(WorkerStatsProvider stats_provider);

  /**
   * Asynchronously computes per_worker(worker_index) on each worker thread
   * then calls gather with the results from an unspecified worker thread.
   *
   * @param per_worker Will be called with each worker index and may return
   *                   a value of any type.
   * @param gather Function to call with a vector of the results of calling
   *               per_worker with each worker index.
   * @return ok() if successful, error otherwise.
   */
  template <typename PerWorkerFunc, typename GatherFunc>
  Status Gather(PerWorkerFunc per_worker, GatherFunc callback);

  /**
   * Asynchronously computes per_worker(worker_index) on each worker thread
   * then calls gather with the results from an unspecified worker thread.
   *
   * Note:
   * This function is made more reliable by using unbounded queues, where
   * writes will always succeed, but are prone to flow control issues.
   * It is intended for infrequent use such as initialization and statistics
   * gathering, and should _not_ be used for any large volume of messages.
   *
   * @param per_worker Will be called with each worker index and may return
   *                   a value of any type.
   * @param gather Function to call with a vector of the results of calling
   *               per_worker with each worker index.
   * This function never fails.
   */
  template <typename PerWorkerFunc, typename GatherFunc>
  void ReliableGather(PerWorkerFunc per_worker, GatherFunc callback);

  /**
   * Synchronous map reduce across workers. Calls map(w) on each worker thread
   * where w is the worker thread index, collects the results into a map then
   * assigns *out = reduce({map(0), map(1), ...}).
   *
   * @param map Mapping function to call on each worker.
   * @param reduce Reducing function, called with vector of results of map.
   * @param out Output for result of reduce.
   * @param timeout Timeout for getting all results.
   * @return ok() if out was written before timeout, otherwise error.
   */
  template <typename MapFunc, typename ReduceFunc, typename Result>
  Status MapReduceSync(MapFunc map,
                       ReduceFunc reduce,
                       Result* out,
                       std::chrono::seconds timeout =
                         std::chrono::seconds(5));

  /**
   * Essentially performs `*out = request()` on the specified worker with a
   * timeout. *out will only be set if the result is ok().
   *
   * @param request Function to invoke on worker thread.
   * @param worker_id Index of worker thread to invoke on.
   * @param out Output parameter for result.
   * @param timeout Timeout for request.
   * @return ok() if successfully invoked, otherwise error.
   */
  template <typename RequestFunc, typename Result>
  Status WorkerRequestSync(RequestFunc request,
                           int worker_id,
                           Result* out,
                           std::chrono::seconds timeout =
                             std::chrono::seconds(5));

 private:
  void SetThreadWorkerIndex(int worker_index);

  BaseEnv* env_;

  // Stores index of the worker for this thread.
  // Reading this is only valid within an EventLoop callback. It is used to
  // define affinities between workers and messages.
  ThreadLocalPtr worker_index_;

  // The Environment Options
  const EnvOptions env_options_;

  // debug message go here
  const std::shared_ptr<Logger> info_log_;

  // The callbacks specified by the application
  std::map<MessageType, MsgCallbackType> msg_callbacks_;

  // The underlying EventLoop callback handlers, and threads.
  std::vector<std::unique_ptr<EventLoop>> event_loops_;
  std::vector<Env::ThreadId> worker_threads_;

  // Name of the message loop.
  // Used for stats and thread naming.
  std::string stats_prefix_;

  /// Represents the full space of stream IDs, both inbound and outbound, used
  /// by this loop.
  StreamAllocator stream_allocator_;
  /// Tells the mapping from a stream to the worker responsible for it.
  StreamAllocator::DivisionMapping stream_mapping_;

  /** External synchronisation for getting sockets. */
  std::mutex stream_allocation_mutex_;

  /** Start timer callback objects **/
  std::vector<std::unique_ptr<rocketspeed::EventCallback>> timer_callbacks_;

  // The EventLoop callback.
  void EventCallback(Flow* flow, std::unique_ptr<Message> msg, StreamID origin);

  // method to provide default handling of ping message
  void ProcessPing(std::unique_ptr<Message> msg, StreamID origin);
  std::map<MessageType, MsgCallbackType> SanitizeCallbacks(
                  const std::map<MessageType, MsgCallbackType>& cb);

  // Load Balancer
  std::unique_ptr<LoadBalancer> load_balancer_;
};

template <typename PerWorkerFunc, typename GatherFunc>
Status MsgLoop::Gather(PerWorkerFunc per_worker, GatherFunc gather) {
  using T = decltype(per_worker(0));

  // Accumulated results and mutex for access.
  using Context = std::pair<port::Mutex, std::vector<T>>;
  auto context = std::make_shared<Context>();

  const int n = GetNumWorkers();
  context->second.reserve(n);
  for (int i = 0; i < n; ++i) {
    // Schedule the per_worker function to be called on each worker.
    // Results will be accumulated in context->second.
    std::unique_ptr<Command> command(
      MakeExecuteCommand([this, i, n, context, per_worker, gather] () {
        bool done;
        T result = per_worker(i);
        {
          MutexLock lock(&context->first);
          context->second.emplace_back(std::move(result));
          done = context->second.size() == n;
        }
        if (done) {
          // Don't need lock here since all workers are done.
          gather(std::move(context->second));
        }
      }));
    Status st = SendCommand(std::move(command), i);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

template <typename PerWorkerFunc, typename GatherFunc>
void MsgLoop::ReliableGather(PerWorkerFunc per_worker, GatherFunc gather) {
  using T = decltype(per_worker(0));

  // Accumulated results and mutex for access.
  using Context = std::pair<port::Mutex, std::vector<T>>;
  auto context = std::make_shared<Context>();

  const int n = GetNumWorkers();
  context->second.reserve(n);
  for (int i = 0; i < n; ++i) {
    // Schedule the per_worker function to be called on each worker.
    // Results will be accumulated in context->second.
    std::unique_ptr<Command> command(
      MakeExecuteCommand([this, i, n, context, per_worker, gather] () {
        bool done;
        T result = per_worker(i);
        {
          MutexLock lock(&context->first);
          context->second.emplace_back(std::move(result));
          done = context->second.size() == n;
        }
        if (done) {
          // Don't need lock here since all workers are done.
          gather(std::move(context->second));
        }
      }));
    SendControlCommand(std::move(command), i);
  }
}

template <typename MapFunc, typename ReduceFunc, typename Result>
Status MsgLoop::MapReduceSync(MapFunc map,
                                  ReduceFunc reduce,
                                  Result* out,
                                  std::chrono::seconds timeout) {
  RS_ASSERT(out);
  using T = decltype(map(0));

  // Request context.
  // Needs to be shared in case of timeout.
  auto done = std::make_shared<port::Semaphore>();
  auto result = std::make_shared<Result>();
  Status st =
    Gather(map,
           [reduce, result, done] (std::vector<T> results) {
             *result = reduce(results);
             done->Post();
           });
  if (!st.ok()) {
    return st;
  }
  if (!done->TimedWait(timeout)) {
    std::string msg = "Queue sizes = ";
    for (int i = 0; i < GetNumWorkers(); ++i) {
      msg += std::to_string(GetQueueSize(i));
      if (i != GetNumWorkers() - 1) {
        msg += + ", ";
      }
    }
    return Status::TimedOut(msg);
  }
  *out = std::move(*result);
  return Status::OK();
}

template <typename RequestFunc, typename Result>
Status MsgLoop::WorkerRequestSync(RequestFunc request,
                                      int worker_id,
                                      Result* out,
                                      std::chrono::seconds timeout) {
  RS_ASSERT(out);

  // Request context.
  // Needs to be shared in case of timeout.
  auto done = std::make_shared<port::Semaphore>();
  auto result = std::make_shared<Result>();
  std::unique_ptr<Command> command(
    MakeExecuteCommand([this, request, done, result] () {
      *result = request();
      done->Post();
    }));
  Status st = SendCommand(std::move(command), worker_id);
  if (!st.ok()) {
    return st;
  }
  if (!done->TimedWait(timeout)) {
    const size_t queue_size = GetQueueSize(worker_id);
    return Status::TimedOut("Queue size = " + std::to_string(queue_size));
  }
  *out = std::move(*result);
  return Status::OK();
}

class MsgLoopThread {
 public:
  MsgLoopThread(BaseEnv* env, MsgLoop* msg_loop, std::string name)
  : env_(env)
  , msg_loop_(msg_loop) {
    RS_ASSERT(env_);
    RS_ASSERT(msg_loop_);
    tid_ = env_->StartThread([msg_loop] () { msg_loop->Run(); }, name);
  }

  ~MsgLoopThread() {
    msg_loop_->Stop();
    if (tid_) {
      env_->WaitForJoin(tid_);
    }
  }

 private:
  BaseEnv* env_;
  MsgLoop* msg_loop_;
  BaseEnv::ThreadId tid_;
};

template <typename TMsg>
Status MsgLoop::SendRequest(TMsg msg, StreamSocket* socket, int worker_id) {
  // Create command and append it to the proper event loop.
  RS_ASSERT(event_loops_[worker_id]->IsOutboundStream(socket->GetStreamID()));
  Status st = SendCommand(RequestCommand(std::move(msg), socket), worker_id);
  if (st.ok()) {
    socket->Open();
  }
  return st;
}

template <typename TMsg>
Status MsgLoop::SendResponse(TMsg msg, StreamID stream, int worker_id) {
  // Create command and append it to the proper event loop.
  RS_ASSERT(event_loops_[worker_id]->IsInboundStream(stream));
  return SendCommand(ResponseCommand(std::move(msg), stream), worker_id);
}

template <typename TMsg>
Status MsgLoop::SendRequest(TMsg msg, StreamSocket* socket) {
  int worker_id = static_cast<int>(stream_mapping_(socket->GetStreamID()));
  return SendRequest(std::move(msg), socket, worker_id);
}

template <typename TMsg>
Status MsgLoop::SendResponse(TMsg msg, StreamID stream) {
  int worker_id = static_cast<int>(stream_mapping_(stream));
  return SendResponse(std::move(msg), stream, worker_id);
}

template <typename TMsg>
std::unique_ptr<Command> MsgLoop::RequestCommand(TMsg msg,
                                                 StreamSocket* socket) {
  return MessageSendCommand::Request(Message::Copy(std::move(msg)), {socket});
}

template <typename TMsg>
std::unique_ptr<Command> MsgLoop::ResponseCommand(TMsg msg,
                                                  StreamID stream) {
  return MessageSendCommand::Response(Message::Copy(std::move(msg)), {stream});
}

}  // namespace rocketspeed
