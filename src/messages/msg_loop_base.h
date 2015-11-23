// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <chrono>

#include "src/messages/commands.h"
#include "src/messages/event_loop.h"
#include "src/messages/messages.h"
#include "src/messages/serializer.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/base_env.h"
#include "src/util/common/statistics.h"
#include "src/util/mutexlock.h"
#include "src/port/Env.h"

namespace rocketspeed {

class Flow;

/**
 * Type of an application callback that is invoked with meassage of appropriate
 * type (depending on what type the callback was registered for), and the
 * origin of the message.
 */
typedef std::function<void(Flow*, std::unique_ptr<Message>, StreamID)>
    MsgCallbackType;

/**
 * An interface of message passing style communication between components,
 * processes and machines.
 */
class MsgLoopBase {
 public:

  explicit MsgLoopBase(BaseEnv* env)
  : env_(env) {
  }

  MsgLoopBase() = delete;
  MsgLoopBase(const MsgLoopBase&) = delete;
  MsgLoopBase(MsgLoopBase&&) = delete;
  MsgLoopBase& operator=(const MsgLoopBase&) = delete;
  MsgLoopBase& operator=(MsgLoopBase&&) = delete;

  virtual ~MsgLoopBase() = default;

  // Registers callbacks for a number of message types.
  virtual void RegisterCallbacks(const std::map<MessageType,
                                 MsgCallbackType>& callbacks) = 0;

  virtual Status RegisterTimerCallback(TimerCallbackType callback,
                                     std::chrono::microseconds period) = 0;

  /**
   * Initializes all event loops.
   * After this point, commands can be sent, but they will not be processed
   * until the message loop begins to run (assuming Initialize succeeded).
   *
   * @return ok() if successful, otherwise error.
   */
  virtual Status Initialize() = 0;

  // Start this instance of the Event Loop
  virtual void Run(void) = 0;

  // Is the MsgLoop up and running?
  virtual bool IsRunning() const = 0;

  // Stop the message loop.
  virtual void Stop() = 0;

  /**
   * Returns a new outbound socket. Returned socket is closed (not yet opened)
   * and its stream is bound to given worker thread of the message loop.
   * @param worker_id An ID of a worker that this stream will be assigned to.
   * @return A brand new stream socket.
   */
  virtual StreamSocket CreateOutboundStream(HostId destination,
                                            int worker_id) = 0;

  /**
   * Sends a command to a specific event loop for processing.
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
   * Sends a request message to a recipient.
   * If this request is the first on on a stream represented by the socket,
   * communication on a stream will be initiated. Requests to the same stream
   * sent from the same thread will be ordered with respect to each other.
   * This call is thread-safe.
   *
   * @param msg The message to send to the recipient.
   * @param socket A socket for the stream.
   * @param worker_id The index of the event loop that should process request.
   * @return OK if enqueued,
   *         NoBuffer if queue is full.
   */
  virtual Status SendRequest(const Message& msg,
                             StreamSocket* socket,
                             int worker_id) = 0;

  /**
   * Sends a response message on a given stream. Responses to the same stream
   * sent from the same thread will be ordered with respect to each other.
   * If no messages have been received from the recipient, the recipient has
   * sent a goodbye message, or the stream broke, then the response will be
   * dropped silently.
   * This call is thread-safe.
   *
   * @param msg The message to send to the recipient.
   * @param stream Stream that this message belongs to.
   * @param worker_id The index of the worker that received a matching request.
   * @return OK if enqueued,
   *         NoBuffer if queue is full.
   */
  virtual Status SendResponse(const Message& msg,
                              StreamID stream,
                              int worker_id) = 0;

  using WorkerStatsProvider = std::function<Statistics(int)>;

  /**
   * Call to aggregate a set of statistics provided by the
   * worker stats provider using the gather pattern. Waits
   * until the gather call finishes and thus is expensive.
   */
  Statistics AggregateStatsSync(WorkerStatsProvider stats_provider);

  virtual Statistics GetStatisticsSync() = 0;

  // Checks that we are running on any EventLoop thread.
  virtual void ThreadCheck() const = 0;

  // Retrieves the number of EventLoop threads.
  virtual int GetNumWorkers() const = 0;

  // Get the worker ID of the least busy event loop.
  virtual int LoadBalancedWorkerId() const = 0;

  // Retrieves the worker ID for the currently running thread.
  virtual int GetThreadWorkerIndex() const = 0;

  // Get the size of the message queue for worker_id.
  virtual size_t GetQueueSize(int worker_id) const = 0;

  /**
   * Waits until the message loop has run, or failed to start. If the loop
   * started and has subsequently stopped, the status will still be OK().
   *
   * @param time Maximum time to wait.
   * @return OK if loop successfully started, error otherwise.
   */
  virtual Status WaitUntilRunning(std::chrono::seconds timeout =
                                    std::chrono::seconds(10)) = 0;

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

 protected:
  BaseEnv* env_;
};

template <typename PerWorkerFunc, typename GatherFunc>
Status MsgLoopBase::Gather(PerWorkerFunc per_worker, GatherFunc gather) {
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

template <typename MapFunc, typename ReduceFunc, typename Result>
Status MsgLoopBase::MapReduceSync(MapFunc map,
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
Status MsgLoopBase::WorkerRequestSync(RequestFunc request,
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
  MsgLoopThread(BaseEnv* env, MsgLoopBase* msg_loop, std::string name)
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
  MsgLoopBase* msg_loop_;
  BaseEnv::ThreadId tid_;
};

}  // namespace rocketspeed
