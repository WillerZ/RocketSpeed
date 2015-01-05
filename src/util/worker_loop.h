// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <utility>
#include "src/util/common/multi_producer_queue.h"
#include "src/port/port.h"
#include "src/port/Env.h"

namespace rocketspeed {

template <typename Command>
class WorkerLoop {
 public:
  /**
   * Constructs a WorkerLoop with a specific queue size.
   *
   * @param env Environment context
   * @param size The size of the worker queue. Due to the queue implementation,
   *             the maximum number of items in the queue will be size - 1.
   */
  explicit WorkerLoop(Env* env,
                      uint32_t size);

  /**
   * Destroys the worker loop, and waits for it to stop.
   */
  ~WorkerLoop();

  /**
   * Runs the worker loop, consuming commands from the queue until Stop().
   *
   * @param callback Will be called for each command.
   */
  void Run(std::function<void(Command cmd)> callback);

  /**
   * Returns if the loop is running.
   */
  bool IsRunning() const {
    return running_.load();
  }

  /**
   * Sends a command to the worker for processing. This can safely be done from
   * a different thread from the Run thread, but can only be called from one
   * thread at a time.
   *
   * @param args Constructor arguments for the command.
   * @return true if the command was successfully sent. If the queue is full
   *         then Send will return false immediately.
   */
  template <typename... Args>
  bool Send(Args&&... args);

  /**
   * Stops the worker. This can safely be called from another thread. All
   * previously commands sent are guaranteed to be processed before the loop
   * exits.
   */
  void Stop();

 private:
  MultiProducerQueue<Command> command_queue_;
  std::atomic<bool> stop_;
  std::atomic<bool> running_;
  port::Semaphore cmd_received_;
};

template <typename Command>
WorkerLoop<Command>::WorkerLoop(Env* env, uint32_t size)
: command_queue_(size)
, stop_(false)
, running_(false) {
}

template <typename Command>
WorkerLoop<Command>::~WorkerLoop() {
  Stop();
  while (running_) {
    std::this_thread::yield();
  }
}

template <typename Command>
void WorkerLoop<Command>::Run(std::function<void(Command cmd)> callback) {
  Command cmd;
  running_ = true;
  do {
    // Wait for commands or Stop() call.
    cmd_received_.Wait();

    // Continue processing commands as they come in.
    if (command_queue_.read(cmd)) {
      callback(std::move(cmd));
    }
  } while (!stop_.load());

  while (command_queue_.read(cmd)) {
    callback(std::move(cmd));
  }

  running_ = false;
}

template <typename Command>
template <typename... Args>
bool WorkerLoop<Command>::Send(Args&&... args) {
  bool result = command_queue_.write(std::forward<Args>(args)...);
  cmd_received_.Post();
  return result;
}

template <typename Command>
void WorkerLoop<Command>::Stop() {
  stop_ = true;
  cmd_received_.Post();
}

}  // namespace rocketspeed
