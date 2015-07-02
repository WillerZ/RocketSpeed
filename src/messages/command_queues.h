// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <memory>

#include "external/folly/producer_consumer_queue.h"

#include "include/Status.h"
#include "src/port/port.h"
#include "src/util/common/thread_check.h"
#include "src/util/common/thread_local.h"

namespace rocketspeed {

class BaseEnv;
class Command;
class Logger;

/**
 * A command + timestamp tuple.
 */
struct TimestampedCommand {
  std::unique_ptr<Command> command;
  uint64_t issued_time;
};

class alignas(CACHE_LINE_SIZE) CommandQueue {
 public:
  class BatchedRead {
   public:
    // Noncopyable & nonmovable
    BatchedRead(const BatchedRead&) = delete;
    BatchedRead& operator=(const BatchedRead&) = delete;
    BatchedRead(BatchedRead&&) = delete;
    BatchedRead& operator=(BatchedRead&&) = delete;

    /**
     * Creates an object which allows reading from given queue.
     * The object should be disposed once reader is done reading this batch,
     * otherwise there is a risk of a reader not being notified when new command
     * appears in the queue.
     * Using this object from thread other than reader for provided queue yields
     * undefined behaviour.
     * This object cannot outlive queue that it was bound to.
     *
     * @param queue A queue to read from.
     */
    explicit BatchedRead(CommandQueue* queue);

    ~BatchedRead();

    /**
     * Reads a timestamped command from the queue.
     *
     * @param ts_cmd Output for read command.
     * @return true iff a command was read, false otherwise.
     */
    bool Read(TimestampedCommand& ts_cmd);

   private:
    CommandQueue* queue_;
    size_t pending_reads_;
  };

  /**
   * Construct a queue with a given size (number of commands).
   *
   * @param env Environment.
   * @param info_log Logging interface.
   * @param size Maximum number of queued up commands.
   */
  CommandQueue(BaseEnv* env, std::shared_ptr<Logger> info_log, size_t size);

  ~CommandQueue();

  /**
   * Writes a command to the queue. If unsuccessful, the command pointer will
   * be left unconsumed.
   *
   * @param command A reference to the command to write.
   * @param check_thread Check that this is called from one thread.
   * @return true iff successful, false otherwise.
   */
  bool Write(std::unique_ptr<Command>& command, bool check_thread = true);

  /**
   * Upper-bound estimate of queue size.
   */
  size_t GetSize() const { return queue_.sizeGuess(); }

  /**
   * File descriptor for EventFd that will be written when new commands
   * are available.
   */
  int GetReadFd() { return ready_fd_.readfd(); }

 private:
  BaseEnv* env_;
  std::shared_ptr<Logger> info_log_;
  folly::ProducerConsumerQueue<TimestampedCommand> queue_;
  rocketspeed::port::Eventfd ready_fd_;
  /**
   * Sequentially consistent size of the queue.
   * Incremented after adding command(s) to the queue and decremented before
   * reading command(s) from it. Both increments and decrements may happen in
   * batches.
   */
  std::atomic<size_t> synced_size_;
  ThreadCheck read_check_;
  ThreadCheck write_check_;
};

/**
 * Lazily constructed CommandQueue per thread.
 */
class ThreadLocalCommandQueues {
 public:
  /**
   * Creates a ThreadLocalCommandQueues with specific lazy creation funciton.
   *
   * @param create_queue Callback for creating thread-local queues.
   */
  explicit ThreadLocalCommandQueues(
      std::function<std::shared_ptr<CommandQueue>()> create_queue);

  // non-copyable, non-moveable
  ThreadLocalCommandQueues(const ThreadLocalCommandQueues&) = delete;
  ThreadLocalCommandQueues(ThreadLocalCommandQueues&&) = delete;
  ThreadLocalCommandQueues& operator=(const ThreadLocalCommandQueues&) = delete;
  ThreadLocalCommandQueues& operator=(ThreadLocalCommandQueues&&) = delete;

  /**
   * The thread-local CommandQueue.
   */
  CommandQueue* GetThreadLocal();

 private:
  ThreadLocalObject<std::shared_ptr<CommandQueue>> thread_local_;
};

}  // namespace rocketspeed
