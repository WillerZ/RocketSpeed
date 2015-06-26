// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include "external/folly/producer_consumer_queue.h"
#include "include/Status.h"
#include "src/port/port.h"
#include "src/util/common/thread_check.h"

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

class CommandQueue {
 public:
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
   * Reads a timestamped command from the queue.
   *
   * @param ts_cmd Output for read command.
   * @return true iff a command was read, false otherwise.
   */
  bool Read(TimestampedCommand& ts_cmd);

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
  size_t GetSize() const {
    return queue_.sizeGuess();
  }

  /**
   * File descriptor for EventFd that will be written when new commands
   * are available.
   */
  int GetReadFd() {
    return ready_fd_.readfd();
  }

 private:
  BaseEnv* env_;
  std::shared_ptr<Logger> info_log_;
  folly::ProducerConsumerQueue<TimestampedCommand> queue_;
  rocketspeed::port::Eventfd ready_fd_;
  eventfd_t pending_reads_;
  ThreadCheck read_check_;
  ThreadCheck write_check_;
};

}  // namespace rocketspeed
