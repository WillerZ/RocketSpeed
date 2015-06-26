// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/command_queues.h"
#include "include/Logger.h"
#include "src/messages/commands.h"
#include "src/util/common/base_env.h"

namespace rocketspeed {

CommandQueue::CommandQueue(BaseEnv* env,
                           std::shared_ptr<Logger> info_log,
                           size_t size)
: env_(env)
, info_log_(std::move(info_log))
, queue_(static_cast<uint32_t>(size))
, ready_fd_(true, true)
, pending_reads_(0) {
}

bool CommandQueue::Read(TimestampedCommand& ts_cmd) {
  read_check_.Check();

  if (pending_reads_ == 0) {
    ready_fd_.read_event(&pending_reads_);
  }
  if (pending_reads_ > 0) {
    --pending_reads_;
    return queue_.read(ts_cmd);
  }
  return false;
}

bool CommandQueue::Write(std::unique_ptr<Command>& command,
                         bool check_thread) {
  if (check_thread) {
    write_check_.Check();
  }

  // Add timestamp to the command (for measuring command processing latency).
  TimestampedCommand ts_cmd { std::move(command), env_->NowMicros() };

  // Attempt to write to queue.
  if (!queue_.write(std::move(ts_cmd))) {
    // The queue was full and the write failed.
    LOG_WARN(info_log_, "The command queue is full");
    assert(ts_cmd.command);
    command = std::move(ts_cmd.command);  // put back
    return false;
  }

  // Write to the command_ready_eventfd_ to send an event to the reader.
  if (ready_fd_.write_event(1)) {
    // Some internal error happened.
    LOG_ERROR(info_log_,
      "Error writing a notification to command eventfd, errno=%d", errno);

    // Can only fail with EAGAIN or EINVAL.
    // EAGAIN only happens if we have written 2^64 events without reading,
    // and EINVAL should never happen since we are writing the correct number
    // of bytes.
    assert(errno != EINVAL);

    // Wtih errno == EAGAIN, we can just let this fall through.
  }

  return true;
}

CommandQueue::~CommandQueue() {
  ready_fd_.closefd();
}

}  // namespace rocketspeed
