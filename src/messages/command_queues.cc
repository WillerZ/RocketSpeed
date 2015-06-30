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

CommandQueue::BatchedRead::BatchedRead(CommandQueue* queue)
    : queue_(queue), allowed_reads_(kMaxBatchSize) {
  queue_->read_check_.Check();
  // Clear notification, we will add it back if the batch doesn't empty the
  // queue.
  eventfd_t ignored_value;
  queue_->ready_fd_.read_event(&ignored_value);
}

CommandQueue::BatchedRead::~BatchedRead() {
  queue_->read_check_.Check();
  // Write back notification if we haven't emptied the queue in this batch; if
  // the queue was emptied we will be notified by the writer that inserts next
  // command.
  if (!queue_->queue_.isEmpty()) {
    if (queue_->ready_fd_.write_event(1)) {
      // Some internal error happened.
      LOG_ERROR(queue_->info_log_,
                "Error writing a notification to command eventfd, errno=%d",
                errno);

      // Can only fail with EAGAIN or EINVAL.
      // EAGAIN only happens if we have written 2^64 events without reading,
      // and EINVAL should never happen since we are writing the correct number
      // of bytes.
      assert(errno != EINVAL);

      // Wtih errno == EAGAIN, we can just let this fall through, because
      // eventfd is readable iff the stored value is nonzero.
    }
  }
}

bool CommandQueue::BatchedRead::Read(TimestampedCommand& ts_cmd) {
  queue_->read_check_.Check();
  // If allowed batch size would be exceeded, return false.
  if (allowed_reads_ == 0) {
    return false;
  }
  --allowed_reads_;
  return queue_->queue_.read(ts_cmd);
}

CommandQueue::CommandQueue(BaseEnv* env,
                           std::shared_ptr<Logger> info_log,
                           size_t size)
: env_(env)
, info_log_(std::move(info_log))
, queue_(static_cast<uint32_t>(size))
, ready_fd_(true, true) {
}

bool CommandQueue::Write(std::unique_ptr<Command>& command,
                         bool check_thread) {
  // Whether or not this flag is set, there is only one concurrent writer
  // executing this function at any time.
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

  // Write to the eventfd only if the queue was empty before the write.
  // Otherwise the eventfd was already notified or there is an ongoing
  // BatchedRead happening, in which case writing a notification is a
  // responsibility of the reader.
  // Size quess seen by the producer is an upper bound, which means that we can
  // notify when it's not needed but never the other way around.
  if (queue_.sizeGuess() == 1) {
    if (ready_fd_.write_event(1)) {
      // Some internal error happened.
      LOG_ERROR(info_log_,
                "Error writing a notification to command eventfd, errno=%d",
                errno);

      // Can only fail with EAGAIN or EINVAL.
      // EAGAIN only happens if we have written 2^64 events without reading,
      // and EINVAL should never happen since we are writing the correct number
      // of bytes.
      assert(errno != EINVAL);

      // Wtih errno == EAGAIN, we can just let this fall through, because
      // eventfd is readable iff the stored value is nonzero.
    }
  }

  return true;
}

CommandQueue::~CommandQueue() {
  ready_fd_.closefd();
}

ThreadLocalCommandQueues::ThreadLocalCommandQueues(
  std::function<std::shared_ptr<CommandQueue>()> create_queue)
: thread_local_([this, create_queue] () {
    return new std::shared_ptr<CommandQueue>(create_queue());
  }) {
}

CommandQueue* ThreadLocalCommandQueues::GetThreadLocal() {
  return thread_local_.GetThreadLocal().get();
}

}  // namespace rocketspeed
