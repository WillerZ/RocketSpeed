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
    : queue_(queue), performed_reads_(0) {
  queue_->read_check_.Check();
  // Clear notification, we will add it back if the batch doesn't empty the
  // queue.
  eventfd_t ignored_value;
  queue_->ready_fd_.read_event(&ignored_value);
}

CommandQueue::BatchedRead::~BatchedRead() {
  queue_->read_check_.Check();
  assert(performed_reads_ >= 0);

  // Decrement sequentially consistent size by the number of commands read
  // in the batch.
  ssize_t size_after_read = queue_->synced_size_ -= performed_reads_;
  // If there is a concurent write batch, the size fetched above can be up to
  // batch size smaller than the number of commands in the queue (how many times
  // read on the queue can succeed).
  //   size_after_read = |queue| - performed_writes
  // We need to notify when there are some elements left in the queue, that is
  // when number of commands is >0, that is then size fetched above is > - (max
  // size of write batch) == -1.
  if (size_after_read > -1) {
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
  if (performed_reads_ >= kMaxBatchSize) {
    return false;
  }
  bool successful = queue_->queue_.read(ts_cmd);
  if (successful) {
    ++performed_reads_;
  }
  return successful;
}

CommandQueue::CommandQueue(BaseEnv* env,
                           std::shared_ptr<Logger> info_log,
                           size_t size)
: env_(env)
, info_log_(std::move(info_log))
, queue_(static_cast<uint32_t>(size))
, synced_size_(0)
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

  // Increment sequentially consistent size by the number of commands written
  // in the batch.
  ssize_t size_after_write = ++synced_size_;
  // If there is a concurent read batch, the size fetched above can be up to
  // batch size bigger than the number of commands in the queue (how many times
  // read on the queue can succeed).
  //   size_after_write = |queue| + performed_reads
  // We need to notify when the queue went from empty to non-empty, that is when
  // queue size changed to 1, that is when the size fetched above is no greater
  // than 1 + max read batch size.
  if (size_after_write <= 1 + BatchedRead::kMaxBatchSize) {
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
