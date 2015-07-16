// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/command_queues.h"

#include <atomic>

#include "include/Logger.h"
#include "src/messages/commands.h"
#include "src/util/common/base_env.h"

namespace rocketspeed {

CommandQueue::BatchedRead::BatchedRead(CommandQueue* queue)
    : queue_(queue), pending_reads_(0), commands_read_(0), delayed_reads_(0) {
  // Clear notification, it will be added if batch finishes after hitting size
  // limit.
  eventfd_t value;
  queue_->ready_fd_.read_event(&value);
  // Number of eventfd writes performed equals the value of eventfd.
  queue_->stats_->eventfd_num_writes->Add(value);
  queue_->stats_->eventfd_num_reads->Add(1);
}

CommandQueue::BatchedRead::~BatchedRead() {
  queue_->stats_->num_reads->Add(commands_read_);
  queue_->stats_->batched_read_size->Record(commands_read_);
  // If we've exited batch because of size limit, we must notify regardless of
  // the locally cached number of commands, as we didn't check if there is a
  // command waiting for us.
  if (commands_read_ >= kMaxBatchSize ||
      pending_reads_ > 0 ||
      delayed_reads_ > 0) {
    // Return tokens back to atomic size.
    queue_->synced_size_.fetch_add(pending_reads_);
    // Notify ourselves, so the EventLoop will pick this queue eventually.
    queue_->stats_->eventfd_num_writes->Add(1);
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

      // Wtih errno == EAGAIN, we can just let this fall through.
    }
  }
}

bool CommandQueue::BatchedRead::Read(TimestampedCommand& ts_cmd) {
  queue_->read_check_.Check();
  // Check if we didn't exceed allowed batch size.
  if (commands_read_ >= kMaxBatchSize) {
    return false;
  }
  if (pending_reads_ == 0) {
    // We try to leave one message to mark the fact that there is an ongoing
    // BatchedRead, this way concurrent writers will not notify the queue.
    pending_reads_ = queue_->synced_size_.load();
    if (pending_reads_ == 0) {
      delayed_reads_ = 0;
      return false;
    } else if (pending_reads_ == 1) {
      pending_reads_ = queue_->synced_size_.exchange(0);
      delayed_reads_ = 0;
    } else {
      // We are the sole reader, therefore synced_size_ can only be > 1 now.
      pending_reads_ = queue_->synced_size_.exchange(1);
      assert(pending_reads_ > 1);
      --pending_reads_;
      delayed_reads_ = 1;
    }
  }
  if (pending_reads_ > 0) {
    bool success = queue_->queue_.read(ts_cmd);
    assert(success);
    ((void)success);
    --pending_reads_;
    ++commands_read_;
    return true;
  }
  return false;
}

CommandQueue::Stats::Stats(const std::string& prefix) {
  batched_read_size = all.AddHistogram(
      prefix + ".batched_read_size", 0, BatchedRead::kMaxBatchSize, 1, 1.1);
  num_reads = all.AddCounter(prefix + ".num_reads");
  eventfd_num_writes = all.AddCounter(prefix + ".eventfd_num_writes");
  eventfd_num_reads = all.AddCounter(prefix + ".eventfd_num_reads");
}

CommandQueue::CommandQueue(BaseEnv* env,
                           std::shared_ptr<Logger> info_log,
                           std::shared_ptr<Stats> stats,
                           size_t size)
    : env_(env)
    , info_log_(std::move(info_log))
    , stats_(std::move(stats))
    , queue_(static_cast<uint32_t>(size))
    , ready_fd_(true, true)
    , synced_size_(0) {
  assert(ready_fd_.status() == 0);
}

CommandQueue::~CommandQueue() {
  ready_fd_.closefd();
}

bool CommandQueue::Write(std::unique_ptr<Command>& command, bool check_thread) {
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

  // Write notification if the queue went from empty to non-empty.
  if (synced_size_.fetch_add(1) == 0) {
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

      // Wtih errno == EAGAIN, we can just let this fall through.
    }
  }

  return true;
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
