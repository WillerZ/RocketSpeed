// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <chrono>
#include <memory>

#include "external/folly/producer_consumer_queue.h"

#include "include/Logger.h"
#include "include/Status.h"
#include "src/messages/event_loop.h"
#include "src/port/port.h"
#include "src/util/common/base_env.h"
#include "src/util/common/flow.h"
#include "src/util/common/statistics.h"
#include "src/util/common/thread_check.h"
#include "src/util/common/thread_local.h"

namespace rocketspeed {

class Command;
class EventCallback;
class EventLoop;

template <typename Item>
class BatchedRead;

/**
 * A item + timestamp tuple.
 */
template <typename Item>
struct Timestamped {
  Item item;
  std::chrono::steady_clock::time_point timestamp;
};

/**
 * Statistics for a queue.
 */
class QueueStats {
 public:
  explicit QueueStats(const std::string& prefix);

  Statistics all;
  Histogram* batched_read_size;
  Histogram* response_latency;
  Counter* num_reads;
  Counter* eventfd_num_writes;
  Counter* eventfd_num_reads;
};

/**
 * Creates an EventCallback on the read availability of an fd.
 */
std::unique_ptr<EventCallback>
CreateEventFdReadCallback(EventLoop* event_loop,
                          int fd,
                          std::function<void()> callback);

/**
 * Fixed-size, single-producer, single-consumer queue.
 */
template <typename Item>
class Queue : public Source<Item>, public Sink<Item> {
 public:
  /**
   * Construct a queue with a given size (number of commands).
   *
   * @param info_log Logging interface.
   * @param stats A stats that can be shared with other queues.
   * @param size Maximum number of queued up commands.
   */
  Queue(std::shared_ptr<Logger> info_log,
        std::shared_ptr<QueueStats> stats,
        size_t size);

  ~Queue();

  bool TryWrite(Item& command, bool check_thread = true) final override;

  void Drain() final override;

  /** Upper-bound estimate of queue size. */
  size_t GetSize() const { return queue_.sizeGuess(); }

  void RegisterReadEvent(EventLoop* event_loop) final override {
    event_loop->RegisterFdReadEvent(read_ready_fd_.readfd(),
                                    [this] () { this->Drain(); });
  }

  void SetReadEnabled(EventLoop* event_loop, bool enabled) final override {
    event_loop->SetFdReadEnabled(read_ready_fd_.readfd(), enabled);
  }

  std::unique_ptr<EventCallback>
  CreateWriteCallback(EventLoop* event_loop,
                      std::function<void()> callback) final override {
    return CreateEventFdReadCallback(event_loop,
                                     write_ready_fd_.readfd(),
                                     std::move(callback));
  }

 private:
  friend class BatchedRead<Item>;

  std::shared_ptr<Logger> info_log_;
  std::shared_ptr<QueueStats> stats_;
  folly::ProducerConsumerQueue<Timestamped<Item>> queue_;
  rocketspeed::port::Eventfd read_ready_fd_;
  rocketspeed::port::Eventfd write_ready_fd_;

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
 * Queue of std::unique_ptr<Command>.
 * Not a typedef so that it can be forward declared.
 */
class CommandQueue : public Queue<std::unique_ptr<Command>> {
 public:
  using Base = Queue<std::unique_ptr<Command>>;
  using Base::Base;
};

/** Maximum number of elements to read from a queue in a batch. */
constexpr size_t kMaxQueueBatchReadSize = 100;

/**
 * Utility for efficiently reading from a queue in batches. Optimized for
 * minimizing eventfd reads and writes.
 */
template <typename Item>
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
   * otherwise there is a risk of a reader not being notified when new item
   * appears in the queue.
   * Using this object from thread other than reader for provided queue yields
   * undefined behaviour.
   * This object cannot outlive queue that it was bound to.
   *
   * @param queue A queue to read from.
   */
  explicit BatchedRead(Queue<Item>* queue);

  ~BatchedRead();

  /**
   * Reads an item from the queue.
   *
   * @param item Output for read item.
   * @return true iff an item was read, false otherwise.
   */
  bool Read(Item& item);

 private:
  Queue<Item>* queue_;
  size_t pending_reads_;  // pending items we intend to process.
  size_t commands_read_;  // successful reads from the queue.
  size_t delayed_reads_;  // pending items we intend not to process (yet).
};

/**
 * Lazily constructed queue per thread.
 */
template <typename T>
class ThreadLocalQueues {
 public:
  /**
   * Creates a ThreadLocalCommandQueues with specific lazy creation funciton.
   *
   * @param create_queue Callback for creating thread-local queues.
   */
  explicit ThreadLocalQueues(
      std::function<std::shared_ptr<Queue<T>>()> create_queue);

  // non-copyable, non-moveable
  ThreadLocalQueues(const ThreadLocalQueues&) = delete;
  ThreadLocalQueues(ThreadLocalQueues&&) = delete;
  ThreadLocalQueues& operator=(const ThreadLocalQueues&) = delete;
  ThreadLocalQueues& operator=(ThreadLocalQueues&&) = delete;

  /**
   * The thread-local queue.
   */
  Queue<T>* GetThreadLocal();

 private:
  ThreadLocalObject<std::shared_ptr<Queue<T>>> thread_local_;
};

using ThreadLocalCommandQueues = ThreadLocalQueues<std::unique_ptr<Command>>;

template <typename T>
ThreadLocalQueues<T>::ThreadLocalQueues(
  std::function<std::shared_ptr<Queue<T>>()> create_queue)
: thread_local_([this, create_queue] () {
    return new std::shared_ptr<Queue<T>>(create_queue());
  }) {
}

template <typename T>
Queue<T>* ThreadLocalQueues<T>::GetThreadLocal() {
  return thread_local_.GetThreadLocal().get();
}

template <typename Item>
BatchedRead<Item>::BatchedRead(Queue<Item>* queue)
    : queue_(queue), pending_reads_(0), commands_read_(0), delayed_reads_(0) {
  // Clear notification, it will be added if batch finishes after hitting size
  // limit.
  eventfd_t value;
  queue_->read_ready_fd_.read_event(&value);
  // Number of eventfd writes performed equals the value of eventfd.
  queue_->stats_->eventfd_num_writes->Add(value);
  queue_->stats_->eventfd_num_reads->Add(1);
}

template <typename Item>
BatchedRead<Item>::~BatchedRead() {
  queue_->stats_->num_reads->Add(commands_read_);
  queue_->stats_->batched_read_size->Record(commands_read_);
  // If we've exited batch because of size limit, we must notify regardless of
  // the locally cached number of commands, as we didn't check if there is a
  // command waiting for us.
  if (commands_read_ >= kMaxQueueBatchReadSize ||
      pending_reads_ > 0 ||
      delayed_reads_ > 0) {
    // Return tokens back to atomic size.
    queue_->synced_size_.fetch_add(pending_reads_);
    // Notify ourselves, so the EventLoop will pick this queue eventually.
    queue_->stats_->eventfd_num_writes->Add(1);
    if (queue_->read_ready_fd_.write_event(1)) {
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

template <typename Item>
bool BatchedRead<Item>::Read(Item& item) {
  queue_->read_check_.Check();
  // Check if we didn't exceed allowed batch size.
  if (commands_read_ >= kMaxQueueBatchReadSize) {
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
      delayed_reads_ = 0;
      pending_reads_ = queue_->synced_size_.exchange(0);
    } else {
      // We are the sole reader, therefore synced_size_ can only be > 1 now.
      pending_reads_ = queue_->synced_size_.exchange(1);
      assert(pending_reads_ > 1);
      --pending_reads_;
      delayed_reads_ = 1;
    }
  }
  if (pending_reads_ > 0) {
    Timestamped<Item> entry;
    bool success = queue_->queue_.read(entry);
    assert(success);
    if (!success) {
      return false;
    }
    auto now = std::chrono::steady_clock::now();
    auto delta = now - entry.timestamp;
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(delta);
    queue_->stats_->response_latency->Record(micros.count());
    item = std::move(entry.item);
    --pending_reads_;
    ++commands_read_;

    if (pending_reads_ == queue_->queue_.maxSize() / 2) {
      // Queue is now half-full, so signal write event.
      queue_->stats_->eventfd_num_writes->Add(1);
      queue_->write_ready_fd_.write_event(1);
    }
    return true;
  }
  return false;
}

template <typename Item>
Queue<Item>::Queue(std::shared_ptr<Logger> info_log,
                   std::shared_ptr<QueueStats> stats,
                   size_t size)
    : info_log_(std::move(info_log))
    , stats_(std::move(stats))
    , queue_(static_cast<uint32_t>(size + 1))  // ProducerConsumerQueue needs
    , read_ready_fd_(true, true)               // n+1 to store n items.
    , write_ready_fd_(true, true)
    , synced_size_(0) {
  assert(read_ready_fd_.status() == 0);
  assert(write_ready_fd_.status() == 0);
}

template <typename Item>
Queue<Item>::~Queue() {
  read_ready_fd_.closefd();
  write_ready_fd_.closefd();
}

template <typename Item>
bool Queue<Item>::TryWrite(Item& item, bool check_thread) {
  if (check_thread) {
    write_check_.Check();
  }

  // Attempt to write to queue.
  Timestamped<Item> entry { std::move(item), std::chrono::steady_clock::now() };
  if (!queue_.write(std::move(entry))) {
    // The queue was full and the write failed.
    LOG_WARN(info_log_, "Queue is overflowing -- this is OK with flow control");

    // Put the item back.
    item = std::move(entry.item);
    return false;
  }

  // Write notification if the queue went from empty to non-empty.
  if (synced_size_.fetch_add(1) == 0) {
    if (read_ready_fd_.write_event(1)) {
      // Some internal error happened.
      LOG_ERROR(info_log_,
                "Error writing a notification to command eventfd, errno=%d",
                errno);

      // Can only fail with EAGAIN or EINVAL.
      // EAGAIN only happens if we have written 2^64 events without reading,
      // and EINVAL should never happen since we are writing the correct number
      // of bytes.
      assert(errno != EINVAL);

      // With errno == EAGAIN, we can just let this fall through.
    }
  }

  return true;
}

template <typename Item>
void Queue<Item>::Drain() {
  BatchedRead<Item> batch(this);
  Item item;
  while (batch.Read(item)) {
    if (!this->DrainOne(std::move(item))) {
      break;
    }
  }
}

}  // namespace rocketspeed
