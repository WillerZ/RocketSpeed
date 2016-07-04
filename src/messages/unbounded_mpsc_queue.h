/*
 * Copyright 2015 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <chrono>
#include <deque>
#include <cstdlib>
#include <limits>
#include <mutex>

#include "include/Logger.h"
#include "include/Status.h"
#include "src/messages/event_loop.h"
#include "src/messages/queues.h"
#include "src/port/port.h"
#include "include/BaseEnv.h"
#include "src/util/common/flow.h"
#include "src/util/common/statistics.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class Command;
class EventCallback;
class EventLoop;

/**
 * Unbounded, multi-producer, single-consumer queue.
 */
template <typename Item>
class UnboundedMPSCQueue : public Source<Item> {
 public:
  /**
   * Construct a queue.
   *
   * @param info_log Logging interface.
   * @param stats A stats that can be shared with other queues.
   * @param soft_limit Queue size limit used by TryWrite.
   */
  UnboundedMPSCQueue(std::shared_ptr<Logger> info_log,
                     std::shared_ptr<QueueStats> stats,
                     size_t soft_limit = std::numeric_limits<size_t>::max());

  ~UnboundedMPSCQueue();

  /**
   * Writes an element to the queue, ignoring soft limit. Always succeeds.
   * WARNING: only use Write if the size of the queue is bounded in some other
   * way. Unmoderated use could result in out-of-memory errors.
   *
   * @param item The item to write to the queue.
   */
  template <typename ItemRef>
  void Write(ItemRef&& item);

  /**
   * Writes an element to the queue if the queue size is under the soft limit,
   * otherwise fails and does not move item.
   *
   * @param item The item to write to the queue.
   * @return false iff the queue size is above the soft limit.
   */
  template <typename ItemRef>
  bool TryWrite(ItemRef&& item);

  /**
   * Try to read an item from the queue.
   *
   * @param item Output for read item.
   * @return true iff an item was read, false otherwise.
   */
  bool TryRead(Item& item);

  size_t GetQueueSize() const;

  void RegisterReadEvent(EventLoop* event_loop) final override {
    event_loop->RegisterFdReadEvent(read_ready_fd_.readfd(),
                                    [this] () { this->Drain(); });
  }

  void SetReadEnabled(EventLoop* event_loop, bool enabled) final override {
    event_loop->SetFdReadEnabled(read_ready_fd_.readfd(), enabled);
  }

 private:
  std::shared_ptr<Logger> info_log_;
  std::shared_ptr<QueueStats> stats_;
  std::deque<Timestamped<Item>> queue_;
  mutable std::mutex mutex_;
  port::Eventfd read_ready_fd_;
  ThreadCheck read_check_;
  const size_t soft_limit_;

  void Drain();
};

/**
 * MPSC Queue of std::unique_ptr<Command>.
 * Not a typedef so that it can be forward declared.
 */
class UnboundedMPSCCommandQueue : public UnboundedMPSCQueue<
                                  std::unique_ptr<Command>> {
 public:
  using Base = UnboundedMPSCQueue<std::unique_ptr<Command>>;
  using Base::Base;
};

template <typename Item>
UnboundedMPSCQueue<Item>::UnboundedMPSCQueue(
    std::shared_ptr<Logger> info_log,
    std::shared_ptr<QueueStats> stats,
    size_t soft_limit)
: info_log_(std::move(info_log))
, stats_(std::move(stats))
, queue_()
, read_ready_fd_(true, true)
, soft_limit_(soft_limit) {
  if (read_ready_fd_.status() != 0) {
    LOG_FATAL(info_log_, "Queue cannot be created: unable to create Eventfd");
    info_log_->Flush();
  }
  RS_ASSERT(read_ready_fd_.status() == 0);
}

template <typename Item>
UnboundedMPSCQueue<Item>::~UnboundedMPSCQueue() {
  read_ready_fd_.closefd();
}

template <typename Item>
template <typename ItemRef>
void UnboundedMPSCQueue<Item>::Write(ItemRef&& item) {
  Timestamped<Item> entry { std::move(item), std::chrono::steady_clock::now() };
  std::unique_lock<std::mutex> lock(mutex_);
  queue_.emplace_back(std::move(entry));
  lock.unlock();

  if (read_ready_fd_.write_event(1)) {
    LOG_ERROR(info_log_,
              "Error writing a notification to command eventfd, errno=%d",
              errno);

    // Can only fail with EAGAIN or EINVAL.
    // Linux: EAGAIN only happens if we have written 2^64 events without reading
    // MacOS: EAGAIN happens after pipe's buffer (small by default) overflows
    // EINVAL should never happen since we are writing the correct number
    // of bytes.
    RS_ASSERT(errno != EINVAL);
  }
}

template <typename Item>
template <typename ItemRef>
bool UnboundedMPSCQueue<Item>::TryWrite(ItemRef&& item) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (queue_.size() >= soft_limit_) {
    return false;
  }
  Timestamped<Item> entry { std::move(item), std::chrono::steady_clock::now() };
  queue_.emplace_back(std::move(entry));
  lock.unlock();

  if (read_ready_fd_.write_event(1)) {
    LOG_ERROR(info_log_,
              "Error writing a notification to command eventfd, errno=%d",
              errno);

    // Can only fail with EAGAIN or EINVAL.
    // Linux: EAGAIN only happens if we have written 2^64 events without reading
    // MacOS: EAGAIN happens after pipe's buffer (small by default) overflows
    // EINVAL should never happen since we are writing the correct number
    // of bytes.
    RS_ASSERT(errno != EINVAL);
    return false;
  }
  return true;
}

template <typename Item>
bool UnboundedMPSCQueue<Item>::TryRead(Item& item) {
  read_check_.Check();

  stats_->eventfd_num_reads->Add(1);
  eventfd_t value;
  if (read_ready_fd_.read_event(&value) < 0) {
    return false;
  }
  RS_ASSERT(value > 0);

  std::unique_lock<std::mutex> lock(mutex_);
  Timestamped<Item> entry = std::move(queue_.front());
  queue_.pop_front();
  lock.unlock();

  read_ready_fd_.write_event(value - 1);
  stats_->eventfd_num_writes->Add(2); // read side + write side

  auto now = std::chrono::steady_clock::now();
  auto delta = now - entry.timestamp;
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(delta);
  stats_->response_latency->Record(micros.count());
  item = std::move(entry.item);

  stats_->num_reads->Add(1);
  return true;
}

template <typename Item>
size_t UnboundedMPSCQueue<Item>::GetQueueSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return queue_.size();
}

template <typename Item>
void UnboundedMPSCQueue<Item>::Drain() {
  Item item;
  while (TryRead(item)) {
    if (!this->DrainOne(std::move(item))) {
      break;
    }
  }
}

}  // namespace rocketspeed
