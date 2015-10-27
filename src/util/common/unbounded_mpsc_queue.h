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

// @author Ayush Lodha (ayushlodha@fb.com)

#pragma once

#include <deque>
#include <cstdlib>
#include <mutex>

namespace rocketspeed {

/*
 * UnboundedMPSCQueue is a unbounded multi producer and single consumer queue.
 */

template<class T>
struct UnboundedMPSCQueue {
  typedef T value_type;

  // non-copyable
  UnboundedMPSCQueue(const UnboundedMPSCQueue&) = delete;
  UnboundedMPSCQueue& operator=(const UnboundedMPSCQueue&) = delete;

  explicit UnboundedMPSCQueue(){}

  template<class ...Args>
  bool write(Args&&... record_args) {
    std::lock_guard<std::mutex> lock(deque_lock_);
    records_.emplace_back(std::forward<Args>(record_args)...);
    return true;
  }

  bool read(T& record) {
    std::lock_guard<std::mutex> lock(deque_lock_);
    if (records_.empty()) {
      // queue is empty
      return false;
    }
    record = std::move(records_.front());
    records_.pop_front();
    return true;
  }

  size_t getSize() const {
    std::lock_guard<std::mutex> lock(deque_lock_);
    return records_.size();
  }

private:
  std::deque<T> records_;
  std::mutex deque_lock_;
};

}  // namespace rocketspeed
