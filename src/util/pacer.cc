// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/pacer.h"

#include <thread>

namespace rocketspeed {

Pacer::Pacer(uint64_t max_throughput,
             std::chrono::microseconds max_latency,
             uint64_t max_inflight,
             uint64_t convergence_samples)
: max_throughput_(max_throughput)
, max_latency_(max_latency)
, convergence_samples_(convergence_samples)
, min_inflight_(1)
, max_inflight_(max_inflight)
, current_inflight_((max_inflight_ + 1) / 2)
, requests_(static_cast<unsigned int>(current_inflight_)) {
  // Validate.
  assert(max_throughput_ > 0);
  assert(max_inflight_ > 0);
  assert(convergence_samples_ > 0);

  if (max_latency == std::chrono::microseconds::max()) {
    // This is a special setting for max_latency, which essentially means to
    // ignore it and rely on max_throughput and max_inflight.
    // We won't need to binary search to tune the latency, so set the inflight
    // range to 0 to signal that we have immediately converged.
    min_inflight_ = max_inflight_;
  }
}

Pacer::Pacer(uint64_t max_throughput,
             uint64_t max_inflight)
: max_throughput_(max_throughput)
, max_latency_(0)
, convergence_samples_(0)
, min_inflight_(max_inflight)  // == max_inflight, so converges immediately
, max_inflight_(max_inflight)
, current_inflight_(max_inflight)
, requests_(static_cast<unsigned int>(current_inflight_)) {
  // Validate.
  assert(max_throughput_ > 0);
  assert(max_inflight_ > 0);
}

void Pacer::Wait() {
  writer_thread_.Check();

  if (max_inflight_ > min_inflight_) {
    // Check to see if we need to adjust window.
    auto window_received = window_received_.load(std::memory_order_acquire);
    if (window_received >= convergence_samples_) {
      // Adjust window size to tune latency.
      // According to Little's Law, window size is positively correlated
      // with throughput, and negatively with latency.
      auto latency = latency_sum_ / window_received;
      if (latency > max_latency_) {
        // Latency too high, decrease window.
        max_inflight_ = current_inflight_;
      } else {
        // Latency too low, increase window.
        min_inflight_ = current_inflight_ + 1;
      }
      // Binary search.
      uint64_t prev_inflight = current_inflight_;
      current_inflight_ = min_inflight_ + (max_inflight_ - min_inflight_) / 2;

      // Adjust semaphore for new window size.
      while (current_inflight_ > prev_inflight) {
        requests_.Post();
        prev_inflight++;
      }
      while (current_inflight_ < prev_inflight) {
        requests_.Wait();
        prev_inflight--;
      }

      // Reset window.
      // Wait for all outstanding messages to be received.
      while (window_sent_) {
        received_.Wait();
        --window_sent_;
      }
      window_received_ = 0;
      latency_sum_ = std::chrono::microseconds(0);
    }
  }

  // Wait for space in the window.
  requests_.Wait();

  if (window_sent_ == 0) {
    // First message in current window.
    window_start_ = clock::now();
  } else {
    // Check that we aren't exceeding throughput for this window.
    auto elapsed = clock::now() - window_start_;
    auto expected = std::chrono::seconds(1) * window_sent_ / max_throughput_;
    if (expected > elapsed) {
      /* sleep override */
      std::this_thread::sleep_for(expected - elapsed);
    }
  }

  ++window_sent_;
  start_times_.emplace_back(clock::now());
}

void Pacer::EndRequest() {
  // Remove an outstanding request and record latency.
  // Note: it doesn't matter what order the requests come back because
  // (e1-s1)+(e2-s2) = (e1-s2)+(e2-s1)
  // and we only care about the sum, so we can just take the earliest.
  {
    std::lock_guard<std::mutex> lock(end_request_mutex_);
    assert(!start_times_.empty());
    latency_sum_ += ToMicros(clock::now() - start_times_.front());
    start_times_.pop_front();
  }
  window_received_.fetch_add(1, std::memory_order_seq_cst);
  requests_.Post();
  received_.Post();
}

bool Pacer::HasConverged() const {
  writer_thread_.Check();
  return max_inflight_ <= min_inflight_;
}

}  // namespace rocketspeed
