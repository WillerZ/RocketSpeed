// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include "src/port/port.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * Controls and measures pace of requests by controlling the number of inflight
 * requests a feeding latencies and throughput back into the system.
 *
 * Usage:
 *
 * Pacer pacer(...);
 * while (!pacer.HasConverged()) {
 *   pacer.Wait();
 *   SendRequest([&pacer] () {
 *     // Request complete callback (async).
 *     pacer.EndRequest();
 *   });
 * }
 * // Pacer has now converged on a window that will give the desired latency.
 * // Can start real workload now, using pacer in the same way:
 * for(;;) {
 *   pacer.Wait();
 *   SendRequest([&pacer] () {
 *     pacer.EndRequest();
 *   });
 * }
 *
 * IMPORTANT: The latency is measured from the end of the Wait() call to the
 * the start of EndRequest() call. In order to ensure the tuned latency and
 * throughput are meaningful, the process should do negligible work between
 * the end of Wait() and the start of the actual request, and also between
 * sending one request and attempting to start the next one.
 *
 * It is assumed that all started requests will eventually complete.
 * EndRequest() must be called exactly once per Wait(), even when the request
 * fails.
 *
 * Note that if the max_latency specified is lower that the minimum latency
 * achievable then the pacer will converge towards minimum throughput.
 */
class Pacer {
 public:
  using clock = std::chrono::steady_clock;

  /**
   * @param max_throughput Maximum requests to send per second.
   * @param max_latency Maximum mean latency to tune for.
   * @param max_inflight Maximum number of requests to have inflight at once.
   * @param convergence_window Time to run to reach stable measurements.
   */
  Pacer(uint64_t max_throughput,
        std::chrono::microseconds max_latency,
        uint64_t max_inflight,
        uint64_t convergence_samples);

  /**
   * Waits until its time for the next request.
   *
   * Not thread safe.
   */
  void Wait();

  /**
   * Signal the end of the request, and feedback a latency into the system.
   * This is the latency that will be tuned.
   * This call is thread safe.
   */
  void EndRequest();

  /**
   * Returns true once the pacer has converged on a window size that optimizes
   * parameters within the maximums specified.
   *
   * Must be called from same thread as writer.
   */
  bool HasConverged() const;

 private:
  /** For typing convenience */
  template <typename Duration>
  static std::chrono::microseconds ToMicros(Duration d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d);
  }

  const uint64_t max_throughput_;
  const std::chrono::microseconds max_latency_;
  const uint64_t convergence_samples_;

  uint64_t min_inflight_;
  uint64_t max_inflight_;
  uint64_t current_inflight_{0};
  port::Semaphore requests_;
  clock::time_point window_start_;
  std::deque<clock::time_point> start_times_;
  uint64_t window_sent_ = 0;
  std::atomic<uint64_t> window_received_{0};
  std::chrono::microseconds latency_sum_{0};
  std::mutex end_request_mutex_;
  port::Semaphore received_;

  ThreadCheck writer_thread_;
};

}  // namespace rocketspeed
