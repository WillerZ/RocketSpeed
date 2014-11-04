// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "src/port/port.h"
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace rocketspeed {

/**
 * Simple counter. Aligned to the cache to avoid potential false sharing.
 *
 * Safe to be written to from one thread and read from others.
 */
class alignas(CACHE_LINE_SIZE) Counter {
 public:
  Counter()
  : count_(0) {
  }

  Counter(const Counter& src)
  : count_(src.count_.load(std::memory_order_acquire)) {
  }

  void Add(uint64_t delta) {
    count_.fetch_add(delta, std::memory_order_acq_rel);
  }

  uint64_t Get() const {
    return count_.load(std::memory_order_acquire);
  }

  void Aggregate(const Counter& counter) {
    Add(counter.Get());
  }

  std::string Report() const;

 private:
  std::atomic<uint64_t> count_{0};
};

/**
 * Histogram with log-scale buckets.
 *
 * Safe to be written to by one thread, and read from others.
 */
class Histogram {
 public:
  /**
   * Creates a histogram with logarithmic bucket sizes. Samples will be
   * clamped between min and max.
   *
   * @param min Minimum allowed sample value.
   * @param max Maximum allowed sample value.
   * @param smallest_bucket Size of the first bucket.
   * @param ratio The ratio of successive bucket sizes. The number of
   *              buckets will be proportional to 1 / log(ratio).
   */
  explicit Histogram(double min,
                     double max,
                     double smallest_bucket,
                     double ratio = 1.2);

  /**
   * Make a copy of a histogram.
   */
  Histogram(const Histogram& src);

  /**
   * Adds a sample to the histogram. If sample is outside the range of
   * [min, max] then it will be clamped.
   */
  void Record(double sample);

  /**
   * Computes an approximate percentile from the sampled data.
   * The error, relative to the minimum sample value is bounded by the bucket
   * ratio.
   */
  double Percentile(double p) const;

  /**
   * Aggregate another histogram into this histogram.
   * The other histogram must have the *exact* same parameters.
   */
  void Aggregate(const Histogram& histogram);

  /**
   * Report some statistics on the histogram.
   */
  std::string Report() const;

 private:
  size_t BucketIndex(double sample) const;

  double min_;
  double max_;
  double smallest_bucket_;
  double ratio_;
  std::atomic<uint64_t> num_samples_;
  std::unique_ptr<std::atomic<uint64_t>[]> bucket_counts_;
  size_t num_buckets_;
};

/**
 * Collection of named statistics.
 *
 * Not thread-safe.
 */
class Statistics {
 public:
  Statistics() {}

  /**
   * Adds a new, named Counter object to the tracked statistics.
   */
  Counter* AddCounter(const std::string& name);

  /**
   * Adds a new, named Histogram object to the tracked statistics.
   */
  Histogram* AddHistogram(const std::string& name,
                          double min,
                          double max,
                          double smallest_bucket,
                          double ratio = 1.2);

  /**
   * Adds a new, named Histogram object with default settings for measuring
   * latencies.
   */
  Histogram* AddLatency(const std::string& name);

  /**
   * Generate a report of all tracked statistics.
   */
  std::string Report() const;

  /**
   * Adds another set of statistics to this statistic.
   * Statistics with the same name should have the same parameters.
   */
  void Aggregate(const Statistics& stats);

 private:
  // Aggregates one set of statistics into another.
  template <typename T>
  static void AggregateOne(
    std::unordered_map<std::string, std::unique_ptr<T>>* dst,
    const std::unordered_map<std::string, std::unique_ptr<T>>& src);

  // Maps of counter/histogram names to those objects.
  std::unordered_map<std::string, std::unique_ptr<Counter>> counters_;
  std::unordered_map<std::string, std::unique_ptr<Histogram>> histograms_;
};

template <typename T>
void Statistics::AggregateOne(
    std::unordered_map<std::string, std::unique_ptr<T>>* dst,
    const std::unordered_map<std::string, std::unique_ptr<T>>& src) {
  for (const auto& stat : src) {
    const std::string& name = stat.first;
    auto it = dst->find(name);
    if (it == dst->end()) {
      // Stat with this name doesn't exist, so add a copy.
      (*dst)[name] = std::unique_ptr<T>(new T(*stat.second.get()));
    } else {
      // Stat already exists, so call Aggregate.
      it->second->Aggregate(*stat.second);
    }
  }
}

}  // namespace rocketspeed
