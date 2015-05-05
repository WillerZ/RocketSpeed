// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "statistics.h"

#include <cmath>

#include <algorithm>
#include <limits>
#include <string>

#include "src/util/common/thread_check.h"

namespace rocketspeed {

std::string Counter::Report() const {
  return std::to_string(count_);
}

Histogram::Histogram(double min,
                     double max,
                     double smallest_bucket,
                     double ratio)
: min_(min)
, max_(max)
, smallest_bucket_(smallest_bucket)
, ratio_(ratio)
, num_samples_(0) {
  assert(max >= min);
  assert(ratio > 1.0);
  num_buckets_ = BucketIndex(max) + 1;
  bucket_counts_.reset(new uint64_t[num_buckets_]);
  for (size_t i = 0; i < num_buckets_; ++i) {
    bucket_counts_[i] = 0;
  }
}

Histogram::Histogram(const Histogram& src)
: min_(src.min_)
, max_(src.max_)
, smallest_bucket_(src.smallest_bucket_)
, ratio_(src.ratio_)
, num_samples_(0)
, num_buckets_(src.num_buckets_) {
  // Copy the size then use the existing Aggregate code to copy.
  bucket_counts_.reset(new uint64_t[num_buckets_]);
  for (size_t i = 0; i < num_buckets_; ++i) {
    bucket_counts_[i] = 0;
  }
  Aggregate(src);
}

Histogram::Histogram(Histogram&& src)
: min_(src.min_)
, max_(src.max_)
, smallest_bucket_(src.smallest_bucket_)
, ratio_(src.ratio_)
, num_samples_(src.num_samples_)
, bucket_counts_(std::move(src.bucket_counts_))
, num_buckets_(src.num_buckets_) {
  src.bucket_counts_.reset(new uint64_t[num_buckets_]);
  src.thread_check_.Reset();
  for (size_t i = 0; i < num_buckets_; ++i) {
    src.bucket_counts_[i] = 0;
  }
  src.num_samples_ = 0;
}

void Histogram::Record(double sample) {
  thread_check_.Check();
  size_t index = std::min(BucketIndex(sample), num_buckets_ - 1);
  bucket_counts_[index] += 1;
  num_samples_ += 1;
}

size_t Histogram::BucketIndex(double sample) const {
  // Compute the log of sample_real in base ratio_.
  // This formula puts samples below min_ + smallest_bucket_ into bucket 0
  // Samples up to min_ + smallest_bucket_ * ratio_ go to bucket 1
  // Samples up to min_ + smallest_bucket_ * ratio_ ^ N go to bucket N
  // and so on.
  sample = std::max(0.0, std::min(sample, max_) - min_);
  double log_ratio = log(ratio_);
  int32_t bucket = static_cast<int32_t>(
    (log_ratio + log(sample) - log(smallest_bucket_)) / log_ratio);
  if (bucket < 0) {
    return 0;
  }
  return bucket;
}

double Histogram::Percentile(double p) const {
  thread_check_.Check();
  assert(p >= 0.0);
  assert(p <= 1.0);

  if (num_samples_ == 0) {
    return min_;
  }

  size_t index = static_cast<size_t>(static_cast<double>(num_samples_) * p);
  if (index >= num_samples_) {
    index = num_samples_ - 1;
  }

  for (size_t bucket = 0; bucket < num_buckets_; ++bucket) {
    size_t count = bucket_counts_[bucket];
    if (index > count || count == 0) {
      // Percentile does not lie in this bucket.
      index -= count;
    } else {
      // Percentile lies in this bucket.
      double end = smallest_bucket_ * pow(ratio_, static_cast<double>(bucket));
      double start = static_cast<double>(bucket) == 0 ? 0.0 : end / ratio_;
      double t = (static_cast<double>(index) + 1.0) /
                 (static_cast<double>(count) + 1.0);  // interpolant
      return min_ + start + (end - start) * t;
    }
  }
  // This shouldn't be possible.
  assert(false);
  return 0.0;
}

void Histogram::Aggregate(const Histogram& histogram) {
  thread_check_.Check();
  histogram.thread_check_.Check();

  // Parameters must match exactly for histograms to aggregate.
  assert(histogram.min_ == min_);
  assert(histogram.max_ == max_);
  assert(histogram.smallest_bucket_ == smallest_bucket_);
  assert(histogram.ratio_ == ratio_);
  assert(histogram.num_buckets_ == num_buckets_);

  // Just sum up the bucket counts and number of samples.
  for (size_t i = 0; i < num_buckets_; ++i) {
    uint64_t n = histogram.bucket_counts_[i];
    bucket_counts_[i] += n;
    num_samples_ += n;
  }
}

std::string Histogram::Report() const {
  thread_check_.Check();
  // Reports the p50, p90, p99, and p99.9 percentiles.
  char buffer[256];
  snprintf(buffer, 256, "p50: %-8.1lf  "
                        "p90: %-8.1lf  "
                        "p99: %-8.1lf  "
                        "p99.9: %-8.1lf  "
                        "(%llu samples)",
    Percentile(0.50), Percentile(0.90), Percentile(0.99), Percentile(0.999),
    static_cast<long long unsigned int>(num_samples_));
  return std::string(buffer);
}

void Statistics::Aggregate(const Statistics& stats) {
  thread_check_.Check();
  AggregateOne(&counters_, stats.counters_);
  AggregateOne(&histograms_, stats.histograms_);
}

Counter* Statistics::AddCounter(const std::string& name) {
  thread_check_.Check();
  counters_[name] = std::unique_ptr<Counter>(new Counter());
  return counters_[name].get();
}

Histogram* Statistics::AddHistogram(const std::string& name,
                                    double min,
                                    double max,
                                    double smallest_bucket,
                                    double bucket_ratio) {
  thread_check_.Check();
  histograms_[name] = std::unique_ptr<Histogram>(
    new Histogram(min, max, smallest_bucket, bucket_ratio));
  return histograms_[name].get();
}

Histogram* Statistics::AddLatency(const std::string& name) {
  thread_check_.Check();
  histograms_[name] = std::unique_ptr<Histogram>(
    new Histogram(0, 1e12, 1.0, 1.1));
  return histograms_[name].get();
}

std::string Statistics::Report() const {
  thread_check_.Check();
  std::vector<std::string> reports;
  size_t width = 40;

  // Add all counters to the report.
  for (const auto& stat : counters_) {
    size_t padding = width - std::min(width, stat.first.size());
    reports.emplace_back(stat.first +
                         ": " + std::string(padding, ' ') +
                         stat.second->Report());
  }

  // Add all histograms to the report.
  for (const auto& stat : histograms_) {
    size_t padding = width - std::min(width, stat.first.size());
    reports.emplace_back(stat.first +
                         ": " + std::string(padding, ' ') +
                         stat.second->Report());
  }

  // Sort the strings (effectively sorting by statistic name).
  std::sort(reports.begin(), reports.end());

  // Generate concatenated report.
  std::string report;
  for (const auto& line : reports) {
    report += line;
    report += '\n';
  }
  return report;
}

Statistics::Statistics(const Statistics &s) {
  // Deep copy of statistics
  for (auto &p : s.counters_) {
    counters_.emplace(
      p.first,
      std::unique_ptr<Counter>(new Counter(*p.second.get()))
    );
  }

  for (auto &p : s.histograms_) {
    histograms_.emplace(
      p.first,
      std::unique_ptr<Histogram>(new Histogram(*p.second.get()))
    );
  }
}

Statistics::Statistics(Statistics&& src)
: counters_(std::move(src.counters_))
, histograms_(std::move(src.histograms_)) {
}

Statistics Statistics::MoveThread() {
  Statistics stats = std::move(*this);
  for (auto& p : stats.counters_) {
    p.second.reset(new Counter(std::move(*p.second)));
  }
  for (auto& p : stats.histograms_) {
    p.second.reset(new Histogram(std::move(*p.second)));
  }
  return stats;
}

}  // namespace rocketspeed
