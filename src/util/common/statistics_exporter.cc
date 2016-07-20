// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/common/statistics_exporter.h"

#include <chrono>
#include <string>

#include "include/Types.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

namespace {
void ExportStatistics(const Statistics& stats,
                      std::string suffix,
                      StatisticsVisitor* visitor) {
  for (const auto& counter : stats.GetCounters()) {
    visitor->VisitCounter(counter.first + suffix, counter.second->Get());
  }
  for (const auto& histogram : stats.GetHistograms()) {
    double p50 = histogram.second->Percentile(0.50);
    double p90 = histogram.second->Percentile(0.90);
    double p99 = histogram.second->Percentile(0.99);
    double p999 = histogram.second->Percentile(0.999);
    const std::string& metric = histogram.first;
    visitor->VisitHistogram(metric + ".p50" + suffix, p50);
    visitor->VisitHistogram(metric + ".p90" + suffix, p90);
    visitor->VisitHistogram(metric + ".p99" + suffix, p99);
    visitor->VisitHistogram(metric + ".p999" + suffix, p999);
  }
}
}

std::vector<StatisticsWindow> StatisticsWindow::kDefaults = {
  {1, ".60"},    // last minute
  {10, ".600"},  // last 10 minutes
  {60, ".3600"}, // last hour
};

StatisticsExporter::StatisticsExporter(BaseEnv* env,
                                       StatisticsQuery statistics_query,
                                       StatisticsVisitor* visitor,
                                       std::vector<StatisticsWindow> windows,
                                       std::chrono::milliseconds tick_duration)
: env_(env) {
  thread_ = env_->StartThread(
    [this, statistics_query, visitor, windows, tick_duration]() {
    // Aggregators for each window size.
    std::vector<std::unique_ptr<StatisticsWindowAggregator>> aggregators;
    for (auto window : windows) {
      if (window.ticks == 1) {
        // Treat window size of 1 as special since we don't need to aggregate.
        aggregators.emplace_back(nullptr);
      } else {
        aggregators.emplace_back(new StatisticsWindowAggregator(window.ticks));
      }
    }

    Statistics previous_all_time;
    while (!shutdown_signal_.TimedWait(tick_duration)) {
      // All time stats.
      Statistics all_time = statistics_query();
      ExportStatistics(all_time, "", visitor);

      // Last tick stats.
      Statistics last_tick = all_time;
      last_tick.Disaggregate(previous_all_time);
      previous_all_time = std::move(all_time);

      // Aggregated stats.
      for (size_t i = 0; i < windows.size(); ++i) {
        auto aggregator = aggregators[i].get();
        const std::string& suffix = windows[i].suffix;
        if (aggregator) {
          aggregator->AddSample(last_tick);
          ExportStatistics(aggregator->GetAggregate(), suffix, visitor);
        } else {
          ExportStatistics(last_tick, suffix, visitor);
        }
      }

      visitor->Flush();
    }
  }, "stats-export");
}

StatisticsExporter::~StatisticsExporter() {
  if (thread_) {
    shutdown_signal_.Post();
    env_->WaitForJoin(thread_);
  }
}

} // namespace rocketspeed
