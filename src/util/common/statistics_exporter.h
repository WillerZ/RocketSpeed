//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "include/BaseEnv.h"
#include "src/port/port.h"

namespace rocketspeed {

class Logger;
class Statistics;
class StatisticsVisitor;

typedef std::function<Statistics()> StatisticsQuery;

struct StatisticsWindow {
  /// Number of ticks inside the window. e.g. if tick duration is 1 minute,
  /// and ticks is 10, then window is for statistics over 10 minutes.
  unsigned int ticks;

  /// Suffix appended to statistics names.
  std::string suffix;

  /// Default windows.
  /// {{1, ".60"}, {10, ".600"}, {60, ".3600"}}
  static std::vector<StatisticsWindow> kDefaults;
};

class StatisticsExporter final {
 public:
  // Noncopyable & nonmovable
  StatisticsExporter(const StatisticsExporter&) = delete;
  StatisticsExporter& operator=(const StatisticsExporter&) = delete;
  StatisticsExporter(StatisticsExporter&&) = delete;
  StatisticsExporter& operator=(StatisticsExporter&&) = delete;

  /**
   * Starts a thread that will export statistics returned by statistic_query
   * every minute. In addition to exporting the raw statistics, statistics over
   * windowed ranges of the last minute, 10 minutes, and hour. For example,
   * for a counter foo, the following will be exported (with default windows):
   *
   *   foo
   *   foo.60
   *   foo.600
   *   foo.3600
   *
   * @param env Environment
   * @param info_log For logging.
   * @param statistics_query Query to capture current statistics.
   * @param visitor Visitor for all computed statistics. Will be invoked each
   *                tick duration on a stable thread.
   * @param windows Sizes and suffixes to use for rolling windows.
   * @param tick_duration How often to wake up and export statistics.
   */
  explicit StatisticsExporter(BaseEnv* env,
                              std::shared_ptr<Logger> info_log,
                              StatisticsQuery statistics_query,
                              StatisticsVisitor* visitor,
                              std::vector<StatisticsWindow> windows =
                                  StatisticsWindow::kDefaults,
                              std::chrono::milliseconds tick_duration =
                                  std::chrono::minutes(1));

  /** Stops thread and waits for join. */
  ~StatisticsExporter();

 private:
  BaseEnv* env_;
  BaseEnv::ThreadId thread_;
  port::Semaphore shutdown_signal_;
};

} // namespace rocketspeed
