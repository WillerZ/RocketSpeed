/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <chrono>
#include <folly/dynamic.h>
#include <folly/Optional.h>
#include <folly/String.h>
#include "types.h"

namespace facebook { namespace logdevice {
class LogGroupAttributesImpl;

/**
 * All the info we can get on a log group
 */
class LogGroupAttributes {
public:
  /**
   * Returns the range of log IDs that form the log group.
   */
  std::pair<logid_t, logid_t> getLogRange();

  /**
   * Returns the symbolic name of the log group.
   */
  folly::fbstring getName();

  /**
   * Returns the number of nodes on which a record is persisted on write.
   */
  int getReplicationFactor();

  /**
   * The largest number of records not released for delivery that the sequencer
   * allows to be outstanding ('z' in the design doc).
   */
  int getMaxWritesInFlight();

  /**
   * Returns the retention period for records in the log group.
   */
  folly::Optional<std::chrono::seconds> getBacklogDuration();

  /**
   * Returns an attribute that isn't recognized by logdevice with the name
   * specified.
   */
  folly::dynamic getCustomField(const folly::dynamic &name);

  /**
   * Actual instances will be of the LogGroupAttributesImpl type, that inherits
   * from LogGroupAttributes. The destructor must be virtual in order to work
   * correctly.
   */
  virtual ~LogGroupAttributes() {}

private:
  LogGroupAttributes() { }
  // non-copyable && non-assignable
  LogGroupAttributes(const LogGroupAttributes&) = delete;
  LogGroupAttributes& operator= (const LogGroupAttributes&) = delete;
  friend class LogGroupAttributesImpl;
  LogGroupAttributesImpl *impl(); // downcasts (this)
};

}} // namespace
