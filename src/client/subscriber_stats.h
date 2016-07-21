/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "src/util/common/statistics.h"

namespace rocketspeed {

class SubscriberStats {
 public:
  explicit SubscriberStats(const std::string& prefix) {
    active_subscriptions = all.AddCounter(prefix + "active_subscriptions");
    hb_timeouts = all.AddCounter(prefix + "hb_timeouts");
    router_version_checks = all.AddCounter(prefix + "router_version_checks");
    router_version_changes = all.AddCounter(prefix + "router_version_changes");
    unsubscribes_invalid_handle =
        all.AddCounter(prefix + "unsubscribes_invalid_handle");
  }

  Counter* active_subscriptions;
  Counter* hb_timeouts;
  Counter* router_version_checks;
  Counter* router_version_changes;
  Counter* unsubscribes_invalid_handle;
  Statistics all;
};

}  // namespace rocketspeed
