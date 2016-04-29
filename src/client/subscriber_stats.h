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
    unsubscribes_invalid_handle =
        all.AddCounter(prefix + "unsubscribes_invalid_handle");
  }

  Counter* active_subscriptions;
  Counter* unsubscribes_invalid_handle;
  Statistics all;
};

}  // namespace rocketspeed
