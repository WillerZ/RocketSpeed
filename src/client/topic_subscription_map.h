// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <cstddef>
#include <functional>
#include <vector>

#include "include/Slice.h"
#include "include/Types.h"

namespace rocketspeed {

class SubscriptionID;

class TopicToSubscriptionMap {
 public:
  explicit TopicToSubscriptionMap(
      std::function<bool(SubscriptionID, NamespaceID*, Topic*)> get_topic);

  SubscriptionID Find(Slice namespace_id, Slice topic_name) const;

  void Insert(Slice namespace_id, Slice topic_name, SubscriptionID sub_id);

  bool Remove(Slice namespace_id, Slice topic_name, SubscriptionID sub_id);

 private:
  /**
   * Extract namespace and topic name for given ID and returns true or
   * returns false if the subscription with the ID doesn't exist.
   */
  const std::function<bool(SubscriptionID, NamespaceID*, Topic*)> get_topic_;
  /**
   * A linear hashing scheme to map namespace and topic to the ID of the only
   * upstream subscription.
   */
  std::vector<SubscriptionID> vector_;
  /**
   * Cached allowed range of the number of upstream subscriptions for current
   * size of the open hashing data structure.
   */
  size_t sub_count_low_;
  size_t sub_count_high_;
  /**
   * Number of elements in the upstream_subscriptions_ vector. This might
   * diverge from the number of subscriptions known by the underlying subscriber
   * on certain occasion, and would be tricky to be kept the same at akk times.
   */
  size_t sub_count_;

  void InsertInternal(Slice namespace_id,
                      Slice topic_name,
                      SubscriptionID sub_id);

  size_t FindOptimalPosition(Slice namespace_id, Slice topic_name) const;

  void Rehash();

  bool NeedsRehash() const;
};

}  // namespace rocketspeed
