//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/topic.h"

#include <utility>
#include <vector>

namespace rocketspeed {

/// @return true iff new subscription was inserted.
static bool UpdateSubscription(TopicList& list,
                               HostNumber hostnum,
                               SequenceNumber seqno) {
  for (TopicSubscription& sub : list) {
    if (sub.GetHostNum() == hostnum) {
      sub.SetSequenceNumber(seqno);
      return false;
    }
  }
  list.emplace_back(hostnum, seqno);
  return true;
}

/// @return true iff subscription was removed.
static bool RemoveSubscription(TopicList& list,
                               HostNumber hostnum) {
  for (auto it = list.begin(); it != list.end(); ++it) {
    if (it->GetHostNum() == hostnum) {
      list.erase(it);
      return true;
    }
  }
  return false;
}

// Add a new subscriber to the topic. The name of the topic and the
// start sequence number from where to start the subscription is
// specified by the caller.
bool
TopicManager::AddSubscriber(const TopicUUID& topic,
                            SequenceNumber start,
                            HostNumber subscriber) {
  return UpdateSubscription(topic_map_[topic], subscriber, start);
}

// remove a subscriber to the topic
bool
TopicManager::RemoveSubscriber(const TopicUUID& topic, HostNumber subscriber) {
  // find list of subscribers for this topic
  auto iter = topic_map_.find(topic);
  if (iter != topic_map_.end()) {
    return RemoveSubscription(iter->second, subscriber);
  }
  return false;
}

void TopicManager::VisitSubscribers(
    const TopicUUID& topic,
    SequenceNumber from,
    SequenceNumber to,
    std::function<void(TopicSubscription*)> visitor) {
  auto iter = topic_map_.find(topic);
  if (iter != topic_map_.end()) {
    for (TopicSubscription& sub : iter->second) {
      if (sub.GetSequenceNumber() >= from && sub.GetSequenceNumber() <= to) {
        visitor(&sub);
      }
    }
  }
}

void TopicManager::VisitTopics(
    std::function<void(const TopicUUID& topic)> visitor) {
  for (auto& topic_sub : topic_map_) {
    visitor(topic_sub.first);
  }
}

}  // namespace rocketspeed
