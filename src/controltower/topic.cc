//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/topic.h"

#include <utility>
#include <vector>

#include "include/Logger.h"
#include "src/controltower/topic_tailer.h"

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

TopicManager::TopicManager(TopicTailer* tailer,
                           std::shared_ptr<Logger> info_log)
: tailer_(tailer)
, info_log_(std::move(info_log)) {
}

TopicManager::~TopicManager() {
  // The TopicTailer is not owned by the TopicManager
}

// Add a new subscriber to the topic. The name of the topic and the
// start sequence number from where to start the subscription is
// specified by the caller.
Status
TopicManager::AddSubscriber(const TopicUUID& topic,
                            SequenceNumber start,
                            HostNumber subscriber) {
  bool newsubscriber = UpdateSubscription(topic_map_[topic], subscriber, start);
  if (!newsubscriber) {
    tailer_->StopReading(topic, subscriber);
  }
  Status st = tailer_->StartReading(topic, start, subscriber);
  if (!st.ok()) {
    LOG_ERROR(info_log_, "Failed to reseek tailer (%s)",
              st.ToString().c_str());
  }
  return st;
}

// remove a subscriber to the topic
Status
TopicManager::RemoveSubscriber(const TopicUUID& topic, HostNumber subscriber) {
  Status status;
  // find list of subscribers for this topic
  auto iter = topic_map_.find(topic);
  if (iter != topic_map_.end()) {
    if (RemoveSubscription(iter->second, subscriber)) {
      status = tailer_->StopReading(topic, subscriber);
    }
  }
  return status;
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

}  // namespace rocketspeed
