// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "include/Types.h"
#include "src/util/topic_uuid.h"
#include "src/util/common/autovector.h"
#include "src/util/common/thread_check.h"
#include "src/controltower/tower.h"

namespace rocketspeed {

class TopicSubscription {
 public:
  TopicSubscription(CopilotSub id, SequenceNumber seqno)
  : id_(id)
  , seqno_(seqno) {
  }

  CopilotSub GetID() const {
    return id_;
  }

  SequenceNumber GetSequenceNumber() const {
    return seqno_;
  }

  void SetSequenceNumber(SequenceNumber seqno) {
    seqno_ = seqno;
  }

 private:
  CopilotSub id_;
  SequenceNumber seqno_;  // next expected seqno
};

// Set of subscriptions for a topic.
//
// The vast majority of the time, a particular topic will only have one
// subscriber. In the worst case, the number of subscribers will be the
// number of copilots, which will be on the order of 100s or maybe 1000s.
// In the worst case, the number of messages being fanned out to those
// subscribers will likely be low, so we can manage the linear search.
// Memory usage is more important in general.
typedef autovector<TopicSubscription, 1> TopicList;

//
// The Topic Manager maintains information between topics
// and its subscribers. The topic name is actually
// the NamespaceId concatenated with the user-specified topic name.
//
class TopicManager {
 public:
  TopicManager() {}
  ~TopicManager() = default;

  /**
   * Add a new subscriber to the topic.
   *
   * @return true iff new subscriber.
   */
  bool AddSubscriber(const TopicUUID& topic,
                     SequenceNumber start,
                     CopilotSub subscriber);

  /**
   * Remove an existing subscriber for a topic.
   *
   * @return true iff no subscribers left on this topic.
   */
  bool RemoveSubscriber(const TopicUUID& topic,
                        CopilotSub subscriber);

  /**
   * Visits the list of subscribers for a specific topic and sequence number
   * range. The visitor will be called for all subscriptions where the sequence
   * number is not less than 'from', and not greater than 'to'. The visitation
   * order is unspecified.
   *
   * @param topic Topic UUID.
   * @param from Lower threshold of subscriptions.
   * @param to Upper threshold of subscriptions.
   * @param visitor Visiting function for subscriptions. Mutation is allowed.
   */
  template <typename Visitor>
  void VisitSubscribers(const TopicUUID& topic,
                        SequenceNumber from,
                        SequenceNumber to,
                        const Visitor& visitor);

  /**
   * Visits the list of topics with subscribers.
   *
   * @param visitor Visiting function for topics.
   */
  template <typename Visitor>
  void VisitTopics(const Visitor& visitor);

 private:
  // Map a topic name to a list of TopicEntries.
  std::unordered_map<TopicUUID, TopicList> topic_map_;
  ThreadCheck thread_check_;
};

template <typename Visitor>
void TopicManager::VisitSubscribers(
    const TopicUUID& topic,
    SequenceNumber from,
    SequenceNumber to,
    const Visitor& visitor) {
  thread_check_.Check();
  auto iter = topic_map_.find(topic);
  if (iter != topic_map_.end()) {
    for (TopicSubscription& sub : iter->second) {
      if (sub.GetSequenceNumber() >= from && sub.GetSequenceNumber() <= to) {
        visitor(&sub);
      }
    }
  }
}

template <typename Visitor>
void TopicManager::VisitTopics(const Visitor& visitor) {
  thread_check_.Check();
  for (auto it = topic_map_.begin(); it != topic_map_.end(); ) {
    // We save next here to allow visitor to RemoveSubscribers on this topic.
    auto next = std::next(it);
    if (!visitor(it->first)) {
      break;
    }
    it = next;
  }
}

}  // namespace rocketspeed
