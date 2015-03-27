// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "include/Types.h"
#include "src/util/hostmap.h"
#include "src/util/topic_uuid.h"
#include "src/util/common/autovector.h"

namespace rocketspeed {

class Logger;
class TopicTailer;

class TopicSubscription {
 public:
  TopicSubscription(HostNumber hostnum, SequenceNumber seqno)
  : hostnum_(hostnum)
  , seqno_(seqno) {
  }

  HostNumber GetHostNum() const {
    return hostnum_;
  }

  SequenceNumber GetSequenceNumber() const {
    return seqno_;
  }

  void SetSequenceNumber(SequenceNumber seqno) {
    seqno_ = seqno;
  }

 private:
  HostNumber hostnum_;
  SequenceNumber seqno_;
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
  /**
   * The Topic Manager does not own the TopicTailer. But it needs to
   * interact with the TopicTailer to be able to seek to appropriate
   * sequence numbered messages stored in the Storage.
   *
   * @param tailer Controlled (but not owned) tailer to serve subscriptions.
   * @param info_log For logging.
   */
  explicit TopicManager(TopicTailer* tailer,
                        std::shared_ptr<Logger> info_log);

  virtual ~TopicManager();

  // Add a new subscriber to the topic.
  Status AddSubscriber(const TopicUUID& topic,
                       SequenceNumber start,
                       HostNumber subscriber);

  // Remove an existing subscriber for a topic
  Status RemoveSubscriber(const TopicUUID& topic,
                          HostNumber subscriber);

  /**
   * Visits the list of subscribers for a specific topic and sequence number
   * range. The visitor will be called for all subscription where the sequence
   * number is not less than 'from', and not greater than 'to'. The visitation
   * order is unspecified.
   *
   * @param topic Topic UUID.
   * @param from Lower threshold of subscriptions.
   * @param to Upper threshold of subscriptions.
   * @param visitor Visiting function for subscriptions. Mutation is allowed.
   * @return List of subscribers on that topic at that sequence number, or
   *         null if there are no subscribers.
   */
  void VisitSubscribers(const TopicUUID& topic,
                        SequenceNumber from,
                        SequenceNumber to,
                        std::function<void(TopicSubscription*)> visitor);

 private:
  // Map a topic name to a list of TopicEntries.
  std::unordered_map<TopicUUID, TopicList> topic_map_;

  TopicTailer* tailer_;

  std::shared_ptr<Logger> info_log_;
};

}  // namespace rocketspeed
