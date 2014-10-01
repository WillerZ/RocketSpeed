//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/topic.h"
#include <utility>
#include <vector>
#include "src/controltower/tailer.h"

namespace rocketspeed {

TopicManager::TopicManager(const Tailer* tailer) : tailer_(tailer) {
}

TopicManager::~TopicManager() {
  // The Tailer is not owned by the TopicManager
}

// Add a new subscriber to the topic. The name of the topic and the
// start sequence number from where to start the subscription is
// specified by the caller.
Status
TopicManager::AddSubscriber(const NamespaceTopic& topic, SequenceNumber start,
                            LogID logid, HostNumber subscriber,
                            unsigned int roomnum) {
  std::unordered_map<NamespaceTopic, unique_ptr<TopicList>>::iterator iter =
                                      topic_map_.find(topic);
  // This is the first time we are receiving any subscription
  // request for this topic
  if (iter == topic_map_.end()) {
    TopicList* ll = new TopicList();
    std::unique_ptr<TopicList> list(ll);
    ll->insert(subscriber);

    auto ret = topic_map_.insert(
                 std::pair<NamespaceTopic, std::unique_ptr<TopicList>>
                 (topic, std::move(list)));
    assert(ret.second);  // inserted successfully
    if (!ret.second) {
      return Status::InternalError("TopicManager::AddSubscriber "
                                   "Unable to add subscriber.");
    }
  } else {
    // There are some pre-existing subscriptions for this topic.
    // Insert new subscriber.
    TopicList* list = iter->second.get();
    list->insert(subscriber);
  }
  Status st = ReseekIfNeeded(logid, start, roomnum);
  assert(st.ok());
  return st;
}

// remove a subscriber to the topic
Status
TopicManager::RemoveSubscriber(const NamespaceTopic& topic, LogID logid,
                               HostNumber subscriber,
                               unsigned int roomnum) {
  // find list of subscribers for this topic
  std::unordered_map<NamespaceTopic, unique_ptr<TopicList>>::iterator iter =
                                      topic_map_.find(topic);
  if (iter != topic_map_.end()) {
    // remove this subscriber from list
    TopicList* list = iter->second.get();
    list->erase(list->find(subscriber));
  }
  return Status::OK();
}

// Re-position the Storage read-point if necessary
Status
TopicManager::ReseekIfNeeded(LogID logid, SequenceNumber start,
                             unsigned int roomnum) {
  Status st;
  SequenceNumber current = 0;
  std::unordered_map<LogID, SequenceNumber>::iterator iter =
                                      last_read_.find(logid);
  if (iter != last_read_.end()) {
    current = iter->second;
  }
  // If the new starting seqno is smaller than the current one,
  // then we need to reseek from Storage
  if (start != 0 && (start <= current || current == 0)) {
    st = tailer_->StartReading(logid, start, roomnum);
  }
  return st;
}

// Updates the last seqno read from Storage
void
TopicManager::SetLastRead(LogID logid, SequenceNumber seqno) {
  assert(seqno > 0);
  last_read_[logid] = seqno;
}

// Returns the list of subscribers for a specified topic.
// Returns null if there are no subscribers.
TopicList*
TopicManager::GetSubscribers(const NamespaceTopic& topic) {
  std::unordered_map<NamespaceTopic, unique_ptr<TopicList>>::iterator iter =
                                      topic_map_.find(topic);
  if (iter != topic_map_.end()) {
    return iter->second.get();
  }
  return nullptr;
}

}  // namespace rocketspeed
