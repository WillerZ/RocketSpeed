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
  bool newsubscriber = true;
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
    newsubscriber = list->insert(subscriber).second;
  }
  Status st = ReseekIfNeeded(logid, start, roomnum, newsubscriber);
  assert(st.ok());
  return st;
}

// remove a subscriber to the topic
Status
TopicManager::RemoveSubscriber(const NamespaceTopic& topic, LogID logid,
                               HostNumber subscriber,
                               unsigned int roomnum) {
  Status status;
  // find list of subscribers for this topic
  std::unordered_map<NamespaceTopic, unique_ptr<TopicList>>::iterator iter =
                                      topic_map_.find(topic);
  if (iter != topic_map_.end()) {
    // remove this subscriber from list
    TopicList* list = iter->second.get();
    assert(list);
    auto list_iter = list->find(subscriber);
    assert(list_iter != list->end());
    if (list_iter != list->end()) {

      // remove subscriber from TopicList
      list->erase(list_iter);

      // Update the number of subscribers of this log
      std::unordered_map<LogID, LogData>::iterator it =
                                      logdata_.find(logid);
      assert(it != logdata_.end() &&
             it->second.num_subscribers > 0);
      it->second.num_subscribers--;

      // If there are no more subscribers for this log, then close log
      if (it->second.num_subscribers == 0) {
        logdata_.erase(it);
        status = tailer_->StopReading(logid, roomnum);
      }
    }
  }
  return status;
}

// Re-position the Storage read-point if necessary
Status
TopicManager::ReseekIfNeeded(LogID logid, SequenceNumber start,
                             unsigned int roomnum, bool newsubscriber) {
  Status st;
  SequenceNumber current = 0;
  std::unordered_map<LogID, LogData>::iterator iter =
                                      logdata_.find(logid);
  assert((iter != logdata_.end()) || newsubscriber);

  if (iter != logdata_.end()) {
    current = iter->second.last_read;
    // Increment the number of unique subscribers for this log
    if (newsubscriber) {
      iter->second.num_subscribers++;
    }
  } else if (newsubscriber) {
    // Increment the number of unique subscribers for this log
    logdata_[logid].num_subscribers++;
  }
  // If the new starting seqno is smaller than the current one,
  // then we need to reseek from Storage
  if (start != 0 && (start <= current || current == 0)) {
    st = tailer_->StartReading(logid, start, roomnum);
    SetLastRead(logid, start - 1);
  }
  return st;
}

// Retrieves the last seqno read from Storage
SequenceNumber
TopicManager::GetLastRead(LogID logid) {
  auto it = logdata_.find(logid);
  return it != logdata_.end() ? it->second.last_read : SequenceNumber(-1);
}

// Updates the last seqno read from Storage
void
TopicManager::SetLastRead(LogID logid, SequenceNumber seqno) {
  assert(seqno != SequenceNumber(-1));
  logdata_[logid].last_read = seqno;
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
