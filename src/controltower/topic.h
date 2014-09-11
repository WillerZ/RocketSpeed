// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "./Types.h"
#include "src/util/storage.h"
#include "src/util/hostmap.h"

namespace rocketspeed {

class Tailer;
typedef std::unordered_set<HostNumber> TopicList;

//
// The Topic Manager maintains information between topics
// and its subscribers.
//
class TopicManager {
 public:
  // The Topic Manager does not own the Tailer. But it needs to
  // interact with the Tailer to be able to seek to appropriate
  // sequence numbered messages stored in the Storage.
  explicit TopicManager(const Tailer* tailer);

  virtual ~TopicManager();

  // Add a new subscriber to the topic.
  Status AddSubscriber(const Topic& topic, SequenceNumber start, LogID logid,
                       HostNumber subscriber, unsigned int roomnum);

  // Remove an existing subscriber for a topic
  Status RemoveSubscriber(const Topic& topic, LogID logid,
                          HostNumber subscriber, unsigned int roomnum);

  // Sets the seqno last read from log
  void SetLastRead(LogID logid, SequenceNumber seqno);

  // Returns the list of subscribers for a specific topic
  TopicList* GetSubscribers(const Topic& topic_name);

 private:
  // Map a topic name to a list of TopicEntries.
  std::unordered_map<Topic, std::unique_ptr<TopicList>>  topic_map_;

  // Map of logid to the last seqno already read from the storage
  std::unordered_map<LogID, SequenceNumber> last_read_;

  const Tailer* tailer_;

  Status ReseekIfNeeded(LogID logid, SequenceNumber start,
                        unsigned int roomid);
};

}  // namespace rocketspeed
