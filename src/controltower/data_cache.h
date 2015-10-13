// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/RocketSpeed.h"
#include "src/messages/messages.h"
#include "src/util/cache.h"
#include "src/util/storage.h"

namespace rocketspeed {

// Create a new cache with a fixed size capacity.
//
extern std::shared_ptr<Cache> NewDataCache(size_t capacity);

class DataCache {
 public:
  DataCache(size_t size_in_bytes, bool cache_data_from_system_namespaces);

  // Sets a new capacity for the cache. Evict data from cache if the
  // current usage exceeds the specified capacity.
  void SetCapacity(size_t size_in_bytes);

  // store gap message into cache
  void StoreGap(LogID log_id, GapType type, SequenceNumber from,
                SequenceNumber to);

  // store data message into cache
  void StoreData(const Slice& namespace_id, const Slice& topic,
                 LogID log_id,
                 std::unique_ptr<MessageData> msg);

  // remove specified record from the cache
  void Erase(LogID log_id, GapType type, SequenceNumber seqno);

  // Removes the entire cache
  void ClearCache();

  // Gets the current usage of the cache
  size_t GetUsage();

  // Gets the current configured capacity of the cache
  size_t GetCapacity();

  // Deliver data from cache starting from 'start' as much as possible.
  // Returns the first sequence number that was not found in the cache.
  // Will stop visiting the cache when on_message returns false.
  SequenceNumber VisitCache(LogID logid,
                            SequenceNumber start,
                            std::function<bool(MessageData* data_raw)>
                              on_message);

 private:

  // What is the cache used for?
  enum Characteristics : unsigned int {
    StoreUserTopics   = (0x1<<0), // store data from topics in user namespace
    StoreSystemTopics = (0x1<<1), // store data from topics in system namespace
    StoreDataRecords  = (0x1<<2), // store data records in cache
    StoreGapRecords   = (0x1<<3), // store gap records in cache
  };

  // Character of this cache
  int characteristics_;

  std::shared_ptr<Cache> rs_cache_;
};

}  // namespace rocketspeed
