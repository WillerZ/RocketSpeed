// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/RocketSpeed.h"
#include "src/messages/messages.h"
#include "src/util/cache.h"
#include "src/util/filter_policy.h"
#include "src/util/storage.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

// Create a new cache with a fixed size capacity.
//
extern std::shared_ptr<Cache> NewDataCache(size_t capacity);

class DataCache {
 friend class CacheEntry;
 friend class _Test_Check;
 friend class _Test_CheckBloom;
 public:
  DataCache(size_t size_in_bytes,
            bool cache_data_from_system_namespaces, // true: cache system ns
            int bloom_bits_per_msg,                 // bits per message
            size_t block_size);                     // # of messages in a block
  ~DataCache();

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
  size_t GetUsage() const;

  // Gets the current configured capacity of the cache
  size_t GetCapacity() const;

  // Gets the statistics for this cache
  Statistics GetStatistics() const;

  // Deliver data from cache starting from 'start' as much as possible.
  // Returns the first sequence number that was not found in the cache.
  // The caller is interested in only those topics specified by topic_name.
  // If the message was successfully processed by the caller, then the
  // caller should set processed to true.
  // Will stop visiting the cache when on_message returns false.
  SequenceNumber VisitCache(LogID logid,
                            SequenceNumber start,
                            const Slice& topic_name,
                            std::function<bool(MessageData* data_raw,
                                          bool* processed)> on_message);

  // Checks if there is an entry at a specific position.
  bool HasEntry(LogID logid, SequenceNumber seqno) const;

 private:
  // number of bloom bits per message
  int bloom_bits_per_msg_;

  // The number of messages in a single block in the cache
  size_t block_size_;

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

  // The bloom filter used for this cache
  std::unique_ptr<FilterPolicy> bloom_filter_;

  size_t GetBlockSize() const { return block_size_; }

  // Collect statistics about cache lookups
  struct Stats {
    Stats() {
      const std::string prefix = "tower.data_cache.";

      cache_hits =
        all.AddCounter(prefix + "cache_hits");
      cache_misses =
        all.AddCounter(prefix + "cache_misses");
      cache_inserts =
        all.AddCounter(prefix + "cache_inserts");
      bloom_hits =
        all.AddCounter(prefix + "bloom_hits");
      bloom_misses =
        all.AddCounter(prefix + "bloom_misses");
      bloom_inserts =
        all.AddCounter(prefix + "bloom_inserts");
      bloom_falsepositives =
        all.AddCounter(prefix + "bloom_falsepositives");
    }

    Statistics all;
    Counter* cache_hits;    // number of records read from cache
    Counter* cache_misses;  // number of instances cache returned no records
    Counter* cache_inserts; // number of records inserted into cache
    Counter* bloom_hits;    // number of times blooms are useful
    Counter* bloom_misses;  // number of times blooms are not useful
    Counter* bloom_inserts; // number of blooms computed
    Counter* bloom_falsepositives;// number of times blooms said that topic
                            // exist but the topic did-not exist in the cache
  } stats_;
};

}  // namespace rocketspeed
