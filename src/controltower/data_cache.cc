//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/controltower/data_cache.h"
#include <xxhash/xxhash.h>

#include <unordered_map>

namespace rocketspeed {

/*
 * Each Entry in the cache is an array of message pointers. The start
 * sequence number in an Entry is always a multiple of block_size_.
 * This is done so that the LRU cache can deal with fewer number of Entries
 * rather than having an Entry for each individual message.
 */

// The key for the LRU cache is 16 bytes, it is made up of a
// 8 byte logid folowed by a 8 byte seqno.
struct alignas(16) CacheKey {
  char buf[16];
};

// Find the first seqno of the block
static SequenceNumber AlignToBlockStart(
    size_t block_size, SequenceNumber seqno) {
  return seqno & ~(block_size - 1);
}

// Generate a key for cache lookup
static void GenerateKey(size_t block_size, LogID logid,
                        SequenceNumber seqno, CacheKey* buf) {
  RS_ASSERT(seqno == AlignToBlockStart(block_size, seqno));
  static_assert(sizeof(CacheKey) == sizeof(buf->buf),
                "Artificial padding found in CacheKey");
  static_assert(sizeof(logid) == 8,
                "Cache lookup assumes 8 byte logids");
  static_assert(sizeof(seqno) == 8,
                "Cache lookup assumes 8 byte sequence number");
  memcpy(static_cast<void *>(&(buf->buf[0])),
         static_cast<void *>(&logid),
         sizeof(logid));
  memcpy(static_cast<void *>(&(buf->buf[8])),
         static_cast<void *>(&seqno),
         sizeof(seqno));
}

//
// A CacheEntry stores data for a specified log
//
class CacheEntry {
 private:
#ifndef NO_RS_ASSERT
  LogID logid_;                // useful for debugging
  SequenceNumber seqno_block_; // useful for debugging
#endif /* NO_RS_ASSERT */
  std::string bloom_bits_;      // bloom filters are stored here
  unsigned short num_messages_; // number of msg in this entry
  std::unique_ptr<MessageData>* mcache_;

  // hcache_[i] stores the lower 16-bits of the hash of mcache_[i]'s topic name.
  // This allows quick strides across the cache without accessing the message.
  // Only 16-bits are stores to save memory.
  // If the entry is 0 then there is no message.
  std::unique_ptr<uint16_t[]> hcache_;

 public:
  explicit CacheEntry(DataCache* data_cache,
                      LogID logid, SequenceNumber seqno_block) {
#ifndef NO_RS_ASSERT
    logid_ = logid;
    seqno_block_ = seqno_block;
#endif /* NO_RS_ASSERT */
    num_messages_ = 0;
    RS_ASSERT((1U << 8 * sizeof(num_messages_)) >
      (data_cache->block_size_ + 1U));
    mcache_ = new std::unique_ptr<MessageData> [data_cache->block_size_];
    hcache_.reset(new uint16_t[data_cache->block_size_]);
    std::fill(hcache_.get(), hcache_.get() + data_cache->block_size_, 0);
  }

  ~CacheEntry() {
    delete [] mcache_;
  }

  uint16_t HashTopic(const Slice topic) {
    auto hash = XXH64(topic.data(), topic.size(), 0x3bd14b007c8b7733ULL);
    // 0 is special so we modulo to a smaller range.
    return static_cast<uint16_t>(hash % 0xFFFF + 1);
  }

  // Stores the specified record in this entry.
  // Returns the increase in charge, if any
  size_t StoreData(DataCache* data_cache,
                   const Slice& namespace_id, const Slice& topic,
                   LogID log_id,
                   const FilterPolicy* bloom_filter,
                   std::unique_ptr<MessageData> msg) {
    SequenceNumber seqno = msg->GetSequenceNumber();
    SequenceNumber seqno_block = AlignToBlockStart(
                     data_cache->block_size_, seqno);
    int offset = (int)(seqno - seqno_block);

#ifndef NO_RS_ASSERT
    RS_ASSERT(logid_ == log_id);
    RS_ASSERT(seqno_block_ == seqno_block);
#endif /* NO_RS_ASSERT */

    // if there already is an entry, then there is nothing more to do
    if (mcache_[offset] != nullptr) {
      RS_ASSERT(mcache_[offset].get()->GetSequenceNumber() ==
             msg->GetSequenceNumber());
      RS_ASSERT(mcache_[offset].get()->GetTotalSize() ==
             msg->GetTotalSize());
      RS_ASSERT(hcache_[offset] != 0);
      return 0;
    }
    RS_ASSERT(hcache_[offset] == 0);
    size_t size = msg->GetTotalSize();
    mcache_[offset] = std::move(msg); // store message
    hcache_[offset] = HashTopic(topic); // store hash
    num_messages_++;                  // one more message
    data_cache->stats_.cache_inserts->Add(1);

    // Update bloom filter when the block is full.
    // We use only the topic name and not the namespaceid to generate
    // blooms because it is likely that there are fewer number of
    // unique namespaceids in the system and the probabilty of a block
    // not having a single message from that namespace is probably small.
    if (num_messages_ == data_cache->block_size_ && bloom_filter) {
      RS_ASSERT(bloom_bits_.size() == 0);
      std::vector<Slice> topics;
      for (unsigned int i = 0; i <num_messages_; i++) {
        topics.push_back(mcache_[i]->GetTopicName());
      }
      bloom_filter->CreateFilter(topics, &bloom_bits_); // update blooms
      data_cache->stats_.bloom_inserts->Add(1);  // create one more bloom
    }
    return size;
  }

  // Remove specified record from the cache
  // Returns the decrease in charge, if any
  size_t Erase(DataCache* data_cache,
               LogID log_id, SequenceNumber seqno) {
    SequenceNumber seqno_block = AlignToBlockStart(
                   data_cache->block_size_, seqno);
    int offset = (int)(seqno - seqno_block);

    // TODO: Bloom filters are not updated on Erase
#ifndef NO_RS_ASSERT
    RS_ASSERT(logid_ == log_id);
    RS_ASSERT(seqno_block_ == seqno_block);
#endif /* NO_RS_ASSERT */

    // if there isn't an entry, then there is nothing more to do
    if (mcache_[offset] == nullptr) {
      RS_ASSERT(hcache_[offset] == 0);
      return 0;
    }
    size_t size = mcache_[offset]->GetTotalSize();
    mcache_[offset] = nullptr;        // erase
    hcache_[offset] = 0;
    return size;
  }

  // Visit the records starting from the specified seqno.
  // The caller is interested in looking at messages that belong to the
  // specified topic.
  SequenceNumber VisitEntry(
      DataCache* data_cache,
      LogID logid,
      SequenceNumber seqno,
      const FilterPolicy* bloom_filter,
      const Slice& lookup_topicname,
      bool* stop,
      const std::function<bool(MessageData* data_raw, bool* deliver)>& visit) {
    SequenceNumber seqno_block = AlignToBlockStart(
            data_cache->block_size_, seqno);
    size_t offset = seqno - seqno_block;
    bool did_bloom_check = false;

#ifndef NO_RS_ASSERT
    RS_ASSERT(logid_ == logid);
    RS_ASSERT(seqno_block_ == seqno_block);
#endif /* NO_RS_ASSERT */

    // If bloom filter enabled, then check bloom filter
    if (bloom_filter) {
      // If caller has specified a topic that it is interested in, then
      // use for bloom filter matching.
      if (lookup_topicname.size() > 0 && bloom_bits_.size() > 0) {
        if (!bloom_filter->KeyMayMatch(lookup_topicname, Slice(bloom_bits_))) {
          data_cache->stats_.bloom_hits->Add(1);  // successful use of blooms
          return seqno_block + data_cache->block_size_;  // record not found
        }
        did_bloom_check = true;
        data_cache->stats_.bloom_misses->Add(1);  // unsuccessful use of blooms
      }
    }

    // scan all messages upto either the first null or the entire block
    size_t index = offset;
    bool delivered = false;
    const auto block_size = data_cache->block_size_;
    const auto mcache = mcache_;
    const auto hcache = hcache_.get();
    if (lookup_topicname.size() == 0) {
      // Unknown lookup topic.
      // In this case, we visit every message in the cache.
      for (; index < block_size && mcache[index]; index++) {
        if (!visit(mcache[index].get(), &delivered)) {
          ++index;
          *stop = true;
          break;
        }
      }
    } else {
      // Lookup topic is known.
      // We can test the topic hash against hcache_ as a fail fast filter.
      // This avoids cache misses and CPU reading the message and visiting.
      const uint16_t hash = HashTopic(lookup_topicname);
      for (; index < block_size && hcache[index]; ++index) {
        if (hcache[index] == hash) {
          if (!visit(mcache[index].get(), &delivered)) {
            ++index;
            *stop = true;
            break;
          }
        }
      }
    }
    // found records in cache
    data_cache->stats_.cache_hits->Add(index - offset);

    // The bloom check said that the topic may exist in the Entry
    // but an exhaustive check proved that the topic does not really
    // exist in this Entry. This is a measure of bloom effectiveness.
    if (did_bloom_check && !delivered) {
      data_cache->stats_.bloom_falsepositives->Add(1);  // false positives
    }
    return seqno_block + index; // return the next seqno
  }

  bool HasEntry(size_t offset) {
    RS_ASSERT((mcache_[offset] == nullptr) == (hcache_[offset] == 0));
    return mcache_[offset] != nullptr;
  }

  size_t GetInitialCharge(DataCache* data_cache) {
    return sizeof(CacheEntry)
           + (data_cache->block_size_ * sizeof(mcache_[0]))
           + (data_cache->block_size_ * sizeof(hcache_[0]))
           + (data_cache->block_size_ * data_cache->bloom_bits_per_msg_)/8;
  }
};

// utility to release memory from the cache callback.
template <class T>
static void DeleteEntry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

DataCache::DataCache(size_t size_in_bytes,
                     bool cache_data_from_system_namespaces,
                     int bloom_bits_per_msg,
                     size_t block_size) :
  bloom_bits_per_msg_(bloom_bits_per_msg),
  block_size_(block_size),
  rs_cache_(size_in_bytes ? NewLRUCache(size_in_bytes) : nullptr) {
  characteristics_ = Characteristics::StoreUserTopics |
                     Characteristics::StoreSystemTopics |
                     Characteristics::StoreDataRecords |
                     Characteristics::StoreGapRecords;
  // block size must be power of 2.
  RS_ASSERT((block_size & (block_size - 1)) == 0);
  if (!cache_data_from_system_namespaces) {
    characteristics_ &= ~Characteristics::StoreSystemTopics;
  }
  if (bloom_bits_per_msg > 0) {
    bloom_filter_.reset(FilterPolicy::NewBloomFilterPolicy(bloom_bits_per_msg));
  }
}

DataCache::~DataCache() {
}

// create a new cache with the existing capacity
void DataCache::ClearCache() {
  if (rs_cache_ == nullptr) { // No caching specified
    return;
  }
  size_t capacity = rs_cache_->GetCapacity();
  rs_cache_ = NewLRUCache(capacity);
}

// sets a new cache size. If the newly set size is 0, then the
// cache is disabled.
void DataCache::SetCapacity(size_t capacity) {
  if (capacity == 0) {
    rs_cache_ = nullptr; // delete existing cache, if any
    return;
  }
  if (rs_cache_ != nullptr) {
    rs_cache_->SetCapacity(capacity);
  } else {
    rs_cache_ = NewLRUCache(capacity);
  }
}

size_t DataCache::GetCapacity() const {
  if (rs_cache_ == nullptr) { // No caching specified
    return 0;
  }
  return rs_cache_->GetCapacity();
}

size_t DataCache::GetUsage() const {
  if (rs_cache_ == nullptr) { // No caching specified
    return 0;
  }
  return rs_cache_->GetUsage();
}

void DataCache::StoreGap(LogID log_id, GapType type, SequenceNumber from,
                         SequenceNumber to) {
  if (rs_cache_ == nullptr) { // No caching specified
    return;
  }
  // Check to see if we do not need to store data
  if (!(characteristics_ & Characteristics::StoreGapRecords)) {
    return;
  } else {
  }
  // Currently, we are not storing the gaps into the cache
}

void DataCache::StoreData(const Slice& namespace_id, const Slice& topic,
                          LogID log_id,
                          std::unique_ptr<MessageData> msg) {
  if (rs_cache_ == nullptr) { // No caching specified
    return;
  }
  // Assert that no blocks are pinned
  RS_ASSERT(rs_cache_->GetPinnedUsage() == 0);

  // Check to see if we do not need to store data
  if (!(characteristics_ & Characteristics::StoreDataRecords)) {
    return;
  } else {
    if (IsReserved(namespace_id)) {
      if (!(characteristics_ & Characteristics::StoreSystemTopics)) {
        return;
      }
    } else if (!(characteristics_ & Characteristics::StoreUserTopics)) {
      return;
    }
  }
  // compute sequence number of block start
  SequenceNumber seqno = msg->GetSequenceNumber();
  SequenceNumber seqno_block = AlignToBlockStart(block_size_, seqno);

  // generate cache key
  CacheKey buffer;
  GenerateKey(block_size_, log_id, seqno_block, &buffer);
  Slice cache_key(buffer.buf, sizeof(buffer.buf));

  CacheEntry* entry = nullptr;

  // Fetch the appropriate entry from the cache
  Cache::Handle* handle = rs_cache_->Lookup(cache_key);
  if (handle) {
    entry = static_cast<CacheEntry *>(rs_cache_->Value(handle));
  } else {
    // Entry does not exist in the cache.
    // Create a new entry and insert into cache.
    entry = new CacheEntry(this, log_id, seqno_block);
    handle = rs_cache_->Insert(cache_key, entry,
                               entry->GetInitialCharge(this),
                               &DeleteEntry<CacheEntry>);
  }

  // Insert this record into the Entry
  size_t delta = entry->StoreData(this, namespace_id, topic, log_id,
                                  bloom_filter_.get(),
                                  std::move(msg));
  rs_cache_->ChargeDelta(handle, delta);
  rs_cache_->Release(handle);
}

// remove records from the cache
void DataCache::Erase(LogID log_id, GapType type, SequenceNumber seqno) {
  if (rs_cache_ == nullptr) { // No caching specified
    return;
  }

  // compute sequence number of block start
  SequenceNumber seqno_block = AlignToBlockStart(block_size_, seqno);

  // generate cache key
  CacheKey buffer;
  GenerateKey(block_size_, log_id, seqno_block, &buffer);
  Slice cache_key(buffer.buf, sizeof(buffer.buf));

  CacheEntry* entry = nullptr;

  // Fetch the appropriate entry from the cache
  Cache::Handle* handle = rs_cache_->Lookup(cache_key);
  if (handle) {
    // Search the entry
    entry = static_cast<CacheEntry *>(rs_cache_->Value(handle));
    size_t delta = entry->Erase(this, log_id, seqno);
    rs_cache_->ChargeDelta(handle, -delta);
    rs_cache_->Release(handle);
  }
}

SequenceNumber DataCache::VisitCache(LogID logid,
                                     SequenceNumber start,
                                     const Slice& lookup_topicname,
    std::function<bool(MessageData* data_raw, bool* processed)> on_message){
  if (rs_cache_ == nullptr) { // No caching specified
    return start;
  }
  // Assert that no blocks are pinned
  RS_ASSERT(rs_cache_->GetPinnedUsage() == 0);

  // compute sequence number of block start
  SequenceNumber seqno_block = AlignToBlockStart(block_size_, start);
  SequenceNumber orig_start = start;

  while (true) {
    // generate cache key
    CacheKey buffer;
    GenerateKey(block_size_, logid, seqno_block, &buffer);
    Slice cache_key(buffer.buf, sizeof(buffer.buf));

    // Fetch the appropriate entry from the cache
    Cache::Handle* handle = rs_cache_->Lookup(cache_key);
    if (!handle) {
      break;
    }

    // visit the relevant records in this entry
    CacheEntry* entry = static_cast<CacheEntry *>(rs_cache_->Value(handle));
    bool stop = false;
    SequenceNumber next =
      entry->VisitEntry(this, logid, start, bloom_filter_.get(),
                        lookup_topicname, &stop, on_message);
    rs_cache_->Release(handle);

    // If the new seqnumber is in the same block, then we are done
    if (next < seqno_block + block_size_ || stop) {
      start = next;
      break;
    }
    RS_ASSERT(next == AlignToBlockStart(block_size_, next));
    start = seqno_block = next;
  }

  if (orig_start == start) {
    stats_.cache_misses->Add(1);  // no relevant records in cache
  }
  return start;         // return next message that is not yet processed
}

bool DataCache::HasEntry(LogID logid, SequenceNumber seqno) const {
  if (rs_cache_ == nullptr) { // No caching specified
    return false;
  }
  SequenceNumber seqno_block = AlignToBlockStart(block_size_, seqno);
  CacheKey buffer;
  GenerateKey(block_size_, logid, seqno_block, &buffer);
  Slice cache_key(buffer.buf, sizeof(buffer.buf));

  // Fetch the appropriate entry from the cache
  bool result = false;
  Cache::Handle* handle = rs_cache_->Lookup(cache_key);
  if (handle) {
    // Check the records in this entry
    CacheEntry* entry = static_cast<CacheEntry*>(rs_cache_->Value(handle));
    result = entry->HasEntry(seqno - seqno_block);
    rs_cache_->Release(handle);
  }
  if (!result) {
    stats_.cache_misses->Add(1);  // no relevant records in cache
  }
  return result;
}


Statistics DataCache::GetStatistics() const {
  return stats_.all;
}
}  // namespace rocketspeed
