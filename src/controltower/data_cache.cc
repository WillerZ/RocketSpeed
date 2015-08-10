//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/controltower/data_cache.h"

#include <unordered_map>

namespace rocketspeed {

/*
 * Each Entry in the cache is an array of message pointers. The start
 * sequence number in an Entry is always a multiple of block_size_.
 * This is done so that the LRU cache can deal with fewer number of Entries
 * rather than having an Entry for each individual message.
 */

// The number of records for a log that is stored inside a
// CacheEntry. It is a compile time constant so that the
// overhead of the CacheEntry size can be reduced.
#define  BLOCK_SIZE 1024

// The key for the LRU cache is 16 bytes, it is made up of a
// 8 byte logid folowed by a 8 byte seqno.
struct alignas(16) CacheKey {
  char buf[16];
};

// Find the first seqno of the block
static SequenceNumber AlignToBlockStart(SequenceNumber seqno) {
  return (seqno / BLOCK_SIZE) * BLOCK_SIZE;
}

// Generate a key for cache lookup
static void GenerateKey(LogID logid, SequenceNumber seqno, CacheKey* buf) {
  assert(seqno == AlignToBlockStart(seqno));
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
  std::unique_ptr<MessageData> mcache_[BLOCK_SIZE];

#ifndef NDEBUG
  LogID logid_;                // useful for debugging
  SequenceNumber seqno_block_; // useful for debugging
#endif /* NDEBUG */

 public:
  explicit CacheEntry(LogID logid, SequenceNumber seqno_block) {
#ifndef NDEBUG
    logid_ = logid;
    seqno_block_ = seqno_block;
#endif /* NDEBUG */
  }

  // Stores the specified record in this entry.
  // Returns the increase in charge, if any
  size_t StoreData(const Slice& namespace_id, const Slice& topic,
                   LogID log_id,
                   std::unique_ptr<MessageData> msg) {
    SequenceNumber seqno = msg->GetSequenceNumber();
    SequenceNumber seqno_block = AlignToBlockStart(seqno);
    int offset = (int)(seqno - seqno_block);

    assert(logid_ == log_id);
    assert(seqno_block_ == seqno_block);

    // if there already is an entry, then there is nothing more to do
    if (mcache_[offset] != nullptr) {
      assert(mcache_[offset].get()->GetSequenceNumber() ==
             msg->GetSequenceNumber());
      assert(mcache_[offset].get()->GetTotalSize() ==
             msg->GetTotalSize());
      return 0;
    }
    size_t size = msg->GetTotalSize();
    mcache_[offset] = std::move(msg); // store message
    return size;
  }

  // Remove specified record from the cache
  // Returns the decrease in charge, if any
  size_t Erase(LogID log_id, SequenceNumber seqno) {
    SequenceNumber seqno_block = AlignToBlockStart(seqno);
    int offset = (int)(seqno - seqno_block);

    assert(logid_ == log_id);
    assert(seqno_block_ = seqno_block);

    // if there isn't an entry, then there is nothing more to do
    if (mcache_[offset] == nullptr) {
      return 0;
    }
    size_t size = mcache_[offset]->GetTotalSize();
    mcache_[offset] = nullptr;        // erase
    return size;
  }

  // Visit the records starting from the specified seqno
  SequenceNumber VisitEntry(LogID logid,
                            SequenceNumber seqno,
                            std::function<void(MessageData* data_raw)> visit) {
    SequenceNumber seqno_block = AlignToBlockStart(seqno);
    int offset = (int)(seqno - seqno_block);

    assert(logid_ == logid);
    assert(seqno_block_ == seqno_block);

    // scan all messages upto either the first null or the entire block
    int index = offset;
    for (; index < BLOCK_SIZE && mcache_[index]; index++) {
      visit(mcache_[index].get());
    }
    return seqno_block + index; // return the next seqno
  }

  size_t GetInitialCharge() {
    return sizeof(CacheEntry);
  }
};

// utility to release memory from the cache callback.
template <class T>
static void DeleteEntry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

DataCache::DataCache(size_t size_in_bytes,
                     bool cache_data_from_system_namespaces) :
  rs_cache_(size_in_bytes ? NewLRUCache(size_in_bytes) : nullptr) {
  characteristics_ = Characteristics::StoreUserTopics |
                     Characteristics::StoreSystemTopics |
                     Characteristics::StoreDataRecords |
                     Characteristics::StoreGapRecords;
  if (!cache_data_from_system_namespaces) {
    characteristics_ &= ~Characteristics::StoreSystemTopics;
  }
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

size_t DataCache::GetCapacity() {
  if (rs_cache_ == nullptr) { // No caching specified
    return 0;
  }
  return rs_cache_->GetCapacity();
}

size_t DataCache::GetUsage() {
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
  SequenceNumber seqno_block = AlignToBlockStart(seqno);

  // generate cache key
  CacheKey buffer;
  GenerateKey(log_id, seqno_block, &buffer);
  Slice cache_key(buffer.buf, sizeof(buffer.buf));

  CacheEntry* entry = nullptr;

  // Fetch the appropriate entry from the cache
  Cache::Handle* handle = rs_cache_->Lookup(cache_key);
  if (handle) {
    entry = static_cast<CacheEntry *>(rs_cache_->Value(handle));
  } else {
    // Entry does not exist in the cache.
    // Create a new entry and insert into cache.
    entry = new CacheEntry(log_id, seqno_block);
    handle = rs_cache_->Insert(cache_key, entry, entry->GetInitialCharge(),
                               &DeleteEntry<CacheEntry>);
  }

  // Insert this record into the Entry
  size_t delta = entry->StoreData(namespace_id, topic, log_id,
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
  SequenceNumber seqno_block = AlignToBlockStart(seqno);

  // generate cache key
  CacheKey buffer;
  GenerateKey(log_id, seqno_block, &buffer);
  Slice cache_key(buffer.buf, sizeof(buffer.buf));

  CacheEntry* entry = nullptr;

  // Fetch the appropriate entry from the cache
  Cache::Handle* handle = rs_cache_->Lookup(cache_key);
  if (handle) {
    // Search the entry
    entry = static_cast<CacheEntry *>(rs_cache_->Value(handle));
    size_t delta = entry->Erase(log_id, seqno);
    rs_cache_->ChargeDelta(handle, -delta);
    rs_cache_->Release(handle);
  }
}

SequenceNumber DataCache::VisitCache(LogID logid,
                                     SequenceNumber start,
                         std::function<void(MessageData* data_raw)> on_message){
  if (rs_cache_ == nullptr) { // No caching specified
    return start;
  }
  // compute sequence number of block start
  SequenceNumber seqno_block = AlignToBlockStart(start);

  while (true) {
    // generate cache key
    CacheKey buffer;
    GenerateKey(logid, seqno_block, &buffer);
    Slice cache_key(buffer.buf, sizeof(buffer.buf));

    // Fetch the appropriate entry from the cache
    Cache::Handle* handle = rs_cache_->Lookup(cache_key);
    if (!handle) {
      break;
    }

    // visit the relevant records in this entry
    CacheEntry* entry = static_cast<CacheEntry *>(rs_cache_->Value(handle));
    SequenceNumber next = entry->VisitEntry(logid, start, on_message);
    rs_cache_->Release(handle);

    // If the new seqnumber is in the same block, then we are done
    if (next < start + BLOCK_SIZE) {
      start = next;
      break;
    }
    assert(next == AlignToBlockStart(next));
    start = seqno_block = next;
  }
  return start;         // return next message that is not yet processed
}
}  // namespace rocketspeed
