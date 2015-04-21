// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <map>

#include "include/RocketSpeed.h"
#include "src/datastore/DataStore.h"
#include "src/client/client.h"
#include "src/util/mutexlock.h"

/**
 * This is the DataStore interface. Applications can use this interface
 * to store key-values.
 * Keys starting with a "_" are metadata records. Applications cannot
 * Get/Put keys starting with "_".
 *
 * In this implementation, the database contents are stored in a single topic.
 * Tail this topic and continuously update all its contents into a std::map.
 * Gets/Iterators retrun data from this in-memory std::map.
 * Puts write a record to the topic and this record might not be immediately
 * visible via a Get. The newly written record would be tailed back from
 * the topic and is updated in the in-memory map. Only then it becomes
 * visible via Get/Iterator.
 */
namespace rocketspeed {

/*
 *  The callback that is invoked by RocketSpeed for every key-value
 *  to be returned
 */
class KeyValue;
typedef std::function<void(const KeyValue&)> KeyValueCallback;

class KeyValue {
 public:

  // The types of database records returned by Reader.
  enum EntryType : char {
    Error                 = 0x00,
    Success               = 0x01,
  };

  KeyValue(const std::string& key, const std::string& value) :
    version_(DATASTORE_VERSION_CURRENT),
    entry_type_(EntryType::Success),
    key_(key),
    value_(value) {
  }

  KeyValue() :
    version_(DATASTORE_VERSION_CURRENT),
    entry_type_(EntryType::Error) {
  }
  /*
   * @return the key
   */
  const std::string& GetKey() {
    return key_;
  }
  /*
   * @return the key
   */
  const std::string& GetValue() {
    return value_;
  }

  /*
   * @return EntryType::Success if this entry
   *                    represents a valid key-value
   *         EntryType::Error if the stream encountered an error
   */
  EntryType GetType() {
    return entry_type_;
  }

  /*
   * Sets the type of this RollcallEntry
   */
  void SetType(const EntryType& type) {
    entry_type_ = type;
  }

  void Serialize(std::string* buffer);
  Status DeSerialize(Slice in);
  ~KeyValue() = default;

 private:
  char  version_;
  EntryType entry_type_;
  std::string key_;
  std::string value_;
  static const char DATASTORE_VERSION_CURRENT = 1;
};

class IteratorImpl;

class DataStoreImpl : public DataStore {
 friend IteratorImpl;
 public:
  // Constructor
  DataStoreImpl(std::unique_ptr<ClientImpl> client,
                bool create_new,
                std::shared_ptr<Logger> info_log);

  // Read an entry from the data store
  Status Get(const std::string& key, std::string* value) override;

  // Write an entry to the topic.
  Status Put(const std::string& key, const std::string& value) override;

  std::unique_ptr<Iterator> CreateNewIterator() override;

   // Closes resources associated with this rollcall stream reader
  virtual ~DataStoreImpl() = default;

  // The default datastore topic name
  static Topic DefaultDataStoreTopicName() {
    return "datastore.";
  }

  // Returns the total number of key-values in the store.
  size_t NumRecords();

 private:
  enum ReaderState : char {               // The states of the datastore
    Invalid                 = 0x00,
    SubscriptionRequestSent = 0x01,
    SubscriptionConfirmed   = 0x02,
  };
  std::shared_ptr<Logger> info_log_;            // informational logs
  ReaderState state_;                           // current state of this reader
  const NamespaceID datastore_namespace_;       // name of namespace
  const Topic datastore_topic_;                 // name of the datastore topic
  const bool create_new_;                       // remove all pre-exisiting data
  const SubscriptionStart start_point_;
  const TopicOptions datastore_topic_options_;
  const MsgId msgid_;

  // Keys starting with "_" are metakeys. Applications cannot use these.
  static const char METAKEY = '_';

  // wait for the database to read in all the data from the topic
  void WaitForData();

  // The number of records fetched till now
  std::atomic<long long> num_fetched_;

  // The max time to wait at database initialization time
  std::chrono::seconds timeout_;

  // The state of the datastore
  Status db_status_;

  // Gets handle initial startup delays by using this synchronization
  port::Semaphore sem_;

  // Initial wait to retrieve the bulk of the keys from the topic.
  // Updates to this member is protected by mutex_.
  bool wait_at_startup_;

  // Mutex to protect rawdata_ and rawdata_version_.
  port::Mutex mutex_;

  // condition variable to indicate that subscription is done
  port::CondVar subscribed_cv_;

  // The key-values read in from the storage
  std::map<std::string, std::string> rawdata_;

  // The version number of rawdata_. Any additions to rawdata_ bumps
  // the version number
  uint64_t rawdata_version_;

  const std::unique_ptr<ClientImpl> rs_client_; // rocket speed client

  // Put with no bounds checks
  Status PutInternal(const std::string& key, const std::string& value,
                     bool is_internal);

  void Serialize(std::string* buffer);
  Status DeSerialize(const Slice& in);
};

/*
 * An implementation of the iterator.
 */
class IteratorImpl : public Iterator {
 public:
  explicit IteratorImpl(DataStoreImpl *db);
  virtual ~IteratorImpl() = default;
  Status Seek();
  Status Seek(const std::string& target);
  Status Next();
  const std::string& key() const;
  const std::string& value() const;
 private:

  // The underlying database
  DataStoreImpl* db_;

  // True, iff the iterator has been positioned at a valid value
  bool is_valid_;

  // The version of the underlying database at iterator seek time
  uint64_t iter_version_;

  // The iterator to the backing map
  std::map<std::string, std::string>::iterator iter_;

  // The last key/value read from the database (cache it here)
  std::string lastread_key_;
  std::string lastread_value_;

  Status SeekInternal(bool first, const std::string& target);
};

}  // namespace rocketspeed
