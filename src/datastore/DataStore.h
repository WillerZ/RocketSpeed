// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>

#include "include/RocketSpeed.h"

/**
 * This is the DataStore interface. This api is used by RocketSpeed
 * to store meta-information about topics, namespaces, retention
 * and any other metadata. The DataStore api is a bare-bones
 * key-value api and various existing database implemenatation
 * (e.g. MySQL, Zookeeper, etc) can be wrapped using this api.
 */
namespace rocketspeed {

class Iterator;

/*
 * The interface that is used to fetch and update key-value data
 */
class DataStore {
 public:
  virtual ~DataStore()  = default;

  /*
   * Open the database that is stored in RocketSpeed itself.
   * @param machine The database name is of the form machine:port/dbname
   * @param create_new remove older data and create a new database
   * @param info_log The logger for logging informational messages
   * @param handle The output handle to be used for accessing data from store
   * @return the status of whether this call was successful or not
   */
  static Status Open(HostId machine,
                     bool create_new,
                     std::shared_ptr<Logger> info_log,
                     std::unique_ptr<DataStore>* handle);

  /*
   * Issues a read request for a specific key in the data store
   * @param key The name of the key
   * @param value The value is returned here
   * @return the status of whether this call was successful or not
   */
  virtual Status Get(const std::string& key, std::string* value) = 0;

  /*
   * Issues a write request for a specific key in the data store
   * @param key The name of the key
   * @param value The value associated with this key
   * @return the status of whether this call was successful or not
   */
  virtual Status Put(const std::string& key, const std::string& value) = 0;

  /*
   * Create a new iterator to scan data in the database.
   */
  virtual std::unique_ptr<Iterator> CreateNewIterator() = 0;
};

/* Iterate over a subset of keys. Keys are stored in the database
 * in lexicographical order.
 */
class Iterator {
 public:
  virtual ~Iterator() = default;

  // Position at the first key in the source.
  // If there are no more entries, then it returns Status::NotFound
  virtual Status Seek() = 0;

  // Position at the first key in the source that at or past target
  // If there are no more entries, then it returns Status::NotFound
  virtual Status Seek(const std::string& target) = 0;

  // Moves to the next entry in the source.
  // If there are no more entries, then it returns Status::NotFound
  virtual Status Next() = 0;

  // Return the key for the current entry.
  // Requires that a previous Seek or Next returned success.
  // The returned key is valid only till the next call to Seek or Next.
  virtual const std::string& key() const = 0;

  // Return the value for the current entry.
  // Requires that a previous Seek or Next returned success.
  // The returned key is valid only till the next call to Seek or Next.
  virtual const std::string& value() const = 0;
};

}  // namespace rocketspeed
