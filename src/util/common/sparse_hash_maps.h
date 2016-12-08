// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It is not thread safe
// It may automatically evict entries to make room for new entries.
// Values have a specified charge against the cache capacity.
// For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.

#pragma once

#include <google/sparse_hash_set>
#include "include/Assert.h"
#include "include/KeylessHashMMap.h"

namespace rocketspeed {

/**
 * Memory optimized hash map for storing pointers.
 * It doesn't keep keys but instead use user specified mapping.
 * Mapping struct must implement 3 functions:
 * // return Key for Value*
 * 1) Key ExtractKey(const Value* ptr) const;
 * // return hash of the Key
 * 2) size_t Hash(const Key& Key) const;
 * // test keys for equality
 * 3) bool Equals(const Key& key1, const Key& key2) const;
 * The mapping must not change during the map existence.
 */
template <typename Key, typename ValuePtr, typename Mapping>
using SparseKeylessMap = rocketspeed::KeylessHashMMap<
    Key, ValuePtr, Mapping, google::sparse_hash_set, SingleIntArray<ValuePtr>>;

template <typename ContainerT, typename ValueToKey>
class STLAdapter {
 public:
  using value_type = typename ContainerT::value_type;
  using key_type = typename ContainerT::key_type;
  using iterator = typename ContainerT::Iterator;

  STLAdapter() = default;
  STLAdapter(const STLAdapter&) = default;

  // STL counterparts
  bool empty() const { return Empty(); }
  iterator begin() const { return Begin(); }
  iterator end() const { return End(); }
  void erase(iterator it) { Erase(it); }
  iterator find(const key_type& key) const { return Find(key); }
  bool emplace(const key_type& key, const value_type& value) {
    RS_ASSERT(key == ValueToKey().ExtractKey(value));
    (void) key;
    return Insert(value);
  }
  size_t size() const { return Size(); }
  void clear() { Clear(); }
  void swap(STLAdapter& other) { Swap(other.sparse_); }

  // forwarded calls
  bool Empty() const { return sparse_.Empty(); }
  iterator Begin() const { return sparse_.Begin(); }
  iterator End() const { return sparse_.End(); }
  void Erase(iterator it) { sparse_.Erase(it); }
  iterator Find(const key_type& key) const { return sparse_.Find(key); }
  bool Insert(const value_type& value) { return sparse_.Insert(value); }
  size_t Size() const { return sparse_.Size(); }
  void Clear() { sparse_.Clear(); }
  void Swap(STLAdapter& other) { sparse_.Swap(other.sparse_); }

 private:
  ContainerT sparse_;
};

}  // rocketspeed
