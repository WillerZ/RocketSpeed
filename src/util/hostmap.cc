//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/hostmap.h"
#include <assert.h>
#include <map>
#include "src/util/mutexlock.h"
#include "src/util/xxhash.h"

namespace rocketspeed {

HostMap::HostMap(unsigned int number_buckets) :
  number_buckets_(number_buckets) {
  // pre-allocate the entire vector
  // initialize every element to nullptr
  hostlist_.resize(number_buckets_, port::AtomicPointer(nullptr));
}

HostMap::~HostMap() {
  MutexLock lock(&hostlock_);
  for (unsigned int i = 0; i < number_buckets_; i++) {
    delete static_cast<ClientID*>(hostlist_[i].Acquire_Load());
    hostlist_[i].Release_Store(nullptr);
  }
}

HostNumber
HostMap::Insert(const ClientID& clientid,
                std::atomic<int>* auxiliary_array,
                int auxiliary_id) {
  // acquire the lock so that conflicting inserts do not trample
  // one another
  MutexLock lock(&hostlock_);

  // generate a hash
  unsigned int hashval = XXH32(clientid.c_str(),
                               static_cast<int>(clientid.size()), 0);
  hashval = hashval % number_buckets_;

  // The hash value gives us the starting point of our search
  // Search the circular-list starting at the hash value index
  unsigned int loop = 0;
  unsigned int index = hashval;
  for (; loop < number_buckets_; index++, loop++) {
    if (index == number_buckets_) {
      index = 0;                  // wrap around circular array
    }
    ClientID* one = static_cast<ClientID*>(hostlist_[index].Acquire_Load());
    if (one != nullptr) {
      if (*one == clientid) {
        return index;            // found it
      }
    } else {
      break;                 // not found
    }
  }
  // no more available slots in the list, return error
  if (loop == number_buckets_) {
    return -1;
  }
  // we found a free slot at offset 'index'
  ClientID* newhost = new ClientID(clientid);

  // insert into auxiliary data structure first
  if (auxiliary_array != nullptr) {
    auxiliary_array[index].store(auxiliary_id, std::memory_order_release);
  }

  // insert into hostmap
  hostlist_[index].Release_Store(newhost);
  return index;
}

//
// Returns the HostNumber for a specified host.
// Lookups are entirely lock-free.
//
HostNumber
HostMap::Lookup(const ClientID& clientid) const {
  // generate a hash
  unsigned int hashval = XXH32(clientid.c_str(),
                               static_cast<int>(clientid.size()), 0);
  hashval = hashval % number_buckets_;

  // The hash value gives us the starting point of our search
  // Search the circular-list starting at the hash value index
  unsigned int loop = 0;
  unsigned int index = hashval;
  for (; loop < number_buckets_; index++, loop++) {
    if (index == number_buckets_) {
      index = 0;                  // wrap around circular array
    }
    const ClientID* one =
      static_cast<const ClientID*>(hostlist_[index].Acquire_Load());
    if (one != nullptr) {
      if (*one == clientid) {
        return index;        // found it
      }
    } else {
      break;                 // not found
    }
  }
  return -1;                 // not found
}

//
// Returns the host for a specified HostNumber.
//
const ClientID*
HostMap::Lookup(HostNumber number) const {
  assert(number >= 0 && (unsigned int)number < number_buckets_);
  const ClientID* one =
    static_cast<const ClientID*>(hostlist_[number].Acquire_Load());
  return  one;
}

// Utility method to convert a list of Hostids to a printable string
std::string
HostMap::ToString(const SendCommand::Recipients& hostlist) {
  std::string out;
  for (ClientID host : hostlist) {
    out += " " + host;
  }
  return out;
}
}  // namespace rocketspeed
