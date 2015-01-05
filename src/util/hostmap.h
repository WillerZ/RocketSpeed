// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <vector>
#include "include/Types.h"
#include "src/port/port.h"
#include "src/port/atomic_pointer.h"
#include "src/messages/commands.h"

namespace rocketspeed {

typedef int HostNumber;

//
// Maps a Hostid to a single unique number.
//
class HostMap {
 public:
  explicit HostMap(unsigned int number_buckets);
  ~HostMap();

  // Insert a new element in the map. Returns the HostNumber.
  // The auxiliary_array, which is a map from the HostNum to the
  // specified auxiliary_id, is updated atomically.
  // Returns -1 if error
  HostNumber Insert(const ClientID& hostid,
                    std::atomic<int>* auxiliary_array = nullptr,
                    int auxiliary_id = 0);

  // There isn't a way to delete an entry.

  // Looks up the id for a host. If the host does not
  // exist, then returns -1
  HostNumber Lookup(const ClientID& hostid) const;

  // Looks up a host given its HostNumber. If the host does
  // not exist, then return nullptr;
  const ClientID* Lookup(HostNumber num) const;

  // Utility method to convert a list of Hostids to a printable string
  static std::string ToString(const SendCommand::Recipients& hostlist);

 private:
  // size of the store
  unsigned int number_buckets_;

  // list of pointers to HostIds
  std::vector<port::AtomicPointer> hostlist_;

  // a lock used to serialize inserts
  port::Mutex hostlock_;
};

}  // namespace rocketspeed
