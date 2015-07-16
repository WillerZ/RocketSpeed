// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Status.h"
#include "include/Slice.h"
#include "include/Types.h"
#include "src/port/port.h"

namespace rocketspeed {

HostId::HostId(std::string s, uint64_t p) :
  hostname(std::move(s)),
  port(p) {
}

HostId::HostId() :
  port(0) {
}

bool HostId::operator<(const HostId& rhs) const {
  if (port < rhs.port) {
    return true;
  } else if (port > rhs.port) {
    return false;
  } else {
    return hostname < rhs.hostname;
  }
}

bool HostId::operator==(const HostId& rhs) const {
  return port == rhs.port && hostname == rhs.hostname;
}

std::string HostId::ToString() const {
  return hostname + ':' + std::to_string(port);
}

}  // namespace rocketspeed

size_t std::hash<rocketspeed::HostId>::operator()(
    const rocketspeed::HostId& host_id) const {
  const size_t x = std::hash<std::string>()(host_id.hostname);
  const size_t y = std::hash<uint64_t>()(host_id.port);
  return x ^ (y + 0x9e3779b9 + (x << 6) + (x >> 2));
}
