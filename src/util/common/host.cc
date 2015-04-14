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
  hostname(s),
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

// converts a HostId to a name:port string
std::string HostId::ToString() const {
  return hostname + ':' + std::to_string(port);
}

// Converts an internal client ID to a HostId
// The client ID must be formatted as <host>:<port><worker_id-byte>
// e.g. "localhost:1234\x01"
HostId HostId::ToHostId(ClientID str) {
  str.pop_back();  // remove the worker ID byte.
  std::string::size_type sep = str.find(":");
  assert(sep != std::string::npos);
  std::string hostname = str.substr(0, sep);
  std::string portstr = str.substr(sep+1, str.size());
  return HostId(hostname, atoi(portstr.c_str()));
}

// converts a HostId to a ClientID
ClientID HostId::ToClientId(int worker_id) const {
  ClientID client_id = ToString();
  assert(worker_id < 256);
  client_id.push_back(static_cast<char>('a' + worker_id));
  return client_id;
}

}  // namespace rocketspeed
