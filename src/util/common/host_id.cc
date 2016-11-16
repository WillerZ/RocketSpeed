// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/HostId.h"

#include <climits>
#include <cstdint>
#include <cstring>
#include <string>

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "include/Slice.h"
#include "include/Status.h"
#include "src/util/common/hash.h"
#include "src/util/common/parsing.h"

namespace rocketspeed {

Status HostId::Resolve(const std::string& str, HostId* out) {
  auto host_and_port = SplitString(str, ':');
  if (host_and_port.size() != 2) {
    return Status::InvalidArgument("Malformed or missing port");
  }
  const auto& port_str = host_and_port[1];
  char* end_ptr;
  long int port_val = strtol(port_str.c_str(), &end_ptr, 10);
  if (port_val < 0 || port_val > 65535 ||
      end_ptr != port_str.c_str() + port_str.length()) {
    return Status::InvalidArgument("Invalid port: " + port_str);
  }
  return Resolve(host_and_port[0], static_cast<uint16_t>(port_val), out);
}

Status HostId::Resolve(const std::string& hostname,
                       uint16_t port,
                       HostId* out) {
  // Allow getaddrinfo to perform DNS resolution if necessary.
  return ResolveInternal(hostname, port, AI_NUMERICSERV, out);
}

Status HostId::CreateFromIP(const std::string& ip_address,
                            uint16_t port,
                            HostId* out) {
  // The address must be valid, resolved IP address, we do not fall back to DNS
  // resolution.
  return ResolveInternal(
      ip_address, port, AI_NUMERICHOST | AI_NUMERICSERV, out);
}

HostId HostId::CreateLocal(uint16_t port, std::string description) {
  sockaddr_in loopback;
  memset(&loopback, 0, sizeof(loopback));
  loopback.sin_family = AF_INET;
  loopback.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  loopback.sin_port = htons(port);

  if (description.empty()) {
    char myname[1024];
    gethostname(&myname[0], sizeof(myname));
    description.append(myname);
    description.push_back(':');
    description.append(std::to_string(port));
  }

  const sockaddr* addr = reinterpret_cast<sockaddr*>(&loopback);
  socklen_t addrlen = static_cast<socklen_t>(sizeof(loopback));
  return HostId(addr, addrlen, std::move(description));
}

HostId HostId::CreateFromSockaddr(const sockaddr* addr, socklen_t addrlen) {
  std::string description;
  switch (addr->sa_family) {
    case AF_INET: {
      auto v4addr = reinterpret_cast<const sockaddr_in*>(addr);
      uint16_t port = v4addr->sin_port;
      char buffer[INET_ADDRSTRLEN];
      const char* str =
          inet_ntop(AF_INET, &(v4addr->sin_addr), buffer, INET_ADDRSTRLEN);
      description = std::string(str) + ":" + std::to_string(port);
      break;
    }
    case AF_INET6: {
      auto v6addr = reinterpret_cast<const sockaddr_in6*>(addr);
      uint16_t port = v6addr->sin6_port;
      char buffer[INET6_ADDRSTRLEN];
      const char* str =
          inet_ntop(AF_INET6, &(v6addr->sin6_addr), buffer, INET6_ADDRSTRLEN);
      description = std::string(str) + ":" + std::to_string(port);
      break;
    }
    default:
      RS_ASSERT_DBG(false) << "Unknown sa_family";
      description = "ERROR - unknown sa_family";
      break;
  }
  return HostId(addr, addrlen, std::move(description));
}


HostId::HostId() : addrlen_(0) {
  memset(&storage_, 0, sizeof(storage_));
}

bool HostId::operator<(const HostId& rhs) const {
  return memcmp(&storage_, &rhs.storage_, sizeof(storage_)) < 0;
}

bool HostId::operator==(const HostId& rhs) const {
  return memcmp(&storage_, &rhs.storage_, sizeof(storage_)) == 0;
}

bool HostId::operator!=(const HostId& rhs) const {
  return !(*this == rhs);
}

size_t HostId::Hash() const {
  return MurmurHash2<Slice>()(
      Slice(reinterpret_cast<const char*>(&storage_), sizeof(storage_)));
}

uint16_t HostId::GetPort() const {
  auto saddr = GetSockaddr();
  uint16_t port_netw = 0;
  if (saddr->sa_family == AF_INET) {
    port_netw = reinterpret_cast<const struct sockaddr_in*>(saddr)->sin_port;
  } else if (saddr->sa_family == AF_INET6) {
    port_netw = reinterpret_cast<const struct sockaddr_in6*>(saddr)->sin6_port;
  }
  return ntohs(port_netw);
}

Status HostId::GetHost(std::string* host) const {
  RS_ASSERT(host);
  // This buffer will be sufficient to hold both IPv4 and IPv6 address.
  char buffer[INET6_ADDRSTRLEN] = {0};
  if (int rv = getnameinfo(GetSockaddr(),
                           GetSocklen(),
                           buffer,
                           INET6_ADDRSTRLEN,
                           nullptr,
                           0,
                           NI_NUMERICHOST | NI_NUMERICSERV)) {
    return Status::IOError("Failed to stringify host, reason: " +
                           std::string(gai_strerror(rv)));
  }
  *host = std::string(buffer);
  return Status::OK();
}

Status HostId::ResolveInternal(const std::string& hostname,
                               uint16_t port,
                               int ai_flags,
                               HostId* out) {
  addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_flags = ai_flags;
  // Accept both IPv4 and IPv6 addresses.
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  std::string port_str(std::to_string(port));
  addrinfo* result = nullptr;
  if (int rv =
          getaddrinfo(hostname.c_str(), port_str.c_str(), &hints, &result)) {
    return Status::IOError("Failed to resolve " + hostname + ":" + port_str +
                           ", reason: " + gai_strerror(rv));
  }

  if (!result) {
    return Status::IOError("No resolution for " + hostname + ":" + port_str);
  }

  // RFC 3484: IPv6 address will precede IPv4 address on the list returned by
  // getaddrinfo.
  *out = HostId(result->ai_addr, result->ai_addrlen, hostname + ":" + port_str);

  freeaddrinfo(result);
  return Status::OK();
}

HostId::HostId(const sockaddr* addr, socklen_t addrlen, std::string description)
: addrlen_(addrlen), description_(std::move(description)) {
  RS_ASSERT(addrlen_ > 0);
  RS_ASSERT(sizeof(storage_) >= addrlen_);
  memset(&storage_, 0, sizeof(storage_));
  memcpy(&storage_, addr, addrlen_);
}
}  // namespace rocketspeed
