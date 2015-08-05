// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <string>

#include <sys/types.h>
#include <sys/socket.h>

namespace rocketspeed {

class Status;

/** Address of a remote service. */
class HostId {
 public:
  static Status Resolve(const std::string& hostname,
                        uint16_t port,
                        HostId* out);

  static Status CreateFromIP(const std::string& ip_address,
                             uint16_t port,
                             HostId* out);

  static HostId CreateLocal(uint16_t port, std::string description = "");

  HostId();

  bool operator<(const HostId& rhs) const;
  bool operator==(const HostId& rhs) const;
  size_t Hash() const;

  bool operator!() const { return addrlen_ == 0; }

  /**
   * Returns a human-readable form of the address.
   * Might return a hostname pre DNS resolution, if this object was created from
   * unresolved hostname string, therefore it is possible for two objects to
   * have the same human-readable representation, but not be equal.
   */
  const std::string& ToString() const { return description_; }

  /** Sets a human-readable form of the address. */
  void SetDescription(std::string description) {
    description_ = std::move(description);
  }

  const sockaddr* GetSockaddr() const {
    return reinterpret_cast<const sockaddr*>(&storage_);
  }

  socklen_t GetSocklen() const { return addrlen_; }

 private:
  static Status ResolveInternal(const std::string& hostname,
                                uint16_t port,
                                int ai_flags,
                                HostId* out);

  sockaddr_storage storage_;
  socklen_t addrlen_;
  std::string description_;

  HostId(const sockaddr* addr, socklen_t addrlen, std::string description);
};

}  // namespace rocketspeed

namespace std {
template <>
struct hash<rocketspeed::HostId> {
  size_t operator()(const rocketspeed::HostId& host_id) const {
    return host_id.Hash();
  }
};
}  // namespace std
