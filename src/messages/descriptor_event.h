//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Status.h"
#include "include/Slice.h"

namespace rocketspeed {

/**
 * A RAII wrapper around file descriptor.
 *
 * This class is not thread safe, unless external synchronization is provided.
 */
class DescriptorEvent {
 public:
  // Noncopyable
  DescriptorEvent(const DescriptorEvent&) = delete;
  DescriptorEvent& operator=(const DescriptorEvent&) = delete;
  // Movable
  DescriptorEvent(DescriptorEvent&& other) noexcept {
    *this = std::move(other);
  }
  DescriptorEvent& operator=(DescriptorEvent&& other) {
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
  }

  DescriptorEvent() : fd_(-1) {}

  explicit DescriptorEvent(int fd) : fd_(fd){};

  ~DescriptorEvent();

  Status Write(Slice data);

 private:
  // The descriptor.
  int fd_;
};

}  // namespace rocketspeed
