//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <unistd.h>

#include <deque>
#include <memory>

#include "include/Logger.h"
#include "include/Status.h"
#include "include/Slice.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class EventLoop;

// TODO(stupaq #5930219) make it asynchronous on linux
class DescriptorEvent {
 public:
  DescriptorEvent(std::shared_ptr<Logger> info_log, int fd);

  ~DescriptorEvent();

  // Performs a write operation.
  Status Write(std::string&& data);

  // Noncopyable
  DescriptorEvent(const DescriptorEvent&) = delete;
  void operator=(const DescriptorEvent&) = delete;

 private:
  // This class is not thread safe, unless external synchronization is provided.
  ThreadCheck thread_check_;
  // Logger for info messages.
  const std::shared_ptr<Logger> info_log_;
  // The descriptor.
  int fd_;
};

}  // namespace rocketspeed
