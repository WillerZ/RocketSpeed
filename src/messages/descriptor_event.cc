//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "descriptor_event.h"

#include <cstddef>
#include <cerrno>
#include <cstring>

namespace rocketspeed {

DescriptorEvent::DescriptorEvent(std::shared_ptr<Logger> info_log, int fd)
    : info_log_(std::move(info_log)), fd_(fd) {
  // It can be moved between threads before the first write is performed.
}

DescriptorEvent::~DescriptorEvent() {
  thread_check_.Check();
  close(fd_);
}

Status DescriptorEvent::Write(std::string&& data) {
  thread_check_.Check();

  ssize_t count = write(fd_, data.data(), data.size());
  if (count == -1) {
    LOG_WARN(info_log_,
             "Wanted to write %zd bytes to fd(%d) but encountered (%d) \"%s\".",
             data.size(),
             fd_,
             errno,
             strerror(errno));
    info_log_->Flush();
    return Status::IOError(strerror(errno));
  } else if (static_cast<size_t>(count) != data.size()) {
    LOG_WARN(info_log_,
             "Wanted to write %zu bytes to fd(%d) but written (%zu).",
             data.size(),
             fd_,
             static_cast<size_t>(count));
    return Status::IOError("Partial write.");
  }
  return Status::OK();
}

}  // namespace rocketspeed
