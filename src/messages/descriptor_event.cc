//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "descriptor_event.h"

#include <cstddef>
#include <cerrno>
#include <cstring>
#include <errno.h>
#include <unistd.h>

namespace rocketspeed {

DescriptorEvent::~DescriptorEvent() {
  if (fd_ != -1) {
    close(fd_);
  }
}

Status DescriptorEvent::Write(Slice data) {
  ssize_t count = write(fd_, data.data(), data.size());
  if (count == -1) {
    return Status::IOError(strerror(errno));
  } else if (static_cast<size_t>(count) != data.size()) {
    return Status::IOError("Partial write.");
  }
  return Status::OK();
}

}  // namespace rocketspeed
