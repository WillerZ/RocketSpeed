//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "descriptor_event.h"

namespace rocketspeed {

DescriptorEvent::DescriptorEvent(std::shared_ptr<Logger> info_log,
                                 int fd)
    : info_log_(std::move(info_log)),
      fd_(fd) {
  thread_check_.Check();
}

DescriptorEvent::~DescriptorEvent() {
  thread_check_.Check();

  LOG_INFO(info_log_,
           "Removing descriptor event and closing fd(%d)", fd_);
  info_log_->Flush();

  close(fd_);
}

void DescriptorEvent::Enqueue(std::string&& data, WriteCallbackType callback) {
  thread_check_.Check();

  // TODO(stupaq #5930219) make it asynchronous on linux
  Status status;
  ssize_t count = write(fd_, data.data(), data.size());
  if (count == -1) {
    LOG_WARN(
        info_log_,
        "Wanted to write %zd bytes to fd(%d) but encountered (%d) \"%s\".",
        data.size(), fd_, errno, strerror(errno));
    info_log_->Flush();
    status = Status::IOError(strerror(errno));
  } else if (static_cast<size_t>(count) != data.size()) {
    LOG_WARN(
        info_log_,
        "Wanted to write %zu bytes to fd(%d) but written (%zd).",
        data.size(), fd_, count);
    status = Status::IOError("Partial write.");
  } else {
    status = Status::OK();
  }
  callback(status);
}

}  // namespace rocketspeed
