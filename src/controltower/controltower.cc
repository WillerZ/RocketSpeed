//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/controltower/controltower.h"

namespace rocketspeed {

void
ControlTower::readcb(struct bufferevent *bev, void *ctx) {
    struct evbuffer *input, *output;
    char *line;
    size_t n;
    input = ld_bufferevent_get_input(bev);
    output = ld_bufferevent_get_output(bev);

    while ((line = ld_evbuffer_readln(input, &n, EVBUFFER_EOL_LF))) {
        ld_evbuffer_add(output, line, n);
        ld_evbuffer_add(output, "\n", 1);
        free(line);
    }
}

void
ControlTower::errorcb(struct bufferevent *bev, short error, void *ctx) {
    if (error & BEV_EVENT_EOF) {
        /* connection has been closed, do any clean up here */
        /* ... */
    } else if (error & BEV_EVENT_ERROR) {
        /* check errno to see what error occurred */
        /* ... */
    } else if (error & BEV_EVENT_TIMEOUT) {
        /* must be a timeout event handle, handle it */
        /* ... */
    }
    ld_bufferld_event_free(bev);
}

void
ControlTower::do_accept(evutil_socket_t listener, short event, void *arg) {
  ControlTower* obj = static_cast<ControlTower *>(arg);
  struct event_base *base = obj->base_;
  struct sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int fd = accept(listener, (struct sockaddr*)&ss, &slen);
  if (fd < 0) {
    Log(InfoLogLevel::WARN_LEVEL, obj->GetOptions().info_log,
        "accept() failed. errno=%d", errno);
  } else if (fd > FD_SETSIZE) {
    close(fd);
  } else {
    struct bufferevent *bev;
    ld_evutil_make_socket_nonblocking(fd);
    bev = ld_bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
    ld_bufferevent_setcb(bev, readcb, nullptr,
                         errorcb, nullptr);
    ld_bufferevent_setwatermark(bev, EV_READ, 2, 128*1024);
    ld_bufferevent_enable(bev, EV_READ|EV_WRITE);
  }
}

/**
 * Sanitize user-specified options
 */
ControlTowerOptions
ControlTower::SanitizeOptions(const ControlTowerOptions& src) {
  ControlTowerOptions result = src;

  if (result.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(src.env,
                                       result.log_dir,
                                       result.log_file_time_to_roll,
                                       result.max_log_file_size,
                                       result.info_log_level,
                                       &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
     result.info_log = nullptr;
    }
  }
  return result;
}

void
ControlTower::Run(void) {
  evutil_socket_t listener;
  struct sockaddr_in sin;

  base_ = ld_event_base_new();
  if (!base_) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
      "Failed to create an event base for an EventLoop thread");
    // err = E::NOMEM;
    return;
  }

  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = 0;
  sin.sin_port = htons(40713);

  listener = socket(AF_INET, SOCK_STREAM, 0);
  if (listener < 0) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "socket() failed. errno=%d", errno);
    switch (errno) {
      case EMFILE:      // system limit
      case ENFILE:
        break;
      case ENOBUFS:
      case ENOMEM:      // no more memory
        break;
      default:
        break;
    }
    return;
  }
  int rv = ld_evutil_make_socket_nonblocking(listener);
  if (rv != 0) {  // unlikely
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "Failed to make fd %d non-blocking. errno=%d",
        listener, errno);
    ::close(listener);
    return;
  }
  int one = 1;
  setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  if (bind(listener, (struct sockaddr*)&sin, sizeof(sin)) < 0) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "bind() failed. errno=%d ", errno);
    return;
  }

  if (listen(listener, 16) < 0) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
      "listen() failed. errno=%d ", errno);
    return;
  }

  struct event* listener_event = ld_event_new(
                                base_, listener, EV_READ|EV_PERSIST,
                                do_accept, reinterpret_cast<void*>(this));
  /* check it */
  rv = ld_event_add(listener_event, nullptr);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "Failed to add 'request pipe is readable' event to event base");
    ld_event_free(listener_event);
    return;
  }
  ld_event_base_dispatch(base_);
}

/**
 * Private constructor for a Control Tower
 */
ControlTower::ControlTower(const ControlTowerOptions& options,
                           const Configuration& conf):
  options_(SanitizeOptions(options)),
  conf_(conf),
  base_(nullptr) {
  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Created a new Control Tower");
}

/**
 * This is a static method to create a ControlTower
 */
Status
ControlTower::CreateNewInstance(const ControlTowerOptions& options,
                                const Configuration& conf,
                                ControlTower** ct) {
  *ct = new ControlTower(options, conf);
  return Status::OK();
}

}  // namespace rocketspeed
