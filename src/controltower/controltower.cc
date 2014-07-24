//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/controltower/controltower.h"

namespace rocketspeed {

/**
 *  Reads a message header from an event. Then sets up another
 *  readcallback for the entire message body.
 */
void
ControlTower::readhdr(struct bufferevent *bev, void *ctx) {
    // Verify that we have at least the msg header available for reading.
    struct evbuffer *input = ld_bufferevent_get_input(bev);
    size_t available = ld_evbuffer_get_length(input);
    assert(available >= MessageHeader::GetSize());

    // Peek at the header.
    // We can optimize this further by using ld_evbuffer_peek
    const char* mem = (const char*)ld_evbuffer_pullup(input,
                                     MessageHeader::GetSize());
    Slice sl(mem);
    MessageHeader hdr(&sl);

    // set up a new callback to read the entire message
    ld_bufferevent_setcb(bev, readmsg, nullptr,
                         errorcb, nullptr);
    ld_bufferevent_setwatermark(bev, EV_READ, hdr.msgsize_,
                                1024L*1024L);
}

/**
 *  Reads an entire message
 */
void
ControlTower::readmsg(struct bufferevent *bev, void *ctx) {
    // Verify that we have at least the msg header available for reading.
    struct evbuffer *input = ld_bufferevent_get_input(bev);
    size_t available = ld_evbuffer_get_length(input);
    assert(available >= MessageHeader::GetSize());

    // Peek at the header.
    const char* mem = (const char*)ld_evbuffer_pullup(input,
                                     MessageHeader::GetSize());
    Slice sl(mem);
    MessageHeader hdr(&sl);

    // Retrieve the entire message.
    // TODO(dhruba) 1111  use ld_evbuffer_peek
    const char* data = (const char*)ld_evbuffer_pullup(input, hdr.msgsize_);

    // Convert the serialized string to a message object
    Slice tmpsl(data);
    Message* msg = Message::CreateNewInstance(&tmpsl);

    // process message here
    MessageType type = msg->GetMessageType();
    if (type != mData || type != mMetadata) {
      fprintf(stdout, "XXX bad metadata type");
    }
    fprintf(stdout, "Great***********\n");

    // free received message
    delete msg;
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

//
// This callback is fired from the first aritificial timer event
// in the dispatch loop.
void
ControlTower::do_startevent(evutil_socket_t listener, short event, void *arg) {
  ControlTower* obj = static_cast<ControlTower *>(arg);
  obj->running_ = true;
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
    if (bev == nullptr) {
      Log(InfoLogLevel::WARN_LEVEL, obj->GetOptions().info_log,
          "bufferevent_socket_new() failed. errno=%d", errno);
      close(fd);
      return;
    }
    // Set up an event to read the msg header first.
    ld_bufferevent_setcb(bev, readhdr, nullptr,
                         errorcb, nullptr);
    ld_bufferevent_setwatermark(bev, EV_READ, MessageHeader::GetSize(),
                                1024L*1024L);
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
  struct timeval zero_seconds = {0, 0};

  base_ = ld_event_base_new();
  if (!base_) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
      "Failed to create an event base for an EventLoop thread");
    // err = E::NOMEM;
    options_.info_log->Flush();
    return;
  }

  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = 0;
  sin.sin_port = htons(options_.port_number);

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
    options_.info_log->Flush();
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
    options_.info_log->Flush();
    return;
  }

  if (listen(listener, 16) < 0) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
      "listen() failed. errno=%d ", errno);
    options_.info_log->Flush();
    return;
  }

  // Listen in for new connections from clients
  struct event* listener_event = ld_event_new(base_, listener,
                                              EV_READ|EV_PERSIST,
                                              this->do_accept,
                                              reinterpret_cast<void*>(this));
  if (listener_event == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "Failed to create event for accepting new connections");
    options_.info_log->Flush();
    return;
  }

  /* check it */
  rv = ld_event_add(listener_event, nullptr);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "Failed to add 'request pipe is readable' event to event base");
    ld_event_free(listener_event);
    options_.info_log->Flush();
    return;
  }

  // Create a non-persistent event that will run as soon as the dispatch
  // loop is run. This is the first event to ever run on the dispatch loop.
  // The firing of this artificial event indicates that the event loop
  // is up and running.
  struct event* startup_event = ld_event_new(base_, listener,
                                             EV_TIMEOUT|EV_READ,
                                             this->do_startevent,
                                             reinterpret_cast<void*>(this));
  if (startup_event == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "Failed to create first startup event");
    options_.info_log->Flush();
    return;
  }
  rv = ld_event_add(startup_event, &zero_seconds);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "Failed to add startup event to event base");
    ld_event_free(listener_event);
    ld_event_free(startup_event);
    options_.info_log->Flush();
    return;
  }
  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Starting ControlTower at port %d", options_.port_number);
  options_.info_log->Flush();
  ld_event_base_dispatch(base_);
}

/**
 * Private constructor for a Control Tower
 */
ControlTower::ControlTower(const ControlTowerOptions& options,
                           const Configuration& conf):
  options_(SanitizeOptions(options)),
  conf_(conf),
  running_(false),
  base_(nullptr) {
  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Created a new Control Tower");
}

ControlTower::~ControlTower() {
  // stop dispatch loop
  if (base_ != nullptr) {
    int ret = ld_event_base_loopexit(base_, nullptr);
    if (ret) {
      Log(InfoLogLevel::FATAL_LEVEL, options_.info_log,
          "Unable to shutdown event dispach loop");
    }
    assert(!ld_event_base_got_exit(base_));
  }
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
