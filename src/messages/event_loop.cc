//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/messages/event_loop.h"
#include "src/messages/serializer.h"
#include "src/messages/messages.h"

namespace rocketspeed {

/**
 *  Reads a message header from an event. Then sets up another
 *  readcallback for the entire message body.
 */
void
EventLoop::readhdr(struct bufferevent *bev, void *arg) {
  // Verify that we have at least the msg header available for reading.
  struct evbuffer *input = ld_bufferevent_get_input(bev);
  size_t available = ld_evbuffer_get_length(input);
  assert(available >= MessageHeader::GetSize());

  // Peek at the header.
  // We can optimize this further by using ld_evbuffer_peek
  const char* mem = (const char*)ld_evbuffer_pullup(input,
                                     MessageHeader::GetSize());
  Slice sl(mem, MessageHeader::GetSize());
  MessageHeader hdr(&sl);

  // set up a new callback to read the entire message
  ld_bufferevent_setcb(bev, readmsg, nullptr,
                       errorcb, arg);
  ld_bufferevent_setwatermark(bev, EV_READ, hdr.msgsize_,
                              1024L*1024L);
}

/**
 *  Reads an entire message
 */
void
EventLoop::readmsg(struct bufferevent *bev, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);

  // Verify that we have at least the msg header available for reading.
  struct evbuffer *input = ld_bufferevent_get_input(bev);
  size_t available = ld_evbuffer_get_length(input);
  assert(available >= MessageHeader::GetSize());

  // Peek at the header.
  const char* mem = (const char*)ld_evbuffer_pullup(input,
                                     MessageHeader::GetSize());
  Slice sl(mem, MessageHeader::GetSize());
  MessageHeader hdr(&sl);

  // Retrieve the entire message.
  // TODO(dhruba) 1111  use ld_evbuffer_peek
  const char* data = (const char*)ld_evbuffer_pullup(input, hdr.msgsize_);

  // Convert the serialized string to a message object
  Slice tmpsl(data, hdr.msgsize_);
  std::unique_ptr<Message> msg = Message::CreateNewInstance(&tmpsl);

  // Invoke the callback. It is the responsibility of the
  // callback to delete this message.
  obj->event_callback_(obj->event_callback_context_, std::move(msg));
}

void
EventLoop::errorcb(struct bufferevent *bev, short error, void *ctx) {
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
  ld_bufferevent_free(bev);
}

//
// This callback is fired from the first aritificial timer event
// in the dispatch loop.
void
EventLoop::do_startevent(evutil_socket_t listener, short event, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  obj->running_ = true;
}

void
EventLoop::do_accept(evutil_socket_t listener, short event, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  struct event_base *base = obj->base_;
  struct sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int fd = accept(listener, (struct sockaddr*)&ss, &slen);
  if (fd < 0) {
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "accept() failed. errno=%d", errno);
  } else if (fd > FD_SETSIZE) {
    close(fd);
  } else {
    struct bufferevent *bev;
    ld_evutil_make_socket_nonblocking(fd);
    bev = ld_bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
    if (bev == nullptr) {
      Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
          "bufferevent_socket_new() failed. errno=%d", errno);
      close(fd);
      return;
    }
    // Set up an event to read the msg header first.
    ld_bufferevent_setcb(bev, readhdr, nullptr,
                         errorcb, arg);
    ld_bufferevent_setwatermark(bev, EV_READ, MessageHeader::GetSize(),
                                1024L*1024L);
    ld_bufferevent_enable(bev, EV_READ|EV_WRITE);
  }
}

void
EventLoop::Run(void) {
  evutil_socket_t listener;
  struct sockaddr_in sin;
  struct timeval zero_seconds = {0, 0};

  base_ = ld_event_base_new();
  if (!base_) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "Failed to create an event base for an EventLoop thread");
    // err = E::NOMEM;
    info_log_->Flush();
    return;
  }

  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = 0;
  sin.sin_port = htons(port_number_);

  listener = socket(AF_INET, SOCK_STREAM, 0);
  if (listener < 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
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
    info_log_->Flush();
    return;
  }
  int rv = ld_evutil_make_socket_nonblocking(listener);
  if (rv != 0) {  // unlikely
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to make fd %d non-blocking. errno=%d",
        listener, errno);
    ::close(listener);
    return;
  }
  int one = 1;
  setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  if (bind(listener, (struct sockaddr*)&sin, sizeof(sin)) < 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "bind() failed. errno=%d ", errno);
    info_log_->Flush();
    return;
  }

  if (listen(listener, 16) < 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "listen() failed. errno=%d ", errno);
    info_log_->Flush();
    return;
  }

  // Listen in for new connections from clients
  struct event* listener_event = ld_event_new(base_, listener,
                                              EV_READ|EV_PERSIST,
                                              this->do_accept,
                                              reinterpret_cast<void*>(this));
  if (listener_event == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to create event for accepting new connections");
    info_log_->Flush();
    return;
  }

  /* check it */
  rv = ld_event_add(listener_event, nullptr);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to add 'request pipe is readable' event to event base");
    ld_event_free(listener_event);
    info_log_->Flush();
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
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to create first startup event");
    info_log_->Flush();
    return;
  }
  rv = ld_event_add(startup_event, &zero_seconds);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to add startup event to event base");
    ld_event_free(listener_event);
    ld_event_free(startup_event);
    info_log_->Flush();
    return;
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "Starting EventLoop at port %d", port_number_);
  info_log_->Flush();
  ld_event_base_dispatch(base_);
}

/**
 * Constructor for a Message Loop
 */
EventLoop::EventLoop(int port_number,
                     const std::shared_ptr<Logger>& info_log,
                     EventCallbackType event_callback) :
  port_number_(port_number),
  running_(false),
  base_(nullptr),
  info_log_(info_log),
  event_callback_(event_callback),
  event_callback_context_(nullptr) {
  Log(InfoLogLevel::INFO_LEVEL, info_log,
      "Created a new Event Loop");
}

EventLoop::~EventLoop() {
  // stop dispatch loop
  if (base_ != nullptr) {
    int ret = ld_event_base_loopexit(base_, nullptr);
    if (ret) {
      Log(InfoLogLevel::FATAL_LEVEL, info_log_,
          "Unable to shutdown event dispach loop");
    }
    assert(!ld_event_base_got_exit(base_));
  }
}
}  // namespace rocketspeed
