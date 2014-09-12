//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/messages/event_loop.h"
#include <limits.h>
#include <sys/eventfd.h>
#include <thread>
#include "src/messages/serializer.h"
#include "src/messages/messages.h"

namespace rocketspeed {

/**
 *  Reads a message header from an event. Then sets up another
 *  readcallback for the entire message body.
 */
void
EventLoop::readhdr(struct bufferevent *bev, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  // Verify that we have at least the msg header available for reading.
  struct evbuffer *input = ld_bufferevent_get_input(bev);
  size_t available __attribute__((unused)) = ld_evbuffer_get_length(input);
  assert(available >= MessageHeader::GetSize());

  // Peek at the header.
  // We can optimize this further by using ld_evbuffer_peek
  const char* mem = (const char*)ld_evbuffer_pullup(input,
                                     MessageHeader::GetSize());
  Slice sl(mem, MessageHeader::GetSize());
  MessageHeader hdr(&sl);

  Log(InfoLogLevel::INFO_LEVEL, obj->info_log_,
      "received msghdr of size %d, msg size %d",  available, hdr.msgsize_);
  obj->info_log_->Flush();
  assert(ld_evbuffer_get_length(input) == available);

  // set up a new callback to read the entire message
  ld_bufferevent_setcb(bev, EventLoop::readmsg, nullptr, errorcb, arg);
  ld_bufferevent_setwatermark(bev, EV_READ, hdr.msgsize_, hdr.msgsize_);
}

/**
 *  Reads an entire message
 */
void
EventLoop::readmsg(struct bufferevent *bev, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);

  // Verify that we have at least the msg header available for reading.
  struct evbuffer *input = ld_bufferevent_get_input(bev);
  size_t available __attribute__((unused)) = ld_evbuffer_get_length(input);
  assert(available >= MessageHeader::GetSize());

  Log(InfoLogLevel::INFO_LEVEL, obj->info_log_,
      "received readmsg of size %d", available);
  obj->info_log_->Flush();

  // Peek at the header.
  const char* mem = (const char*)ld_evbuffer_pullup(input,
                                     MessageHeader::GetSize());
  Slice sl(mem, MessageHeader::GetSize());
  MessageHeader hdr(&sl);

  // Retrieve the entire message.
  // TODO(dhruba) 1111  use ld_evbuffer_peek
  const char* data = (const char*)ld_evbuffer_pullup(input, hdr.msgsize_);

  // Convert the serialized string to a message object
  assert(available >= hdr.msgsize_);
  Slice tmpsl(data, hdr.msgsize_);
  std::unique_ptr<Message> msg = Message::CreateNewInstance(&tmpsl);
  if (msg) {
    // Invoke the callback. It is the responsibility of the
    // callback to delete this message.
    obj->event_callback_(obj->event_callback_context_, std::move(msg));
  } else {
    // Failed to decode message.
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
      "failed to decode message");
    obj->info_log_->Flush();
  }

  // drain the processed message from the event buffer
  if (ld_evbuffer_drain(input, hdr.msgsize_)) {
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "unable to drain msg of size %d from event buffer", hdr.msgsize_);
  }

  // Set up the callback event to read the msg header first.
  ld_bufferevent_setcb(bev, EventLoop::readhdr, nullptr, errorcb, arg);
  ld_bufferevent_setwatermark(bev, EV_READ, MessageHeader::GetSize(),
                              MessageHeader::GetSize());
}

void
EventLoop::errorcb(struct bufferevent *bev, short error, void *ctx) {
  EventLoop* obj = static_cast<EventLoop *>(ctx);
  Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
      "bufferevent errorcb callback invoked, error = %d", error);
  if (error & BEV_EVENT_EOF) {
    ld_bufferevent_free(bev);
  } else if (error & BEV_EVENT_ERROR) {
    ld_bufferevent_free(bev);
    /* check errno to see what error occurred */
  } else if (error & BEV_EVENT_TIMEOUT) {
    /* must be a timeout event handle, handle it */
  }
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
EventLoop::do_shutdown(evutil_socket_t listener, short event, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  ld_event_base_loopexit(obj->base_, NULL);
}

void
EventLoop::do_command(evutil_socket_t listener, short event, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);

  // Posix guarantees that writes occur atomically below a certain size, so
  // if this callback occurs then the whole command should have been written.
  // No need for buffer events.
  Command* command;
  ssize_t received = read(obj->command_pipe_fds_[0],
                          reinterpret_cast<void*>(&command),
                          sizeof(command));

  // Check for read errors.
  if (received == -1) {
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "Reading from command pipe failed. errno=%d", errno);
    return;
  } else if (received < static_cast<ssize_t>(sizeof(command))) {
    // Partial read from the pipe. This may happen if the write call is
    // interrupted by a signal.
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "Partial read from command pipe.");
    return;
  }

  // Make sure we have a callback.
  if (obj->command_callback_) {
    obj->command_callback_(command);
  }

  delete command;
}

void
EventLoop::do_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen,
    void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  struct event_base *base = obj->base_;
  struct bufferevent *bev = ld_bufferevent_socket_new(base, fd,
                                                      BEV_OPT_CLOSE_ON_FREE);
  if (bev == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "bufferevent_socket_new() failed. errno=%d", errno);
    return;
  }
  // Set up an event to read the msg header first.
  ld_bufferevent_setcb(bev, &EventLoop::readhdr, nullptr, errorcb, arg);
  ld_bufferevent_setwatermark(bev, EV_READ, MessageHeader::GetSize(),
                              MessageHeader::GetSize());
  if (ld_bufferevent_enable(bev, EV_READ|EV_WRITE)) {
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "accept on socket %dmsghdr, error in enabling event errno=%d\n",
        fd, errno);
    return;
  }
  Log(InfoLogLevel::INFO_LEVEL, obj->info_log_,
      "accept successful on socket %d", fd);
}

void
EventLoop::accept_error_cb(struct evconnlistener *listener, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  struct event_base *base = ld_evconnlistener_get_base(listener);
  int err = EVUTIL_SOCKET_ERROR();
  Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
    "Got an error %d (%s) on the listener. "
    "Shutting down.\n", err, evutil_socket_error_to_string(err));
  ld_event_base_loopexit(base, NULL);
}

void
EventLoop::Run(void) {
  base_ = ld_event_base_new();
  if (!base_) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "Failed to create an event base for an EventLoop thread");
    info_log_->Flush();
    return;
  }

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = 0;
  sin.sin_port = htons(port_number_);

  // Create libevent connection listener.
  listener_ = ld_evconnlistener_new_bind(
    base_,
    &EventLoop::do_accept,
    reinterpret_cast<void*>(this),
    LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
    -1,  // backlog
    reinterpret_cast<struct sockaddr*>(&sin),
    sizeof(sin));

  if (listener_ == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to create connection listener");
    info_log_->Flush();
    return;
  }

  ld_evconnlistener_set_error_cb(listener_, &EventLoop::accept_error_cb);

  // Create a non-persistent event that will run as soon as the dispatch
  // loop is run. This is the first event to ever run on the dispatch loop.
  // The firing of this artificial event indicates that the event loop
  // is up and running.
  struct event *startup_event = evtimer_new(
    base_,
    this->do_startevent,
    reinterpret_cast<void*>(this));

  if (startup_event == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to create first startup event");
    info_log_->Flush();
    return;
  }
  struct timeval zero_seconds = {0, 0};
  int rv = evtimer_add(startup_event, &zero_seconds);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to add startup event to event base");
    info_log_->Flush();
    return;
  }

  // Create a shutdown event that will run when we want to stop the loop.
  // It creates an eventfd that the loop listens for reads on. When a read
  // is available, that indicates that the loop should stop.
  // This allows us to communicate to the event loop from another thread
  // safely without locks.
  shutdown_event_ = ld_event_new(
    base_,
    eventfd(0, 0),
    EV_PERSIST|EV_READ,
    this->do_shutdown,
    reinterpret_cast<void*>(this));
  if (shutdown_event_ == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to create shutdown event");
    info_log_->Flush();
    return;
  }
  rv = ld_event_add(shutdown_event_, nullptr);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to add shutdown event to event base");
    info_log_->Flush();
    return;
  }

  // This is a pipe that other threads can write to for asynchronous
  // processing on the event thread.
  if (pipe(command_pipe_fds_)) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to create command pipe.");
    info_log_->Flush();
    return;
  }

  command_pipe_event_ = ld_event_new(
    base_,
    command_pipe_fds_[0],
    EV_PERSIST|EV_READ,
    this->do_command,
    reinterpret_cast<void*>(this));
  if (command_pipe_event_ == nullptr) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to create command pipe event");
    info_log_->Flush();
    return;
  }
  rv = ld_event_add(command_pipe_event_, nullptr);
  if (rv != 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
        "Failed to add command event to event base");
    info_log_->Flush();
    return;
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "Starting EventLoop at port %d", port_number_);
  info_log_->Flush();

  // Start the event loop.
  // This will not exit until Stop is called, or some error
  // happens within libevent.
  ld_event_base_dispatch(base_);
  running_ = false;
}

void EventLoop::Stop() {
  if (base_ != nullptr) {
    int shutdown_fd = ld_event_get_fd(shutdown_event_);
    if (running_) {
      // Write to the shutdown event FD to signal the event loop thread
      // to shutdown and stop looping.
      uint64_t value = 1;
      int result;
      do {
        result = write(shutdown_fd, &value, sizeof(value));
      } while (running_ && (result < 0 || errno == EAGAIN));

      // Wait for event loop to exit on the loop thread.
      while (running_) {
        std::this_thread::yield();
      }

      // Shutdown everything
      if (listener_) {
        ld_evconnlistener_free(listener_);
      }
      ld_event_base_free(base_);
      close(shutdown_fd);
      close(command_pipe_fds_[0]);
      close(command_pipe_fds_[1]);
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "Stopped EventLoop at port %d", port_number_);
    info_log_->Flush();
    base_ = nullptr;
  }
}

Status EventLoop::SendCommand(Command* command) {
  // Check the pipe hasn't been closed due to partial write (see below).
  if (command_pipe_fds_[1] == 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "Trying to write to closed command pipe.", errno);
    delete command;
    return Status::InternalError("pipe closed");
  }

  // Write the command *pointer* to the pipe.
  //
  // Even though this may be written to by multiple threads, no synchronization
  // is needed because Posix guarantees that writes of sizes <= PIPE_BUF are
  // atomic.
  //
  // Sanity check:
  static_assert(sizeof(command) <= PIPE_BUF,
                "PIPE_BUF too small for atomic writes, need locks");

  // Note: this write could block if the pipe is full, so care needs to be taken
  // to ensure that the event loop processing can keep up with the threads
  // writing to the pipe if blocking is undesirable.
  ssize_t written = write(command_pipe_fds_[1],
                          reinterpret_cast<const void*>(&command),
                          sizeof(command));

  if (written == sizeof(command)) {
    // No errors, return success.
    return Status::OK();
  } else if (written == -1) {
    // Some internal error happened.
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "Error writing command to event loop, errno=%d", errno);
    delete command;
    return Status::InternalError("write returned error");
  } else {
    // Partial write. This is bad; only part of the pointer has been written
    // to the pipe. This should never happen, but if it does we need to close
    // the pipe so no more data is written to it.
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "Partial write of command to event loop, written=%d", written);

    // Close the pipe.
    close(command_pipe_fds_[0]);
    close(command_pipe_fds_[1]);
    command_pipe_fds_[0] = 0;
    command_pipe_fds_[1] = 0;
    char err_msg[64];
    snprintf(err_msg, sizeof(err_msg), "partial write, %d/%d bytes",
      static_cast<int>(written), static_cast<int>(sizeof(command)));
    delete command;
    return Status::InternalError(err_msg);
  }
}

/**
 * Constructor for a Message Loop
 */
EventLoop::EventLoop(int port_number,
                     const std::shared_ptr<Logger>& info_log,
                     EventCallbackType event_callback,
                     CommandCallbackType command_callback) :
  port_number_(port_number),
  running_(false),
  base_(nullptr),
  info_log_(info_log),
  event_callback_(event_callback),
  command_callback_(command_callback),
  event_callback_context_(nullptr),
  listener_(nullptr) {
  Log(InfoLogLevel::INFO_LEVEL, info_log,
      "Created a new Event Loop at port %d", port_number);
}

EventLoop::~EventLoop() {
  // stop dispatch loop
  Stop();
}

}  // namespace rocketspeed
