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

namespace rocketspeed {

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
  Command* data;
  ssize_t received = read(obj->command_pipe_fds_[0],
                          reinterpret_cast<void*>(&data),
                          sizeof(data));

  // Check for read errors.
  if (received == -1) {
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "Reading from command pipe failed. errno=%d", errno);
    obj->info_log_->Flush();
    return;
  } else if (received < static_cast<ssize_t>(sizeof(data))) {
    // Partial read from the pipe. This may happen if the write call is
    // interrupted by a signal.
    Log(InfoLogLevel::WARN_LEVEL, obj->info_log_,
        "Partial read from command pipe.");
    obj->info_log_->Flush();
    return;
  }

  // Take ownership of the command.
  std::unique_ptr<Command> command(data);

  // Make sure we have a callback.
  if (obj->command_callback_) {
    // Pass ownership to the callback (it can decide whether or not to delete).
    obj->command_callback_(std::move(command));
  }
}

struct SocketEvent {
 public:
  SocketEvent(EventLoop* event_loop, int fd, event_base* base)
  : hdr_idx_(0)
  , msg_idx_(0)
  , msg_size_(0)
  , fd_(fd)
  , ev_(nullptr)
  , event_loop_(event_loop) {
    ld_evutil_make_socket_nonblocking(fd);
    ev_ = ld_event_new(base, fd, EV_READ|EV_PERSIST, EventCallback, this);
    if (ev_ == nullptr || ld_event_add(ev_, nullptr)) {
      Log(InfoLogLevel::WARN_LEVEL, event_loop_->GetLog(),
          "Failed to create socket event for %d", fd);
      delete this;
    }
  }

  ~SocketEvent() {
    close(fd_);
    if (ev_) {
      ld_event_free(ev_);
    }
  }

 private:
  static void EventCallback(evutil_socket_t fd, short what, void* arg) {
    static_cast<SocketEvent*>(arg)->Callback(fd);
  }

  void Callback(evutil_socket_t fd) {
    // This will keep reading while there is data to be read,
    // but not more than 1MB to give other sockets a chance to read.
    ssize_t total_read = 0;
    while (total_read < 1024 * 1024) {
      if (hdr_idx_ < sizeof(hdr_buf_)) {
        // Read the header.
        ssize_t count = sizeof(hdr_buf_) - hdr_idx_;
        ssize_t n = read(fd_, hdr_buf_ + hdr_idx_, count);
        // If n == -1 then an error has occurred (don't close on EAGAIN though).
        // If n == 0 and this is our first read (total_read == 0) then this
        // means the other end has closed, so we should close, too.
        if (n == -1 || (n == 0 && total_read == 0)) {
          if (!(n == -1 && errno == EAGAIN)) {
            // Read error, close connection.
            delete this;
          }
          return;
        }
        total_read += n;
        hdr_idx_ += n;
        if (n < count) {
          // Still more header to be read, wait for next event.
          return;
        }

        // Now have read header, prepare msg buffer.
        Slice hdr_slice(hdr_buf_, sizeof(hdr_buf_));
        MessageHeader hdr(&hdr_slice);
        msg_size_ = hdr.msgsize_;
        if (msg_size_ <= sizeof(hdr_buf_)) {
          // Message size too small, bad data. Close connection.
          delete this;
          return;
        }
        msg_buf_.reset(new char[msg_size_]);

        // Copy in header data.
        memcpy(msg_buf_.get(), hdr_buf_, sizeof(hdr_buf_));
        msg_idx_ = sizeof(hdr_buf_);
      }
      assert(msg_idx_ < msg_size_);

      ssize_t count = msg_size_ - msg_idx_;
      ssize_t n = read(fd_, msg_buf_.get() + msg_idx_, count);
      // If n == -1 then an error has occurred (don't close on EAGAIN though).
      // If n == 0 and this is our first read (total_read == 0) then this
      // means the other end has closed, so we should close, too.
      if (n == -1 || (n == 0 && total_read == 0)) {
        if (!(n == -1 && errno == EAGAIN)) {
          // Read error, close connection.
          delete this;
        }
        return;
      }
      total_read += n;
      msg_idx_ += n;
      if (n < count) {
        // Still more message to be read, wait for next event.
        return;
      }

      // Now have whole message, process it.
      std::unique_ptr<Message> msg =
        Message::CreateNewInstance(std::move(msg_buf_), msg_size_);
      if (msg) {
        // Invoke the callback for this message.
        event_loop_->Dispatch(std::move(msg));
      } else {
        // Failed to decode message.
        Log(InfoLogLevel::WARN_LEVEL, event_loop_->GetLog(),
            "Failed to decode message");
        event_loop_->GetLog()->Flush();
      }

      // Reset state for next message.
      hdr_idx_ = 0;
      msg_idx_ = 0;
    }
  }

  size_t hdr_idx_;
  char hdr_buf_[MessageHeader::size];
  size_t msg_idx_;
  size_t msg_size_;
  std::unique_ptr<char[]> msg_buf_;
  evutil_socket_t fd_;
  event* ev_;
  EventLoop* event_loop_;
};

void
EventLoop::do_accept(struct evconnlistener *listener,
                     evutil_socket_t fd,
                     struct sockaddr *address,
                     int socklen,
                     void *arg) {
  EventLoop* event_loop = static_cast<EventLoop *>(arg);

  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  new SocketEvent(event_loop, fd, event_loop->base_);

  Log(InfoLogLevel::INFO_LEVEL, event_loop->info_log_,
      "Accept successful on socket %d", fd);
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

  // Port number <= 0 indicates that there is no accept loop.
  if (port_number_ > 0) {
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
          "Failed to create connection listener on port %d", port_number_);
      info_log_->Flush();
      return;
    }

    ld_evconnlistener_set_error_cb(listener_, &EventLoop::accept_error_cb);
  }

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

Status EventLoop::SendCommand(std::unique_ptr<Command> command) {
  // Check the pipe hasn't been closed due to partial write (see below).
  if (command_pipe_fds_[1] == 0) {
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "Trying to write to closed command pipe.", errno);
    info_log_->Flush();
    return Status::InternalError("pipe closed");
  }

  // Write the command *pointer* to the pipe.
  //
  // Even though this may be written to by multiple threads, no synchronization
  // is needed because Posix guarantees that writes of sizes <= PIPE_BUF are
  // atomic.
  //
  // Sanity check:
  Command* data = command.get();
  static_assert(sizeof(data) <= PIPE_BUF,
                "PIPE_BUF too small for atomic writes, need locks");

  // Note: this write could block if the pipe is full, so care needs to be taken
  // to ensure that the event loop processing can keep up with the threads
  // writing to the pipe if blocking is undesirable.
  ssize_t written = write(command_pipe_fds_[1],
                          reinterpret_cast<const void*>(&data),
                          sizeof(data));

  if (written == sizeof(data)) {
    // Command was successfully written, so release the pointer.
    command.release();
    return Status::OK();
  } else if (written == -1) {
    // Some internal error happened.
    Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "Error writing command to event loop, errno=%d", errno);
    info_log_->Flush();
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
    return Status::InternalError(err_msg);
  }
}

void EventLoop::Dispatch(std::unique_ptr<Message> message) {
  event_callback_(event_callback_context_, std::move(message));
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
