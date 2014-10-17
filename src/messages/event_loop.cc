//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/messages/event_loop.h"
#include <limits.h>
#include <sys/eventfd.h>
#include <deque>
#include <thread>
#include "src/messages/serializer.h"

namespace rocketspeed {

struct SocketEvent {
 public:
  SocketEvent(EventLoop* event_loop, int fd, event_base* base)
  : hdr_idx_(0)
  , msg_idx_(0)
  , msg_size_(0)
  , fd_(fd)
  , ev_(nullptr)
  , write_ev_(nullptr)
  , event_loop_(event_loop)
  , known_remote_(false)
  , write_ev_added_(false) {

    // create read and write events
    ev_ = ld_event_new(base, fd, EV_READ|EV_PERSIST,
                       EventCallback, this);
    write_ev_ = ld_event_new(base, fd, EV_WRITE|EV_PERSIST,
                       EventCallback, this);

    // register only the read callback
    if (ev_ == nullptr || write_ev_ == nullptr ||
        ld_event_add(ev_, nullptr)) {
      LOG_WARN(event_loop_->GetLog(),
          "Failed to create socket event for %d", fd);
      delete this;
    }
  }

  // this constructor is used by server-side connection initiation
  SocketEvent(EventLoop* event_loop, int fd, event_base* base,
              const HostId& remote)
  : hdr_idx_(0)
  , msg_idx_(0)
  , msg_size_(0)
  , fd_(fd)
  , ev_(nullptr)
  , event_loop_(event_loop)
  , known_remote_(true)
  , remote_host_(remote)
  , write_ev_added_(false) {

    // create read and write events
    ev_ = ld_event_new(base, fd, EV_READ|EV_PERSIST,
                       EventCallback, this);
    write_ev_ = ld_event_new(base, fd, EV_WRITE|EV_PERSIST,
                       EventCallback, this);
    // register only the read callback
    if (ev_ == nullptr || write_ev_  == nullptr ||
        ld_event_add(ev_, nullptr)) {
      LOG_WARN(event_loop_->GetLog(),
          "Failed to create socket event for %d", fd);
      delete this;
    }

    // Insert the write_ev_ into the connection cache. This is
    // necessary so that further outgoing messages get queued up
    // on this write event.
    bool inserted = event_loop_->insert_connection_cache(remote_host_, this);
    if (inserted) {
      LOG_INFO(event_loop_->GetLog(),
          "Server Socket %d to %s inserted into connection_cache_.",
          fd_, remote_host_.ToString().c_str());
    } else {
      LOG_WARN(event_loop_->GetLog(),
          "Failed to insert server socket %d connected to %s into cache",
          fd_, remote_host_.ToString().c_str());
      delete this;
    }
  }

  ~SocketEvent() {
    close(fd_);
    if (ev_) {
      ld_event_free(ev_);
    }
    // remove the socket from the connection cache
    bool removed  __attribute__((__unused__)) = 
      event_loop_->remove_connection_cache(remote_host_, this);
    assert(removed || !known_remote_);
    ld_event_free(write_ev_);
  }

  // One message message to be sent out.
  Status Enqueue(std::unique_ptr<Message> msg) {
    Status status;
    // insert into outgoing queue
    send_queue_.push_back(std::move(msg));
    assert(send_queue_.front().get() != nullptr);

    // If the write-ready event was not currently registered, then
    // register it now. The enqueued message will be sent out when the
    // socket is ready to be written.
    if (!write_ev_added_) {
      if (ld_event_add(write_ev_, nullptr)) {
        LOG_WARN(event_loop_->GetLog(),
            "Failed to add write event for %d", fd_);
        status = Status::InternalError("Failed to enqueue write message");
      } else {
        write_ev_added_ = true;
      }
    }
    return status;
  }

 private:
  static void EventCallback(evutil_socket_t fd, short what, void* arg) {
    if (what & EV_READ) {
      static_cast<SocketEvent*>(arg)->ReadCallback(fd);
    } else if (what & EV_WRITE) {
      static_cast<SocketEvent*>(arg)->WriteCallback(fd);
    } else if (what & EV_TIMEOUT) {
    } else if (what & EV_SIGNAL) {
    }
  }

  void WriteCallback(evutil_socket_t fd) {
    assert(write_ev_added_ && send_queue_.size() > 0);
    assert(send_queue_.front().get() != nullptr);

    while (send_queue_.size() > 0) {
      //
      // if there is any pending data from the previously sent
      // partial-message, then send it.
      if (partial_.size() > 0) {
        assert(send_queue_.size() > 0);
        int count = write(fd_, (const void *)partial_.data(), partial_.size());
        if (count == -1) {
          LOG_WARN(event_loop_->info_log_,
              "Wanted to write %d bytes to remote host but connection closed.",
              (int)partial_.size());
          event_loop_->info_log_->Flush();
          // write error, close connection.
          delete this;
          return;
        }
        if ((unsigned int)count != partial_.size()) {
          LOG_WARN(event_loop_->info_log_,
              "Wanted to write %d bytes to remote host but only %d success.",
              (int)partial_.size(), (int)count);
          event_loop_->info_log_->Flush();
          // update partial data pointers
          partial_ = Slice(partial_.data() + count, partial_.size() - count);
          return;
        }
        // The partial message is completely sent out. Remove it from queue.
        partial_ = Slice();
        send_queue_.pop_front();
      }

      // No more partial data to be sent out.
      if (send_queue_.size() > 0) {
        // If there are any new pending messages, serialize it.
        Message* msg = send_queue_.front().get();
        partial_ = msg->Serialize();
        assert(partial_.size() > 0);
      } else {
        // No more queued messages. Switch off ready-to-write event on socket.
        if (ld_event_del(write_ev_)) {
          LOG_WARN(event_loop_->GetLog(),
              "Failed to remove write event for %d", fd_);
        } else {
          write_ev_added_ = false;
        }
      }
    }
  }

  void ReadCallback(evutil_socket_t fd) {
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
        // If this is the first complete message received on this socket,
        // then insert this connection into the connection cache_.
        if (!known_remote_) {
          known_remote_ = true;
          remote_host_ = msg.get()->GetOrigin();
          bool inserted = event_loop_->insert_connection_cache(
                            remote_host_, this);
          if (inserted) {
            LOG_INFO(event_loop_->GetLog(),
                "Client Socket %d connected to %s "
                "inserted into connection_cache_.",
                fd_, remote_host_.ToString().c_str());
          }
        }

        // Invoke the callback for this message.
        event_loop_->Dispatch(std::move(msg));
      } else {
        // Failed to decode message.
        LOG_WARN(event_loop_->GetLog(), "Failed to decode message");
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
  std::unique_ptr<char[]> msg_buf_;  // receive buffer
  evutil_socket_t fd_;
  event* ev_;
  event* write_ev_;
  EventLoop* event_loop_;
  bool known_remote_;      // we know the remote identity
  HostId remote_host_;     // id of remote entity
  bool write_ev_added_;    // is the write event added?

  // The list of outgoing messages.
  // partial_ records the next valid offset in the earliest message.
  std::deque<std::unique_ptr<Message>> send_queue_;
  Slice partial_;
};

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
  ld_event_base_loopexit(obj->base_, nullptr);
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
    LOG_WARN(obj->info_log_,
        "Reading from command pipe failed. errno=%d", errno);
    obj->info_log_->Flush();
    return;
  } else if (received < static_cast<ssize_t>(sizeof(data))) {
    // Partial read from the pipe. This may happen if the write call is
    // interrupted by a signal.
    LOG_WARN(obj->info_log_,
        "Partial read from command pipe.");
    obj->info_log_->Flush();
    return;
  }
  // Take ownership of the command.
  std::unique_ptr<Command> command(data);

  if (command.get()->IsSendCommand()) {
    // If this is a message-send command, then process it inline.
    // Have to handle the case when the message-send failed to write
    // to output socket and have to invoke *some* callback to the app. XXX
    HostId remote = command.get()->GetDestination();
    SocketEvent* sev = obj->lookup_connection_cache(remote);

    // If the remote side has not yet established a connection, then
    // create a new connection and insert into connection cache.
    if (sev == nullptr) {
      sev = obj->setup_connection(remote);
    }
    if (sev != nullptr) {
      // enqueue data to SocketEvent queue. This message will be sent out
      // when the output socket is ready to write.
      std::unique_ptr<Message> msg = command.get()->GetMessage();
      assert(msg.get() != nullptr);
      sev->Enqueue(std::move(msg));
    } else {
      LOG_WARN(obj->info_log_,
          "No Socket to send msg to host %s, msg dropped...",
          remote.ToString().c_str());
      obj->info_log_->Flush();
    }
  } else if (obj->command_callback_) {
    // Pass ownership to the callback (it can decide whether or not to delete).
    obj->command_callback_(std::move(command));
  }
}

void
EventLoop::do_accept(struct evconnlistener *listener,
                     evutil_socket_t fd,
                     struct sockaddr *address,
                     int socklen,
                     void *arg) {
  EventLoop* event_loop = static_cast<EventLoop *>(arg);
  setup_fd(fd, event_loop);

  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  new SocketEvent(event_loop, fd, event_loop->base_);

  LOG_INFO(event_loop->info_log_, "Accept successful on socket %d", fd);
}

//
// Sets up the socket descriptor appropriately.
Status
EventLoop::setup_fd(evutil_socket_t fd, EventLoop* event_loop) {
  Status status;

  // make socket non-blocking
  if (ld_evutil_make_socket_nonblocking(fd) != 0) {
    status = Status::InternalError("Unable to make socket non-blocking");
  }

  // Set buffer sizes.
  if (event_loop->env_options_.tcp_send_buffer_size) {
    int sz = event_loop->env_options_.tcp_send_buffer_size;
    int r = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
    if (r) {
      LOG_WARN(event_loop->info_log_,
          "Failed to set send buffer size on socket %d", fd);
      status = Status::InternalError("Failed to set send buffer size");
    }
  }

  if (event_loop->env_options_.tcp_recv_buffer_size) {
    int sz = event_loop->env_options_.tcp_recv_buffer_size;
    int r = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
    if (r) {
      LOG_WARN(event_loop->info_log_,
          "Failed to set receive buffer size on socket %d", fd);
      status = Status::InternalError("Failed to set receive buffer size");
    }
  }
  return status;
}

void
EventLoop::accept_error_cb(struct evconnlistener *listener, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  struct event_base *base = ld_evconnlistener_get_base(listener);
  int err = EVUTIL_SOCKET_ERROR();
  LOG_WARN(obj->info_log_,
    "Got an error %d (%s) on the listener. "
    "Shutting down.\n", err, evutil_socket_error_to_string(err));
  ld_event_base_loopexit(base, NULL);
}

void
EventLoop::Run(void) {
  base_ = ld_event_base_new();
  if (!base_) {
    LOG_WARN(info_log_,
      "Failed to create an event base for an EventLoop thread");
    info_log_->Flush();
    return;
  }

  // Port number <= 0 indicates that there is no accept loop.
  if (port_number_ > 0) {
    struct sockaddr_in6 sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin6_family = AF_INET6;
    sin.sin6_addr = in6addr_any;
    sin.sin6_port = htons(port_number_);

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
      LOG_WARN(info_log_,
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
    LOG_WARN(info_log_, "Failed to create first startup event");
    info_log_->Flush();
    return;
  }
  struct timeval zero_seconds = {0, 0};
  int rv = evtimer_add(startup_event, &zero_seconds);
  if (rv != 0) {
    LOG_WARN(info_log_, "Failed to add startup event to event base");
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
    LOG_WARN(info_log_, "Failed to create shutdown event");
    info_log_->Flush();
    return;
  }
  rv = ld_event_add(shutdown_event_, nullptr);
  if (rv != 0) {
    LOG_WARN(info_log_, "Failed to add shutdown event to event base");
    info_log_->Flush();
    return;
  }

  // This is a pipe that other threads can write to for asynchronous
  // processing on the event thread.
  if (pipe(command_pipe_fds_)) {
    LOG_WARN(info_log_, "Failed to create command pipe.");
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
    LOG_WARN(info_log_, "Failed to create command pipe event");
    info_log_->Flush();
    return;
  }
  rv = ld_event_add(command_pipe_event_, nullptr);
  if (rv != 0) {
    LOG_WARN(info_log_, "Failed to add command event to event base");
    info_log_->Flush();
    return;
  }

  LOG_INFO(info_log_, "Starting EventLoop at port %d", port_number_);
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
    LOG_INFO(info_log_, "Stopped EventLoop at port %d", port_number_);
    info_log_->Flush();
    base_ = nullptr;
  }
}

Status EventLoop::SendCommand(std::unique_ptr<Command> command) {
  // Check the pipe hasn't been closed due to partial write (see below).
  if (command_pipe_fds_[1] == 0) {
    LOG_WARN(info_log_,
        "Trying to write to closed command pipe, errno=%d", errno);
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
    LOG_WARN(info_log_, "Error writing command to event loop, errno=%d", errno);
    info_log_->Flush();
    return Status::InternalError("write returned error");
  } else {
    // Partial write. This is bad; only part of the pointer has been written
    // to the pipe. This should never happen, but if it does we need to close
    // the pipe so no more data is written to it.
    LOG_WARN(info_log_,
      "Partial write of command to event loop, written=%zd", written);

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

// Adds an entry into the connection cache
// Returns true if the object was inserted successfully,
// otherwise returns false if the object already existed.
bool
EventLoop::insert_connection_cache(const HostId& host, SocketEvent* sev) {
  auto ret = connection_cache_.insert(
    std::pair<HostId, SocketEvent*>(host, sev));
  if (!ret.second) {
    return false;   // object already existed
  }
  return true;  // successfully inserted
}

// Removes an entry from the connection cache.
// Returns true if the object existed before this call,
// otherwise returns false if the object was not found.
bool
EventLoop::remove_connection_cache(const HostId& host, SocketEvent* sev) {
  auto iter = connection_cache_.find(host);
  if (iter != connection_cache_.end()) {
    assert(iter->second == sev);
    connection_cache_.erase(iter);
    return true;    // deleted successfully
  }
  return false;     // not found
}

// Finds an entry from the connection cache.
// Returns null if the entry does not exist
SocketEvent*
EventLoop::lookup_connection_cache(const HostId& host) const {
  auto iter = connection_cache_.find(host);
  if (iter != connection_cache_.end()) {
    return iter->second;
  }
  return nullptr;     // not found
}

// Creates a socket connection to specified host and
// inserts it into connection cache.
// Returns null on error.
SocketEvent*
EventLoop::setup_connection(const HostId& host) {
  int fd;
  Status status =  create_connection(host, false, &fd);
  if (!status.ok()) {
    return nullptr;
  }

  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  SocketEvent* sev = new SocketEvent(this, fd, base_, host);

  LOG_INFO(info_log_,
      "Connect to %s scheduled on socket %d",
      host.ToString().c_str(), fd);
  return sev;
}


// Create a connection to the specified host
Status
EventLoop::create_connection(const HostId& host,
                             bool blocking,
                             int* fd) {
  struct addrinfo hints, *servinfo, *p;
  int rv;
  std::string port_string(std::to_string(host.port));
  int sockfd;

  // handle both IPV4 and IPV6 addresses.
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(host.hostname.c_str(), port_string.c_str(),
                        &hints, &servinfo)) != 0) {
      return Status::IOError("getaddrinfo: " + host.hostname +
                             ":" + port_string);
  }
  // loop through all the results and connect to the first we can
  for (p = servinfo; p != nullptr; p = p->ai_next) {
      if ((sockfd = socket(p->ai_family, p->ai_socktype,
              p->ai_protocol)) == -1) {
          continue;
      }

      // set non-blocking, if requested. Do this before the
      // connect call.
      if (!blocking) {
        auto flags = fcntl(sockfd, F_GETFL, 0);
        if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK)) {
          close(sockfd);
          continue;
        }
      }

      int one = 1;
      setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

      if (env_options_.tcp_send_buffer_size) {
        int sz = env_options_.tcp_send_buffer_size;
        setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
      }

      if (env_options_.tcp_recv_buffer_size) {
        int sz = env_options_.tcp_recv_buffer_size;
        setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
      }

      if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
          if (!blocking && errno == EINPROGRESS) {
            // if this is a nonblocking socket, then connect might
            // not be successful immediately. This is not a problem
            // because it can still be added to select or poll.
          } else {
            close(sockfd);
            continue;
          }
      }
      break;
  }
  if (p == nullptr) {
      return Status::IOError("failed to connect: " + host.hostname +
                             ":" + port_string);
  }
  freeaddrinfo(servinfo);
  *fd = sockfd;
  return Status::OK();
}

/**
 * Constructor for a Message Loop
 */
EventLoop::EventLoop(EnvOptions env_options,
                     int port_number,
                     const std::shared_ptr<Logger>& info_log,
                     EventCallbackType event_callback,
                     CommandCallbackType command_callback) :
  env_options_(env_options),
  port_number_(port_number),
  running_(false),
  base_(nullptr),
  info_log_(info_log),
  event_callback_(event_callback),
  command_callback_(command_callback),
  event_callback_context_(nullptr),
  listener_(nullptr) {
  LOG_INFO(info_log, "Created a new Event Loop at port %d", port_number);
}

EventLoop::~EventLoop() {
  // stop dispatch loop
  Stop();
}

}  // namespace rocketspeed
