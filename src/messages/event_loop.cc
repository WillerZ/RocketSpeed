//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "event_loop.h"

#include <limits.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <deque>
#include <functional>
#include <thread>

#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>
#include "src/messages/event2_version.h"

#include "src/port/port.h"
#include "src/messages/serializer.h"

static_assert(std::is_same<evutil_socket_t, int>::value,
  "EventLoop assumes evutil_socket_t is int.");

namespace rocketspeed {

const int EventLoop::kLogSeverityDebug = _EVENT_LOG_DEBUG;
const int EventLoop::kLogSeverityMsg = _EVENT_LOG_MSG;
const int EventLoop::kLogSeverityWarn = _EVENT_LOG_WARN;
const int EventLoop::kLogSeverityErr = _EVENT_LOG_ERR;

class SocketEvent {
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
    event_loop->thread_check_.Check();

    // create read and write events
    ev_ = event_new(base, fd, EV_READ|EV_PERSIST,
                       EventCallback, this);
    write_ev_ = event_new(base, fd, EV_WRITE|EV_PERSIST,
                       EventCallback, this);

    // register only the read callback
    if (ev_ == nullptr || write_ev_ == nullptr ||
        event_add(ev_, nullptr)) {
      LOG_WARN(event_loop_->GetLog(),
          "Failed to create socket event for fd(%d)", fd);
      delete this;
    }
  }

  // this constructor is used by server-side connection initiation
  SocketEvent(EventLoop* event_loop, int fd, event_base* base,
              const ClientID& remote)
  : hdr_idx_(0)
  , msg_idx_(0)
  , msg_size_(0)
  , fd_(fd)
  , ev_(nullptr)
  , event_loop_(event_loop)
  , known_remote_(true)
  , remote_host_(remote)
  , write_ev_added_(false) {
    event_loop_->thread_check_.Check();

    // create read and write events
    ev_ = event_new(base, fd, EV_READ|EV_PERSIST,
                       EventCallback, this);
    write_ev_ = event_new(base, fd, EV_WRITE|EV_PERSIST,
                       EventCallback, this);
    // register only the read callback
    if (ev_ == nullptr || write_ev_  == nullptr ||
        event_add(ev_, nullptr)) {
      LOG_WARN(event_loop_->GetLog(),
          "Failed to create socket event for fd(%d)", fd);
      delete this;
    }

    // Insert the write_ev_ into the connection cache. This is
    // necessary so that further outgoing messages get queued up
    // on this write event.
    bool inserted = event_loop_->insert_connection_cache(remote_host_, this);
    if (inserted) {
      LOG_INFO(event_loop_->GetLog(),
          "Server Socket fd(%d) to %s inserted into connection_cache_.",
          fd_, remote_host_.c_str());
    } else {
      LOG_WARN(event_loop_->GetLog(),
          "Failed to insert server socket fd(%d) connected to %s into cache",
          fd_, remote_host_.c_str());
      delete this;
    }
  }

  ~SocketEvent() {
    event_loop_->thread_check_.Check();
    LOG_INFO(event_loop_->GetLog(),
             "Closing fd(%d)",
             fd_);
    event_loop_->GetLog()->Flush();
    if (ev_) {
      event_free(ev_);
    }
    // remove the socket from the connection cache
    bool removed  __attribute__((__unused__)) =
      event_loop_->remove_connection_cache(remote_host_, this);
    assert(!event_loop_->running_ || removed || !known_remote_);
    event_free(write_ev_);
    close(fd_);

    // free up unsent pending messages
    for (const auto msg : send_queue_) {
      msg->refcount--;
      if (msg->refcount == 0) {
        event_loop_->FreeString(msg);
      }
    }
  }

  // One message to be sent out.
  Status Enqueue(SharedString* msg) {
    event_loop_->thread_check_.Check();
    Status status;
    // insert into outgoing queue
    send_queue_.push_back(msg);
    assert(msg->refcount);

    // If the write-ready event was not currently registered, then
    // register it now. The enqueued message will be sent out when the
    // socket is ready to be written.
    if (!write_ev_added_) {
      // Try to write everything now.
      WriteCallback(fd_);

      if (!send_queue_.empty()) {
        // Failed to write everything, so add a write event to notify us
        // later when writing is available on this socket.
        if (event_add(write_ev_, nullptr)) {
          LOG_WARN(event_loop_->GetLog(),
              "Failed to add write event for fd(%d)", fd_);
          status = Status::InternalError("Failed to enqueue write message");
        } else {
          write_ev_added_ = true;
        }
      }
    }
    return status;
  }

  evutil_socket_t GetFd() {
    return fd_;
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
    event_loop_->thread_check_.Check();
    assert(send_queue_.size() > 0);

    while (send_queue_.size() > 0) {
      //
      // if there is any pending data from the previously sent
      // partial-message, then send it.
      if (partial_.size() > 0) {
        assert(send_queue_.size() > 0);
        int count = write(fd_, (const void *)partial_.data(), partial_.size());
        if (count == -1) {
          LOG_WARN(event_loop_->info_log_,
              "Wanted to write %d bytes to remote host fd(%d) but encountered "
              "errno(%d) \"%s\".",
              (int)partial_.size(), fd_, errno, strerror(errno));
          event_loop_->info_log_->Flush();
          if (errno != EAGAIN && errno != EWOULDBLOCK) {
            // write error, close connection.
            delete this;
          }
          return;
        }
        if ((unsigned int)count != partial_.size()) {
          LOG_WARN(event_loop_->info_log_,
              "Wanted to write %d bytes to remote host fd(%d) but only "
              "%d success.",
              (int)partial_.size(), fd_, (int)count);
          event_loop_->info_log_->Flush();
          // update partial data pointers
          partial_ = Slice(partial_.data() + count, partial_.size() - count);
          return;
        } else {
          LOG_INFO(event_loop_->info_log_,
              "Successfully wrote %d bytes to remote host fd(%d)",
              count, fd_);
        }
        // The partial message is completely sent out. Remove it from queue.
        partial_ = Slice();
        SharedString* str = send_queue_.front();
        if (--(str->refcount) == 0) {
          event_loop_->FreeString(str);
        }
        send_queue_.pop_front();
        event_loop_->stats_.write_latency->Record(
          event_loop_->env_->NowMicros() - str->command_issue_time);
      }

      // No more partial data to be sent out.
      if (send_queue_.size() > 0) {
        // If there are any new pending messages, start processing it.
        partial_ = Slice(send_queue_.front()->store);
        assert(partial_.size() > 0);
      } else if (write_ev_added_) {
        // No more queued messages. Switch off ready-to-write event on socket.
        if (event_del(write_ev_)) {
          LOG_WARN(event_loop_->GetLog(),
              "Failed to remove write event for fd(%d)", fd_);
        } else {
          write_ev_added_ = false;
        }
      }
    }
  }

  void ReadCallback(evutil_socket_t fd) {
    event_loop_->thread_check_.Check();
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
                "Client Socket fd(%d) connected to %s "
                "inserted into connection_cache_.",
                fd_, remote_host_.c_str());
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
  ClientID remote_host_;   // id of remote entity
  bool write_ev_added_;    // is the write event added?

  // The list of outgoing messages.
  // partial_ records the next valid offset in the earliest message.
  std::deque<SharedString*> send_queue_;
  Slice partial_;
};

class AcceptCommand : public Command {
 public:
  explicit AcceptCommand(int fd,
                         uint64_t issued_time)
      : Command(issued_time),
        fd_(fd) {}

  CommandType GetCommandType() const { return kAcceptCommand; }

  int GetFD() const { return fd_; }

 private:
  int fd_;
};

void EventLoop::RegisterCallback(CommandType type,
                                 CommandCallbackType callbacks) {
  // Cannot modify callbacks after the loop has started.
  assert(!IsRunning());

  // Cannnot modify internal callbacks.
  assert(type != CommandType::kAcceptCommand);
  assert(type != CommandType::kSendCommand);

  // We do not allow any duplicates.
  assert(command_callbacks_.find(type) == command_callbacks_.end());

  command_callbacks_[type] = callbacks;
}

void EventLoop::HandleSendCommand(std::unique_ptr<Command> command) {
  // Need using otherwise SendCommand is confused with the member function.
  using rocketspeed::SendCommand;
  SendCommand* send_cmd = static_cast<SendCommand*>(command.get());

  // Have to handle the case when the message-send failed to write
  // to output socket and have to invoke *some* callback to the app.
  const SendCommand::Recipients& remote = send_cmd->GetDestination();

  // Move the message payload into a ref-counted string. The string
  // will be deallocated only when all the remote destinations
  // have been served.
  std::string out;
  send_cmd->GetMessage(&out);
  assert(out.size() > 0);
  SharedString* msg = AllocString(std::move(out),
                                  remote.size(),
                                  send_cmd->GetIssuedTime());

  // Increment ref count again in case it is deleted inside Enqueue.
  msg->refcount++;
  for (const ClientID& clientid : remote) {
    Status status;
    SocketEvent* sev = lookup_connection_cache(clientid);

    // If the remote side has not yet established a connection, and
    // this is a new request then create a new connection and insert
    // into connection cache. if this is not a new request but is a
    // response to some other request, then the connection to the
    // remote side should already exist.
    if (sev == nullptr && send_cmd->IsNewRequest()) {
      HostId host = HostId::ToHostId(clientid);
      sev = setup_connection(host, clientid);
    }
    if (sev != nullptr) {
      // Enqueue data to SocketEvent queue. This message will be sent out
      // when the output socket is ready to write.
      status = sev->Enqueue(msg);
    }
    if (sev == nullptr || !status.ok()) {
      if (sev == nullptr) {
        LOG_WARN(info_log_,
                 "No Socket to send msg to host %s, msg dropped...",
                 clientid.c_str());
      } else {
        LOG_WARN(info_log_,
                 "Failed to enqueue message (%s) to host %s",
                 status.ToString().c_str(),
                 clientid.c_str());
      }
      info_log_->Flush();
      msg->refcount--;  // unable to queue msg, decrement refcount
    }
  }  // for loop
  if (--msg->refcount == 0) {
    FreeString(msg);
  }
}

void EventLoop::HandleAcceptCommand(std::unique_ptr<Command> command) {
  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  AcceptCommand* accept_cmd = static_cast<AcceptCommand*>(command.get());
  new SocketEvent(this, accept_cmd->GetFD(), this->base_);
  stats_.accepts->Add(1);
}

//
// This callback is fired from the first aritificial timer event
// in the dispatch loop.
void
EventLoop::do_startevent(evutil_socket_t listener, short event, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  obj->thread_check_.Check();
  obj->running_ = true;
}

void
EventLoop::do_shutdown(evutil_socket_t listener, short event, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  obj->thread_check_.Check();
  event_base_loopexit(obj->base_, nullptr);
}

void EventLoop::do_command(evutil_socket_t listener, short event, void* arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  obj->thread_check_.Check();

  // Read the value from the eventfd to find out how many commands are ready.
  uint64_t available;
  if (eventfd_read(obj->command_ready_eventfd_, &available)) {
    LOG_WARN(obj->info_log_, "Failed to read eventfd value, errno=%d", errno);
    obj->info_log_->Flush();
    return;
  }

  // Read commands from the queue (there might have been multiple
  // commands added since we have received the last notification).
  while (available--) {
    std::unique_ptr<Command> command;

    // Read a command from the queue.
    if (!obj->command_queue_.read(command)) {
      // This should not happen.
      LOG_WARN(obj->info_log_, "Wrong number of available commands reported");
      obj->info_log_->Flush();
      return;
    }

    uint64_t now = obj->env_->NowMicros();
    obj->stats_.command_latency->Record(now - command->GetIssuedTime());
    obj->stats_.commands_processed->Add(1);

    // Search for callback registered for this command type.
    // Command ownership will be passed along to the callback.
    const auto type = command->GetCommandType();
    auto iter = obj->command_callbacks_.find(type);
    if (iter != obj->command_callbacks_.end()) {
      iter->second(std::move(command));
    } else {
      // If the user has not registered a callback for this command type, then
      // the command will be droped silently.
      LOG_WARN(obj->info_log_,
               "No registered command callback for command type %d",
               type);
      obj->info_log_->Flush();
    }
  }
}

void
EventLoop::do_accept(evconnlistener *listener,
                     evutil_socket_t fd,
                     sockaddr *address,
                     int socklen,
                     void *arg) {
  EventLoop* event_loop = static_cast<EventLoop *>(arg);
  event_loop->thread_check_.Check();
  setup_fd(fd, event_loop);
  event_loop->accept_callback_(fd);
  LOG_INFO(event_loop->info_log_, "Accept successful on socket fd(%d)", fd);
}

//
// Sets up the socket descriptor appropriately.
Status
EventLoop::setup_fd(evutil_socket_t fd, EventLoop* event_loop) {
  event_loop->thread_check_.Check();
  Status status;

  // make socket non-blocking
  if (evutil_make_socket_nonblocking(fd) != 0) {
    status = Status::InternalError("Unable to make socket non-blocking");
  }

  // Set buffer sizes.
  if (event_loop->env_options_.tcp_send_buffer_size) {
    int sz = event_loop->env_options_.tcp_send_buffer_size;
    int r = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
    if (r) {
      LOG_WARN(event_loop->info_log_,
          "Failed to set send buffer size on socket fd(%d)", fd);
      status = Status::InternalError("Failed to set send buffer size");
    }
  }

  if (event_loop->env_options_.tcp_recv_buffer_size) {
    int sz = event_loop->env_options_.tcp_recv_buffer_size;
    int r = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
    if (r) {
      LOG_WARN(event_loop->info_log_,
          "Failed to set receive buffer size on socket fd(%d)", fd);
      status = Status::InternalError("Failed to set receive buffer size");
    }
  }
  return status;
}

void
EventLoop::accept_error_cb(evconnlistener *listener, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  obj->thread_check_.Check();
  event_base *base = evconnlistener_get_base(listener);
  int err = EVUTIL_SOCKET_ERROR();
  LOG_WARN(obj->info_log_,
    "Got an error %d (%s) on the listener. "
    "Shutting down.\n", err, evutil_socket_error_to_string(err));
  event_base_loopexit(base, NULL);
}

void
EventLoop::Run(void) {
  base_ = event_base_new();
  if (!base_) {
    LOG_WARN(info_log_,
      "Failed to create an event base for an EventLoop thread");
    info_log_->Flush();
    return;
  }

  // Port number <= 0 indicates that there is no accept loop.
  if (port_number_ > 0) {
    sockaddr_in6 sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin6_family = AF_INET6;
    sin.sin6_addr = in6addr_any;
    sin.sin6_port = htons(port_number_);

    // Create libevent connection listener.
    listener_ = evconnlistener_new_bind(
      base_,
      &EventLoop::do_accept,
      reinterpret_cast<void*>(this),
      LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
      -1,  // backlog
      reinterpret_cast<sockaddr*>(&sin),
      sizeof(sin));

    if (listener_ == nullptr) {
      LOG_WARN(info_log_,
          "Failed to create connection listener on port %d", port_number_);
      info_log_->Flush();
      return;
    }

    evconnlistener_set_error_cb(listener_, &EventLoop::accept_error_cb);
  }

  // Create a non-persistent event that will run as soon as the dispatch
  // loop is run. This is the first event to ever run on the dispatch loop.
  // The firing of this artificial event indicates that the event loop
  // is up and running.
  startup_event_ = evtimer_new(
    base_,
    this->do_startevent,
    reinterpret_cast<void*>(this));

  if (startup_event_ == nullptr) {
    LOG_WARN(info_log_, "Failed to create first startup event");
    info_log_->Flush();
    return;
  }
  timeval zero_seconds = {0, 0};
  int rv = evtimer_add(startup_event_, &zero_seconds);
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
  shutdown_event_ = event_new(
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
  rv = event_add(shutdown_event_, nullptr);
  if (rv != 0) {
    LOG_WARN(info_log_, "Failed to add shutdown event to event base");
    info_log_->Flush();
    return;
  }

  // An event that signals new commands in the command queue.
  command_ready_eventfd_ = eventfd(0, 0);
  if (command_ready_eventfd_ < 0) {
    LOG_WARN(info_log_, "Failed to create eventfd for waiting commands");
    info_log_->Flush();
    return;
  }
  command_ready_event_ = event_new(
    base_,
    command_ready_eventfd_,
    EV_PERSIST|EV_READ,
    this->do_command,
    reinterpret_cast<void*>(this));
  if (command_ready_event_ == nullptr) {
    LOG_WARN(info_log_, "Failed to create command queue event");
    info_log_->Flush();
    return;
  }
  rv = event_add(command_ready_event_, nullptr);
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
  event_base_dispatch(base_);
  running_ = false;
}

void EventLoop::Stop() {
  if (base_ != nullptr) {
    int shutdown_fd = event_get_fd(shutdown_event_);
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
        evconnlistener_free(listener_);
      }
      event_free(startup_event_);
      event_free(shutdown_event_);
      event_free(command_ready_event_);
      event_base_free(base_);
      close(shutdown_fd);
      close(command_ready_eventfd_);

      // Reset the thread checker for clear_connection_cache since the
      // event loop thread is no longer running.
      thread_check_.Reset();
      clear_connection_cache();
    }
    LOG_INFO(info_log_, "Stopped EventLoop at port %d", port_number_);
    info_log_->Flush();
    base_ = nullptr;
  }
}

Status EventLoop::SendCommand(std::unique_ptr<Command> command) {
  bool success = command_queue_.write(std::move(command));

  if (!success) {
    // The queue was full and the write failed.
    LOG_WARN(info_log_, "The command queue is full");
    info_log_->Flush();
    return Status::InternalError("Full command queue");
  }

  // Write to the command_ready_eventfd_ to send an event to the reader.
  if (eventfd_write(command_ready_eventfd_, 1)) {
    // Some internal error happened.
    LOG_WARN(info_log_,
        "Error writing a notification to eventfd, errno=%d", errno);
    info_log_->Flush();
    return Status::InternalError("eventfd_write returned error");
  }

  return Status::OK();
}

void EventLoop::Accept(int fd) {
  // May be called from another thread, so must add to the command queue.
  std::unique_ptr<Command> command(new AcceptCommand(fd, env_->NowMicros()));
  SendCommand(std::move(command));
}

void EventLoop::Dispatch(std::unique_ptr<Message> message) {
  event_callback_(std::move(message));
}

// Adds an entry into the connection cache
// Returns true if the object was inserted successfully,
// otherwise returns false if the object already existed.
bool
EventLoop::insert_connection_cache(const ClientID& host, SocketEvent* sev) {
  thread_check_.Check();
  auto iter = connection_cache_.find(host);

  // There exists a mapping about this host
  if (iter != connection_cache_.end()) {
    for (auto item: iter->second) {
      if (item == sev) {
        return false;   // pre-existing object
      }
    }
    iter->second.push_back(sev);  // add new object into cache
    return true;                  // new object added
  }

  // There isn't any mapping for this host.
  std::vector<SocketEvent*> newarray;
  newarray.push_back(sev);

  // Create first mapping for this host.
  auto ret = connection_cache_.insert(
    std::pair<ClientID, std::vector<SocketEvent*>>(host, newarray));
  if (!ret.second) {
    return false;   // object already existed
  }
  active_connections_.fetch_add(1, std::memory_order_acq_rel);
  assert(active_connections_ == connection_cache_.size());
  return true;  // successfully inserted
}

// Removes an entry from the connection cache.
// Returns true if the object existed before this call,
// otherwise returns false if the object was not found.
bool
EventLoop::remove_connection_cache(const ClientID& host, SocketEvent* sev) {
  thread_check_.Check();
  auto iter = connection_cache_.find(host);
  if (iter != connection_cache_.end()) {
    for (unsigned int i = 0; i < iter->second.size(); i++) {
      if (iter->second[i] == sev) {
        iter->second.erase(iter->second.begin() + i);
        if (iter->second.empty()) {
          // No more SocketEvents for this host, so remove the entry.
          connection_cache_.erase(host);
          active_connections_.fetch_sub(1, std::memory_order_acq_rel);
          assert(active_connections_ == connection_cache_.size());
        }
        return true;    // deleted successfully
      }
    }
  }
  return false;     // not found
}

// Finds an entry from the connection cache.
// If there are multiple socket connections to the specified host,
// then returns the latest connection to that host.
// Returns null if the host does not have any connected socket.
SocketEvent*
EventLoop::lookup_connection_cache(const ClientID& host) const {
  thread_check_.Check();
  auto iter = connection_cache_.find(host);
  if (iter != connection_cache_.end()) {
    assert(!iter->second.empty());
    if (!iter->second.empty()) {
      return iter->second.back();  // return the last element for now
    }
  }
  return nullptr;     // not found
}

// Clears out the connection cache
void
EventLoop::clear_connection_cache() {
  // accumulate all the pending SocketEvents in temporary vector
  std::vector<SocketEvent*> socks;
  for (auto iter = connection_cache_.begin();
       iter != connection_cache_.end(); ++iter) {
    for (auto sev : iter->second) {
      assert(sev);
      socks.push_back(sev);
    }
  }
  // Clears the connection cache.
  connection_cache_.clear();

  // Delete all pending SocketEvents. This is done after cleaning the
  // connect_cache_ because the destructor of SocketEvent checks to see
  // if the SocketEvent being destroyed is part of the connect_cache_.
  for(SocketEvent* sev : socks) {
    delete sev;
  }
}

// Creates a socket connection to specified host and
// inserts it into connection cache.
// Returns null on error.
SocketEvent*
EventLoop::setup_connection(const HostId& host, const ClientID& remote_client) {
  thread_check_.Check();
  int fd;
  Status status =  create_connection(host, false, &fd);
  if (!status.ok()) {
    LOG_WARN(info_log_,
             "create_connection to %s failed: %s",
             host.ToString().c_str(),
             status.ToString().c_str());
    return nullptr;
  }

  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  SocketEvent* sev = new SocketEvent(this, fd, base_, remote_client);

  LOG_INFO(info_log_,
      "Connect to %s scheduled on socket fd(%d)",
      host.ToString().c_str(), fd);
  return sev;
}


// Create a connection to the specified host
Status
EventLoop::create_connection(const HostId& host,
                             bool blocking,
                             int* fd) {
  thread_check_.Check();
  addrinfo hints, *servinfo, *p;
  int rv;
  std::string port_string(std::to_string(host.port));
  int sockfd;
  int last_errno = 0;

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
          last_errno = errno;
          continue;
      }

      // set non-blocking, if requested. Do this before the
      // connect call.
      if (!blocking) {
        auto flags = fcntl(sockfd, F_GETFL, 0);
        if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK)) {
          last_errno = errno;
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
          last_errno = errno;
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
                             ":" + port_string +
                             "last_errno:" + std::to_string(last_errno));
  }
  freeaddrinfo(servinfo);
  *fd = sockfd;
  return Status::OK();
}

void EventLoop::EnableDebugThreadUnsafe(DebugCallback log_cb) {
#if LIBEVENT_VERSION_NUMBER >= 0x02010300
  event_enable_debug_logging(EVENT_DBG_ALL);
  event_set_log_callback(log_cb);
  event_enable_debug_mode();
#endif
}

/**
 * Constructor for a Message Loop
 */
EventLoop::EventLoop(BaseEnv* env,
                     EnvOptions env_options,
                     int port_number,
                     const std::shared_ptr<Logger>& info_log,
                     EventCallbackType event_callback,
                     AcceptCallbackType accept_callback,
                     const std::string& stats_prefix,
                     uint32_t command_queue_size) :
  env_(env),
  env_options_(env_options),
  port_number_(port_number),
  running_(false),
  base_(nullptr),
  info_log_(info_log),
  event_callback_(std::move(event_callback)),
  accept_callback_(std::move(accept_callback)),
  listener_(nullptr),
  command_queue_(command_queue_size),
  active_connections_(0),
  stats_(stats_prefix) {

  // Setup callbacks.
  command_callbacks_[CommandType::kAcceptCommand] = [this](
      std::unique_ptr<Command> command) {
    HandleAcceptCommand(std::move(command));
  };
  command_callbacks_[CommandType::kSendCommand] = [this](
      std::unique_ptr<Command> command) {
    HandleSendCommand(std::move(command));
  };

  LOG_INFO(info_log, "Created a new Event Loop at port %d", port_number);
}

EventLoop::~EventLoop() {
  // stop dispatch loop
  Stop();
}

const char* EventLoop::SeverityToString(int severity) {
  if (severity == EventLoop::kLogSeverityDebug) {
    return "dbg";
  } else if (severity == EventLoop::kLogSeverityMsg) {
    return "msg";
  } else if (severity == EventLoop::kLogSeverityWarn) {
    return "wrn";
  } else if (severity == EventLoop::kLogSeverityErr) {
    return "err";
  } else {
    return "???"; // never reached
  }
}

void EventLoop::GlobalShutdown() {
  libevent_global_shutdown();
}

}  // namespace rocketspeed
