//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "event_loop.h"

#include <limits.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <deque>
#include <functional>
#include <thread>
#include <tuple>
#include <vector>

#include "src/messages/event2_version.h"
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "src/port/port.h"
#include "src/messages/serializer.h"
#include "src/util/common/coding.h"

static_assert(std::is_same<evutil_socket_t, int>::value,
  "EventLoop assumes evutil_socket_t is int.");

namespace rocketspeed {

const int EventLoop::kLogSeverityDebug = _EVENT_LOG_DEBUG;
const int EventLoop::kLogSeverityMsg = _EVENT_LOG_MSG;
const int EventLoop::kLogSeverityWarn = _EVENT_LOG_WARN;
const int EventLoop::kLogSeverityErr = _EVENT_LOG_ERR;

#define ROCKETSPEED_CURRENT_MSG_VERSION 1

struct MessageHeader {
  /**
   * Attempts to parse slice into a MessageHeader.
   *
   * @param in Input bytes (will be advanced past header).
   * @param header Output header.
   * @return ok() if fully parsed, error otherwise.
   */
  static Status Parse(Slice* in, MessageHeader* header) {
    if (!GetFixed8(in, &header->version)) {
      return Status::InvalidArgument("Bad version");
    }
    if (!GetFixed32(in, &header->size)) {
      return Status::InvalidArgument("Bad size");
    }
    return Status::OK();
  }

  /**
   * @return Header encoded as string.
   */
  std::string ToString() {
    std::string result;
    result.reserve(encoding_size);
    PutFixed8(&result, version);
    PutFixed32(&result, size);
    return result;
  }

  uint8_t version;
  uint32_t size;

  /** Size of MessageHeader encoding */
  static constexpr size_t encoding_size = sizeof(version) + sizeof(size);
};

struct TimestampedString {
  std::string string;
  uint64_t issued_time;
};

class SocketEvent {
 public:
  static std::unique_ptr<SocketEvent> Create(EventLoop* event_loop, int fd) {
    std::unique_ptr<SocketEvent> sev(new SocketEvent(event_loop, fd, false));

    // register only the read callback
    if (sev->ev_ == nullptr ||
        sev->write_ev_ == nullptr ||
        event_add(sev->ev_, nullptr)) {
      LOG_ERROR(event_loop->GetLog(),
          "Failed to create socket event for fd(%d)", fd);
      return nullptr;
    }
    return sev;
  }

  // this constructor is used by server-side connection initiation
  static std::unique_ptr<SocketEvent> Create(EventLoop* event_loop,
                                             int fd,
                                             const ClientID& destination) {
    std::unique_ptr<SocketEvent> sev(new SocketEvent(event_loop, fd, true));

    // register only the read callback
    if (sev->ev_ == nullptr ||
        sev->write_ev_ == nullptr ||
        event_add(sev->ev_, nullptr)) {
      LOG_ERROR(event_loop->GetLog(),
          "Failed to create socket event for fd(%d)", fd);
      return nullptr;
    }

    // Set destination of the socket, so that it can be reused.
    sev->destination_ = std::move(destination);
    return sev;
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
    event_free(write_ev_);
    close(fd_);
  }

  // One message to be sent out.
  Status Enqueue(const std::shared_ptr<TimestampedString>& msg) {
    event_loop_->thread_check_.Check();

    send_queue_.emplace_back(msg);

    // If the write-ready event is not currently registered, and the socket
    // is ready for writing, then we'll try to write immediately. If the
    // socket isn't ready for writing, we'll queue up the message, add a write
    // event and wait until its ready.
    if (!write_ev_added_) {
      if (ready_for_writing_) {
        // Try to write everything now.
        WriteCallback();
      }

      if (!send_queue_.empty()) {
        // Failed to write everything, so add a write event to notify us
        // later when writing is available on this socket.
        if (event_add(write_ev_, nullptr)) {
          LOG_WARN(event_loop_->GetLog(),
              "Failed to add write event for fd(%d)", fd_);
          return Status::InternalError("Failed to enqueue write message");
        }
        write_ev_added_ = true;
      }
    }
    return Status::OK();
  }

  const ClientID& GetDestination() const {
    return destination_;
  }

  std::list<std::unique_ptr<SocketEvent>>::iterator GetListHandle() const {
    return list_handle_;
  }

  void SetListHandle(std::list<std::unique_ptr<SocketEvent>>::iterator it) {
    list_handle_ = it;
  }

 private:
  SocketEvent(EventLoop* event_loop, int fd, bool initiated)
  : hdr_idx_(0)
  , msg_idx_(0)
  , msg_size_(0)
  , fd_(fd)
  , ev_(nullptr)
  , write_ev_(nullptr)
  , event_loop_(event_loop)
  , write_ev_added_(false)
  , was_initiated_(initiated)
  , ready_for_writing_(false) {
    // Can only add events from the event loop thread.
    event_loop->thread_check_.Check();

    // Create read and write events
    ev_ = event_new(
        event_loop->base_, fd, EV_READ|EV_PERSIST, EventCallback, this);
    write_ev_ = event_new(
        event_loop->base_, fd, EV_WRITE|EV_PERSIST, EventCallback, this);
  }

  static void EventCallback(evutil_socket_t /*fd*/, short what, void* arg) {
    SocketEvent* sev = static_cast<SocketEvent*>(arg);
    Status st;
    if (what & EV_READ) {
      st = sev->ReadCallback();
    } else if (what & EV_WRITE) {
      sev->ready_for_writing_ = true;
      st = sev->WriteCallback();
    } else if (what & EV_TIMEOUT) {
    } else if (what & EV_SIGNAL) {
    }

    if (!st.ok()) {
      // Inform MsgLoop that clients have disconnected.
      auto origin_type = sev->was_initiated_
                             ? MessageGoodbye::OriginType::Server
                             : MessageGoodbye::OriginType::Client;

      EventLoop* event_loop = sev->event_loop_;
      // TODO(stupaq) this is a hack for now to preserve semantics of goodbye
      ClientID destination = sev->GetDestination();
      // Remove and close streams that were assigned to this connection.
      auto globals = event_loop->stream_router_.RemoveConnection(sev);
      // Delete the socket event.
      event_loop->teardown_connection(sev);
      sev = nullptr;

      if (origin_type == MessageGoodbye::OriginType::Server) {
        std::unique_ptr<Message> msg(
            new MessageGoodbye(Tenant::InvalidTenant,
                               destination,
                               MessageGoodbye::Code::SocketError,
                               origin_type));
        event_loop->Dispatch(std::move(msg));
      } else {
        for (StreamID global : globals) {
          // We send goodbye using the global stream IDs, as these are only
          // known by the entity using the loop.
          std::unique_ptr<Message> msg(
              new MessageGoodbye(Tenant::InvalidTenant,
                                 global,
                                 MessageGoodbye::Code::SocketError,
                                 origin_type));
          event_loop->Dispatch(std::move(msg));
        }
      }
    }
  }

  Status WriteCallback() {
    event_loop_->thread_check_.Check();
    assert(send_queue_.size() > 0);

    while (send_queue_.size() > 0) {
      //
      // if there is any pending data from the previously sent
      // partial-message, then send it.
      if (partial_.size() > 0) {
        assert(send_queue_.size() > 0);
        ssize_t count = write(fd_, partial_.data(), partial_.size());
        if (count == -1) {
          LOG_WARN(event_loop_->info_log_,
              "Wanted to write %d bytes to remote host fd(%d) but encountered "
              "errno(%d) \"%s\".",
              (int)partial_.size(), fd_, errno, strerror(errno));
          event_loop_->info_log_->Flush();
          if (errno != EAGAIN && errno != EWOULDBLOCK) {
            // write error, close connection.
            return Status::IOError("write call failed ", std::to_string(errno));
          }
          return Status::OK();
        }
        if ((unsigned int)count != partial_.size()) {
          LOG_WARN(event_loop_->info_log_,
              "Wanted to write %d bytes to remote host fd(%d) but only "
              "%d bytes written successfully.",
              (int)partial_.size(), fd_, (int)count);
          event_loop_->info_log_->Flush();
          // update partial data pointers
          partial_ = Slice(partial_.data() + count, partial_.size() - count);
          return Status::OK();
        } else {
          LOG_INFO(event_loop_->info_log_,
              "Successfully wrote %ld bytes to remote host fd(%d)",
              count, fd_);
        }
        // The partial message is completely sent out. Remove it from queue.
        partial_.clear();

        event_loop_->stats_.write_latency->Record(
            event_loop_->env_->NowMicros() - send_queue_.front()->issued_time);
        send_queue_.pop_front();
      }

      // No more partial data to be sent out.
      if (send_queue_.size() > 0) {
        // If there are any new pending messages, start processing it.
        partial_ = send_queue_.front()->string;
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
    return Status::OK();
  }

  Status ReadCallback() {
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
            return Status::IOError("read call failed ", std::to_string(errno));
          }
          return Status::OK();
        }
        total_read += n;
        hdr_idx_ += n;
        if (n < count) {
          // Still more header to be read, wait for next event.
          return Status::OK();
        }

        // Now have read header, prepare msg buffer.
        Slice hdr_slice(hdr_buf_, sizeof(hdr_buf_));
        MessageHeader hdr;
        Status st = MessageHeader::Parse(&hdr_slice, &hdr);
        if (!st.ok()) {
          return st;
        }
        msg_size_ = hdr.size;
        msg_buf_.reset(new char[msg_size_]);
        msg_idx_ = 0;
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
          return Status::IOError("read call failed ", std::to_string(errno));
        }
        return Status::OK();
      }
      total_read += n;
      msg_idx_ += n;
      if (n < count) {
        // Still more message to be read, wait for next event.
        return Status::OK();
      }

      // Now have whole message, process it.
      std::unique_ptr<Message> msg =
        Message::CreateNewInstance(std::move(msg_buf_), msg_size_);

      if (msg) {
        if (!was_initiated_) {
          // Attempt to add this stream/socket pair to the stream map.
          StreamID local = msg->GetOrigin();
          // Insert connection and remap stream ID.
          bool inserted;
          StreamID global;
          std::tie(inserted, global) =
              event_loop_->stream_router_.InsertInboundStream(this, local);
          // Overwrite stream ID in the message.
          msg->SetOrigin(global);

          // Log a new inbound stream.
          if (inserted) {
            LOG_INFO(event_loop_->GetLog(),
                     "New stream (%s) was associated with socket fd(%d)",
                     global.c_str(),
                     fd_);
          }

          // EventLoop needs to process goodbye messages.
          if (msg->GetMessageType() == MessageType::mGoodbye) {
            MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
            LOG_INFO(event_loop_->GetLog(),
                     "Received goodbye message (code %d) for stream (%s)",
                     static_cast<int>(goodbye->GetCode()),
                     global.c_str());
            // Update stream router.
            bool removed;
            SocketEvent* sev;
            std::tie(removed, sev, std::ignore) =
                event_loop_->stream_router_.RemoveStream(global);
            assert(removed);
            if (sev) {
              assert(sev == this);
              LOG_INFO(event_loop_->GetLog(),
                       "Socket fd(%d) has no more streams on it.",
                       sev->fd_);
            }
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
    return Status::OK();
  }

  size_t hdr_idx_;
  char hdr_buf_[MessageHeader::encoding_size];
  size_t msg_idx_;
  size_t msg_size_;
  std::unique_ptr<char[]> msg_buf_;  // receive buffer
  evutil_socket_t fd_;
  event* ev_;
  event* write_ev_;
  EventLoop* event_loop_;
  bool write_ev_added_;    // is the write event added?
  bool was_initiated_;   // was this connection initiated by us?
  bool ready_for_writing_;  // is the socket ready for writing?
  /**
   * A remote destination, if non-empty the socket can be reused by anyone, who
   * wants to talk the remote host.
   */
  ClientID destination_;

  // Handle into the EventLoop's socket event list (for fast removal).
  std::list<std::unique_ptr<SocketEvent>>::iterator list_handle_;

  // The list of outgoing messages.
  // partial_ records the next valid offset in the earliest message.
  std::deque<std::shared_ptr<TimestampedString>> send_queue_;
  Slice partial_;
};

class AcceptCommand : public Command {
 public:
  explicit AcceptCommand(int fd)
      : fd_(fd) {}

  CommandType GetCommandType() const { return kAcceptCommand; }

  int GetFD() const { return fd_; }

 private:
  int fd_;
};

Status StreamRouter::GetOutboundStream(const SendCommand::StreamSpec& spec,
                                       EventLoop* event_loop,
                                       SocketEvent** out_sev,
                                       StreamID* out_local) {
  thread_check_.Check();

  assert(out_sev);
  assert(out_local);
  StreamID global = spec.stream;
  const ClientID& destination = spec.destination;

  // Find global -> (connection, local).
  if (open_streams_.FindLocalAndContext(global, out_sev, out_local)) {
    return Status::OK();
  }
  // We don't know about the stream, so if the destination was not provided, we
  // have to drop the message.
  if (destination.empty()) {
    return Status::InternalError(
        "Stream is not opened and destination was not provided.");
  }
  {  // Look for connection based on destination.
    const auto it_c = open_connections_.find(destination);
    if (it_c != open_connections_.end()) {
      SocketEvent* found_sev = it_c->second;
      // Open the stream, reusing the socket.
      open_streams_.InsertGlobal(global, found_sev);
      *out_sev = found_sev;
      *out_local = global;
      return Status::OK();
    }
  }
  // We know the destination, but cannot reuse connection. Create a new one.
  HostId host = HostId::ToHostId(destination);
  SocketEvent* new_sev = event_loop->setup_connection(host, destination);
  if (!new_sev) {
    return Status::InternalError("Failed to create a new connection.");
  }
  // Open the stream, using the new socket.
  open_streams_.InsertGlobal(global, new_sev);
  *out_sev = new_sev;
  *out_local = global;
  return Status::OK();
}

std::pair<bool, StreamID> StreamRouter::InsertInboundStream(
    SocketEvent* new_sev,
    StreamID local) {
  thread_check_.Check();

  // Insert into global <-> (connection, local) map and allocate global.
  auto result = open_streams_.GetGlobal(new_sev, local);
  StreamID global = result.second;
  // Insert into destination -> connection cache.
  const ClientID& destination = new_sev->GetDestination();
  if (!destination.empty()) {
    // This connection has a known remote endpoint, we can reuse it later on.
    // In case of any conflicts, just remove the old connection mapping, it
    // will not disturb existing streams, but can only affect future choice
    // of connection for that destination.
    open_connections_[destination] = new_sev;
  }
  return result;
}

std::tuple<StreamRouter::RemovalStatus, SocketEvent*, StreamID>
StreamRouter::RemoveStream(StreamID global) {
  thread_check_.Check();

  RemovalStatus status;
  SocketEvent* sev;
  StreamID local;
  status = open_streams_.RemoveGlobal(global, &sev, &local);
  if (status == RemovalStatus::kRemovedLast) {
    // We don't have streams on this connection.
    // Remove destination -> connection mapping and return the pointer, so
    // the connection can be closed.
    const auto it_c = open_connections_.find(sev->GetDestination());
    // Note that we skip removal from destination cache if the connections do
    // not match, as we could potentially replace connection with another one to
    // the same destination.
    if (it_c != open_connections_.end() && it_c->second == sev) {
      open_connections_.erase(it_c);
    }
  }
  return std::make_tuple(status, sev, local);
}

std::vector<StreamID> StreamRouter::RemoveConnection(SocketEvent* sev) {
  thread_check_.Check();

  std::vector<StreamID> result;
  {  // Remove all open streams for this connection.
    auto removed = open_streams_.RemoveContext(sev);
    for (const auto& entry : removed) {
      StreamID global = entry.second;
      result.push_back(global);
    }
  }
  // Remove mapping from destination to the connection.
  const auto it_c = open_connections_.find(sev->GetDestination());
  if (it_c != open_connections_.end() && it_c->second == sev) {
    open_connections_.erase(it_c);
  }
  return result;
}

void EventLoop::RegisterCallback(CommandType type,
                                 CommandCallbackType callbacks) {
  // Cannot modify callbacks after the loop has started.
  assert(!IsRunning());

  // Cannnot modify internal callbacks.
  assert(type != CommandType::kAcceptCommand);
  assert(type != CommandType::kSendCommand);
  assert(type != CommandType::kExecuteCommand);

  // We do not allow any duplicates.
  assert(command_callbacks_.find(type) == command_callbacks_.end());

  command_callbacks_[type] = callbacks;
}

void EventLoop::HandleSendCommand(std::unique_ptr<Command> command,
                                  uint64_t issued_time) {
  // Need using otherwise SendCommand is confused with the member function.
  using rocketspeed::SendCommand;
  SendCommand* send_cmd = static_cast<SendCommand*>(command.get());

  auto msg = std::make_shared<TimestampedString>();
  send_cmd->GetMessage(&msg->string);
  msg->issued_time = issued_time;
  assert (!msg->string.empty());

  // Have to handle the case when the message-send failed to write
  // to output socket and have to invoke *some* callback to the app.
  for (const SendCommand::StreamSpec& spec : send_cmd->GetDestinations()) {
    // Find or create a connection and original stream ID.
    SocketEvent* sev = nullptr;
    StreamID local;
    Status st = stream_router_.GetOutboundStream(spec, this, &sev, &local);

    if (st.ok()) {
      assert(sev);
      assert(!local.empty());

      if (spec.stream != local) {
        LOG_DEBUG(info_log_,
                  "Stream ID (%s) converted to local (%s)",
                  spec.stream.c_str(),
                  local.c_str());
      }
      // Use stream ID local to the socket when sending back the message.
      {  // TODO(stupaq, pja) take origin out of the message
        std::unique_ptr<char[]> buffer = Slice(msg->string).ToUniqueChars();
        std::unique_ptr<Message> message =
            Message::CreateNewInstance(std::move(buffer), msg->string.size());
        message->SetOrigin(local);
        message->SerializeToString(&msg->string);
      }

      // Enqueue data to SocketEvent queue. This message will be sent out
      // when the output socket is ready to write.
      MessageHeader header { ROCKETSPEED_CURRENT_MSG_VERSION,
                             static_cast<uint32_t>(msg->string.size()) };
      auto hdr = std::make_shared<TimestampedString>();
      hdr->string = header.ToString();
      hdr->issued_time = issued_time;

      // Add message header and contents.
      st = sev->Enqueue(hdr);
      if (st.ok()) {
        st = sev->Enqueue(msg);
      }
    }
    // No else, so we catch error on adding to queue as well.

    if (!st.ok()) {
      LOG_WARN(info_log_,
               "Failed to send message on stream (%s) to host '%s': %s",
               spec.stream.c_str(),
               spec.destination.c_str(),
               st.ToString().c_str());
      info_log_->Flush();
    } else {
      LOG_DEBUG(info_log_,
                "Enqueued message on stream (%s) to host '%s': %s",
                spec.stream.c_str(),
                spec.destination.c_str(),
                st.ToString().c_str());
    }
  }
}

void EventLoop::HandleAcceptCommand(std::unique_ptr<Command> command,
                                    uint64_t issued_time) {
  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  thread_check_.Check();
  AcceptCommand* accept_cmd = static_cast<AcceptCommand*>(command.get());
  std::unique_ptr<SocketEvent> sev = SocketEvent::Create(this,
                                                         accept_cmd->GetFD());
  if (sev) {
    all_sockets_.emplace_front(std::move(sev));
    all_sockets_.front()->SetListHandle(all_sockets_.begin());
    active_connections_.fetch_add(1, std::memory_order_acq_rel);
    stats_.accepts->Add(1);
  }
}

//
// This callback is fired from the first aritificial timer event
// in the dispatch loop.
void
EventLoop::do_startevent(evutil_socket_t listener, short event, void *arg) {
  EventLoop* obj = static_cast<EventLoop *>(arg);
  obj->thread_check_.Check();
  obj->start_status_ = Status::OK();
  obj->running_ = true;
  obj->start_signal_.Post();
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
  if (obj->command_ready_eventfd_.read_event(&available)) {
    LOG_WARN(obj->info_log_, "Failed to read eventfd value, errno=%d", errno);
    obj->info_log_->Flush();
    return;
  }

  // Read commands from the queue (there might have been multiple
  // commands added since we have received the last notification).
  while (available--) {
    TimestampedCommand ts_cmd;

    // Read a command from the queue.
    if (!obj->command_queue_.read(ts_cmd)) {
      // This should not happen.
      LOG_WARN(obj->info_log_, "Wrong number of available commands reported");
      obj->info_log_->Flush();
      return;
    }

    // Call registered callback.
    obj->Dispatch(std::move(ts_cmd.command), ts_cmd.issued_time);
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
    socklen_t sizeof_sz = static_cast<socklen_t>(sizeof(sz));
    int r = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof_sz);
    if (r) {
      LOG_WARN(event_loop->info_log_,
          "Failed to set send buffer size on socket fd(%d)", fd);
      status = Status::InternalError("Failed to set send buffer size");
    }
  }

  if (event_loop->env_options_.tcp_recv_buffer_size) {
    int sz = event_loop->env_options_.tcp_recv_buffer_size;
    socklen_t sizeof_sz = static_cast<socklen_t>(sizeof(sz));
    int r = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof_sz);
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
EventLoop::Run() {

/**
 * gcc complains that a format string that is not constant is
 * a security risk. Switch off security check for this piece of code.
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-security"
  auto start_error = [&] (const std::string& error) {
    LOG_ERROR(info_log_, error.c_str());
    info_log_->Flush();
    start_status_ = Status::InternalError(error);
    start_signal_.Post();
  };
#pragma GCC diagnostic pop

  thread_check_.Reset();
  base_ = event_base_new();
  if (!base_) {
    start_error("Failed to create an event base for an EventLoop thread");
    return;
  }

  // Port number <= 0 indicates that there is no accept loop.
  if (port_number_ > 0) {
    sockaddr_in6 sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin6_family = AF_INET6;
    sin.sin6_addr = in6addr_any;
    sin.sin6_port = htons(static_cast<uint16_t>(port_number_));

    // Create libevent connection listener.
    listener_ = evconnlistener_new_bind(
      base_,
      &EventLoop::do_accept,
      reinterpret_cast<void*>(this),
      LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
      -1,  // backlog
      reinterpret_cast<sockaddr*>(&sin),
      static_cast<int>(sizeof(sin)));

    if (listener_ == nullptr) {
      start_error("Failed to create connection listener on port " +
        std::to_string(port_number_));
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
    start_error("Failed to create first startup event");
    return;
  }
  timeval zero_seconds = {0, 0};
  int rv = evtimer_add(startup_event_, &zero_seconds);
  if (rv != 0) {
    start_error("Failed to add startup event to event base");
    return;
  }

  // An event that signals new commands in the command queue.
  if (shutdown_eventfd_.status() < 0) {
    start_error("Failed to create eventfd for shutdown commands");
    return;
  }

  // Create a shutdown event that will run when we want to stop the loop.
  // It creates an eventfd that the loop listens for reads on. When a read
  // is available, that indicates that the loop should stop.
  // This allows us to communicate to the event loop from another thread
  // safely without locks.
  shutdown_event_ = event_new(
    base_,
    shutdown_eventfd_.readfd(),
    EV_PERSIST|EV_READ,
    this->do_shutdown,
    reinterpret_cast<void*>(this));
  if (shutdown_event_ == nullptr) {
    start_error("Failed to create shutdown event");
    return;
  }
  rv = event_add(shutdown_event_, nullptr);
  if (rv != 0) {
    start_error("Failed to add shutdown event to event base");
    return;
  }

  // An event that signals new commands in the command queue.
  if (command_ready_eventfd_.status() < 0) {
    start_error("Failed to create eventfd for waiting commands");
    return;
  }
  command_ready_event_ = event_new(
    base_,
    command_ready_eventfd_.readfd(),
    EV_PERSIST|EV_READ,
    this->do_command,
    reinterpret_cast<void*>(this));
  if (command_ready_event_ == nullptr) {
    start_error("Failed to create command queue event");
    return;
  }
  rv = event_add(command_ready_event_, nullptr);
  if (rv != 0) {
    start_error("Failed to add command event to event base");
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
    if (running_) {
      // Write to the shutdown event FD to signal the event loop thread
      // to shutdown and stop looping.
      int result;
      do {
        result = shutdown_eventfd_.write_event(1);
      } while (running_ && (result < 0 || errno == EAGAIN));

      // Wait for event loop to exit on the loop thread.
      while (running_) {
        std::this_thread::yield();
      }
    }

    // Shutdown everything
    if (listener_) {
      evconnlistener_free(listener_);
    }
    if (startup_event_) event_free(startup_event_);
    if (shutdown_event_) event_free(shutdown_event_);
    if (command_ready_event_) event_free(command_ready_event_);
    event_base_free(base_);
    command_ready_eventfd_.closefd();
    shutdown_eventfd_.closefd();

    // Reset the thread checker since the event loop thread is exiting.
    thread_check_.Reset();
    stream_router_.CloseAll();
    teardown_all_connections();
    LOG_INFO(info_log_, "Stopped EventLoop at port %d", port_number_);
    info_log_->Flush();
    base_ = nullptr;
  }
}

Status EventLoop::SendCommand(std::unique_ptr<Command> command) {
  TimestampedCommand ts_cmd { std::move(command), env_->NowMicros() };
  bool success = command_queue_.write(std::move(ts_cmd));

  if (!success) {
    // The queue was full and the write failed.
    LOG_WARN(info_log_, "The command queue is full");
    info_log_->Flush();
    return Status::NoBuffer();
  }

  // Write to the command_ready_eventfd_ to send an event to the reader.
  if (command_ready_eventfd_.write_event(1)) {
    // Some internal error happened.
    LOG_ERROR(info_log_,
        "Error writing a notification to eventfd, errno=%d", errno);
    info_log_->Flush();
    return Status::InternalError("eventfd_write returned error");
  }

  return Status::OK();
}

void EventLoop::Accept(int fd) {
  // May be called from another thread, so must add to the command queue.
  std::unique_ptr<Command> command(new AcceptCommand(fd));
  SendCommand(std::move(command));
}

void EventLoop::Dispatch(std::unique_ptr<Message> message) {
  event_callback_(std::move(message));
}

void EventLoop::Dispatch(std::unique_ptr<Command> command,
                         int64_t issued_time) {
  uint64_t now = env_->NowMicros();
  stats_.command_latency->Record(now - issued_time);
  stats_.commands_processed->Add(1);

  // Search for callback registered for this command type.
  // Command ownership will be passed along to the callback.
  const auto type = command->GetCommandType();
  auto iter = command_callbacks_.find(type);
  if (iter != command_callbacks_.end()) {
    iter->second(std::move(command), issued_time);
  } else {
    // If the user has not registered a callback for this command type, then
    // the command will be droped silently.
    LOG_WARN(
        info_log_, "No registered command callback for command type %d", type);
    info_log_->Flush();
  }
}

Status EventLoop::WaitUntilRunning(std::chrono::seconds timeout) {
  if (!running_) {
    if (!start_signal_.TimedWait(timeout)) {
      return Status::TimedOut();
    }
  }
  return start_status_;
}

// Removes an socket event created by setup_connection.
void EventLoop::teardown_connection(SocketEvent* sev) {
  thread_check_.Check();
  all_sockets_.erase(sev->GetListHandle());
  active_connections_.fetch_sub(1, std::memory_order_acq_rel);
}

// Clears out the connection cache
void EventLoop::teardown_all_connections() {
  while (!all_sockets_.empty()) {
    teardown_connection(all_sockets_.front().get());
  }
}

// Creates a socket connection to specified host, returns null on error.
SocketEvent*
EventLoop::setup_connection(const HostId& host, const ClientID& remote_client) {
  thread_check_.Check();
  int fd;
  Status status = create_connection(host, false, &fd);
  if (!status.ok()) {
    LOG_WARN(info_log_,
             "create_connection to %s failed: %s",
             host.ToString().c_str(),
             status.ToString().c_str());
    return nullptr;
  }

  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  std::unique_ptr<SocketEvent> sev = SocketEvent::Create(this,
                                                         fd,
                                                         remote_client);
  if (!sev) {
    return nullptr;
  }
  all_sockets_.emplace_front(std::move(sev));
  all_sockets_.front()->SetListHandle(all_sockets_.begin());
  active_connections_.fetch_add(1, std::memory_order_acq_rel);

  LOG_INFO(info_log_,
      "Connect to %s scheduled on socket fd(%d)",
      host.ToString().c_str(), fd);
  return all_sockets_.front().get();
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
                             ":" + port_string + ":" + gai_strerror(rv));
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
      socklen_t sizeof_one = static_cast<socklen_t>(sizeof(one));
      setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof_one);

      if (env_options_.tcp_send_buffer_size) {
        int sz = env_options_.tcp_send_buffer_size;
        socklen_t sizeof_sz = static_cast<socklen_t>(sizeof(sz));
        setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof_sz);
      }

      if (env_options_.tcp_recv_buffer_size) {
        int sz = env_options_.tcp_recv_buffer_size;
        socklen_t sizeof_sz = static_cast<socklen_t>(sizeof(sz));
        setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof_sz);
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
                     StreamAllocator inbound_alloc,
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
  shutdown_eventfd_(rocketspeed::port::Eventfd(true, true)),
  command_queue_(command_queue_size),
  command_ready_eventfd_(rocketspeed::port::Eventfd(true, true)),
  stream_router_(std::move(inbound_alloc)),
  active_connections_(0),
  stats_(stats_prefix) {

  // Setup callbacks.
  command_callbacks_[CommandType::kAcceptCommand] = [this](
      std::unique_ptr<Command> command, uint64_t issued_time) {
    HandleAcceptCommand(std::move(command), issued_time);
  };
  command_callbacks_[CommandType::kSendCommand] = [this](
      std::unique_ptr<Command> command, uint64_t issued_time) {
    HandleSendCommand(std::move(command), issued_time);
  };
  command_callbacks_[CommandType::kExecuteCommand] = [](
      std::unique_ptr<Command> command, uint64_t issued_time) {
    static_cast<ExecuteCommand*>(command.get())->Execute();
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

int EventLoop::GetNumClients() const {
  return static_cast<int>(stream_router_.GetNumStreams());
}

}  // namespace rocketspeed
