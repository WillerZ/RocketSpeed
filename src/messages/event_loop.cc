//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "event_loop.h"

#include <limits.h>
#include <sys/socket.h>
#include <sys/uio.h>
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

#include "external/folly/move_wrapper.h"

#include "src/port/port.h"
#include "src/messages/queues.h"
#include "src/messages/serializer.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/coding.h"

static_assert(std::is_same<evutil_socket_t, int>::value,
  "EventLoop assumes evutil_socket_t is int.");

namespace rocketspeed {

const int EventLoop::kLogSeverityDebug = _EVENT_LOG_DEBUG;
const int EventLoop::kLogSeverityMsg = _EVENT_LOG_MSG;
const int EventLoop::kLogSeverityWarn = _EVENT_LOG_WARN;
const int EventLoop::kLogSeverityErr = _EVENT_LOG_ERR;

#define ROCKETSPEED_CURRENT_MSG_VERSION 1

/**
 * Maximum number of iovecs to write at once. Note that an array of iovec will
 * be allocated on the stack with this length, so it should not be too high.
 */
static const size_t kMaxIovecs = 256;

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
    if (!sev->read_ev_ || !sev->write_ev_) {
      LOG_ERROR(event_loop->GetLog(),
          "Failed to create socket event for fd(%d)", fd);
      return nullptr;
    }
    sev->read_ev_->Enable();
    return sev;
  }

  // this constructor is used by server-side connection initiation
  static std::unique_ptr<SocketEvent> Create(EventLoop* event_loop,
                                             int fd,
                                             const HostId& destination) {
    std::unique_ptr<SocketEvent> sev(new SocketEvent(event_loop, fd, true));

    // register only the read callback
    if (!sev->read_ev_ || !sev->write_ev_) {
      LOG_ERROR(event_loop->GetLog(),
          "Failed to create socket event for fd(%d)", fd);
      return nullptr;
    }
    sev->read_ev_->Enable();

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
    read_ev_.reset();
    write_ev_.reset();
    close(fd_);
  }

  // One message to be sent out.
  Status Enqueue(std::shared_ptr<TimestampedString> msg) {
    event_loop_->thread_check_.Check();

    send_queue_.emplace_back(std::move(msg));

    // If the write-ready event is not currently registered, add a write
    // event and wait until its ready.
    if (!write_ev_added_) {
      write_ev_->Enable();
      write_ev_added_ = true;
    }
    return Status::OK();
  }

  const HostId& GetDestination() const {
    return destination_;
  }

  std::list<std::unique_ptr<SocketEvent>>::iterator GetListHandle() const {
    return list_handle_;
  }

  void SetListHandle(std::list<std::unique_ptr<SocketEvent>>::iterator it) {
    list_handle_ = it;
  }

  /**
   * Does everything necessary to close a socket connection: removes streams,
   * cleans up connection cache, dispatches goodbyes, and frees the SocketEvent.
   *
   * @param sev The SocketEvent to disconnect.
   * @param timed_out True is the socked is descroyed due to connect timeout.
   */
  static void Disconnect(SocketEvent* sev, bool timed_out) {
    // Inform MsgLoop that clients have disconnected.
    auto origin_type = sev->was_initiated_
                           ? MessageGoodbye::OriginType::Server
                           : MessageGoodbye::OriginType::Client;

    // Remove and close streams that were assigned to this connection.
    EventLoop* event_loop = sev->event_loop_;  // make a copy, sev is destroyed.
    auto globals = event_loop->stream_router_.RemoveConnection(sev);
    // Delete the socket event.
    event_loop->teardown_connection(sev, timed_out);
    for (StreamID global : globals) {
      // We send goodbye using the global stream IDs, as these are only
      // known by the entity using the loop.
      std::unique_ptr<Message> msg(
        new MessageGoodbye(Tenant::InvalidTenant,
                           MessageGoodbye::Code::SocketError,
                           origin_type));
      event_loop->Dispatch(std::move(msg), global);
    }
  }

 private:
  SocketEvent(EventLoop* event_loop, int fd, bool initiated)
  : hdr_idx_(0)
  , msg_idx_(0)
  , msg_size_(0)
  , fd_(fd)
  , event_loop_(event_loop)
  , write_ev_added_(false)
  , was_initiated_(initiated)
  , timeout_cancelled_(false) {
    // Can only add events from the event loop thread.
    event_loop->thread_check_.Check();

    // Create read and write events
    read_ev_ = EventCallback::CreateFdReadCallback(
      event_loop,
      fd,
      [this] () {
        if (!ReadCallback().ok()) {
          Disconnect(this, false);
        } else {
          ProcessHeartbeats();
        }
      });

    write_ev_ = EventCallback::CreateFdWriteCallback(
      event_loop,
      fd,
      [this] () {
        if (!WriteCallback().ok()) {
          Disconnect(this, false);
        } else {
          ProcessHeartbeats();
        }
      });
  }

  void ProcessHeartbeats() {
    if (event_loop_->heartbeat_enabled_) {
      event_loop_->heartbeat_.ProcessExpired(
        event_loop_->heartbeat_timeout_,
        event_loop_->heartbeat_expired_callback_,
        event_loop_->heartbeat_expire_batch_);
    }
  }

  Status WriteCallback() {
    event_loop_->thread_check_.Check();

    if (!timeout_cancelled_) {
      // This socket is now writable, so we can cancel the connect timeout.
      event_loop_->connect_timeout_.Erase(this);
      timeout_cancelled_ = true;
    }

    assert(send_queue_.size() > 0);

    // Sanity check stats.
    // write_succeed_* should have a record for all write_size_*
    assert(event_loop_->stats_.write_size_bytes->GetNumSamples() ==
           event_loop_->stats_.write_succeed_bytes->GetNumSamples());
    assert(event_loop_->stats_.write_size_iovec->GetNumSamples() ==
           event_loop_->stats_.write_succeed_iovec->GetNumSamples());

    while (send_queue_.size() > 0) {
      // if there is any pending data from the previously sent
      // partial-message, then send it.
      if (partial_.size() > 0) {
        assert(send_queue_.size() > 0);

        // Prepare iovecs.
        iovec iov[kMaxIovecs];
        int iovcnt = 0;
        int limit = static_cast<int>(std::min(kMaxIovecs, send_queue_.size()));
        size_t total = 0;
        for (; iovcnt < limit; ++iovcnt) {
          Slice v(iovcnt != 0 ? Slice(send_queue_[iovcnt]->string) : partial_);
          iov[iovcnt].iov_base = (void*)v.data();
          iov[iovcnt].iov_len = v.size();
          total += v.size();
        }

        event_loop_->stats_.write_size_bytes->Record(total);
        event_loop_->stats_.write_size_iovec->Record(iovcnt);
        event_loop_->stats_.socket_writes->Add(1);
        ssize_t count = writev(fd_, iov, iovcnt);
        if (count == -1) {
          auto e = errno;
          LOG_WARN(event_loop_->info_log_,
            "Wanted to write %zu bytes to remote host fd(%d) but encountered "
            "errno(%d) \"%s\".",
            total, fd_, e, strerror(e));
          event_loop_->stats_.write_succeed_bytes->Record(0);
          event_loop_->stats_.write_succeed_iovec->Record(0);
          event_loop_->info_log_->Flush();
          if (e != EAGAIN && e != EWOULDBLOCK) {
            // write error, close connection.
            return Status::IOError("write call failed: " +
                                   std::to_string(e));
          }
          return Status::OK();
        }
        event_loop_->stats_.write_succeed_bytes->Record(count);
        if (static_cast<size_t>(count) != total) {
          event_loop_->stats_.partial_socket_writes->Add(1);
          LOG_WARN(event_loop_->info_log_,
              "Wanted to write %zu bytes to remote host fd(%d) but only "
              "%zd bytes written successfully.",
              total, fd_, count);
        }

        size_t written = static_cast<size_t>(count);
        for (int i = 0; i < iovcnt; ++i) {
          assert(!send_queue_.empty());
          auto& item = send_queue_.front();
          if (i != 0) {
            partial_ = Slice(item->string);
          }
          if (written >= partial_.size()) {
            // Fully wrote section.
            written -= partial_.size();
          } else {
            // Only partially written, update partial and return.
            partial_.remove_prefix(written);
            event_loop_->stats_.write_succeed_iovec->Record(i);
            return Status::OK();
          }
          event_loop_->stats_.write_latency->Record(
            event_loop_->env_->NowMicros() - item->issued_time);
          send_queue_.pop_front();
        }
        event_loop_->stats_.write_succeed_iovec->Record(iovcnt);
        assert(written == 0);
        partial_.clear();

        LOG_DEBUG(event_loop_->info_log_,
                  "Successfully wrote %zd bytes to remote host fd(%d)",
                  count,
                  fd_);
      }

      // No more partial data to be sent out.
      if (send_queue_.size() > 0) {
        // If there are any new pending messages, start processing it.
        partial_ = send_queue_.front()->string;
        assert(partial_.size() > 0);
      } else if (write_ev_added_) {
        // No more queued messages. Switch off ready-to-write event on socket.
        write_ev_->Disable();
        write_ev_added_ = false;
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
            return Status::IOError("read call failed: " +
                                   std::to_string(errno));
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
          return Status::IOError("read call failed: " + std::to_string(errno));
        }
        return Status::OK();
      }
      total_read += n;
      msg_idx_ += n;
      if (n < count) {
        // Still more message to be read, wait for next event.
        return Status::OK();
      }
      // Now have whole message, reset state for next message.
      hdr_idx_ = 0;
      msg_idx_ = 0;
      // No reader state modification shall happen after this point.

      // Process received message.
      Slice in(msg_buf_.get(), msg_size_);

      // Decode the recipients.
      StreamID local = 0;
      if (!DecodeOrigin(&in, &local)) {
        continue;
      }

      // Decode the rest of the message.
      std::unique_ptr<Message> msg =
          Message::CreateNewInstance(std::move(msg_buf_), in);
      if (!msg) {
        LOG_WARN(event_loop_->GetLog(), "Failed to decode message");
        continue;
      }

      // We need to remap stream ID local to the connection into globally
      // (within MsgLoop) unique stream ID.
      StreamID global;
      // We do not allow incoming streams on outgoing connections.
      const bool do_insert = !was_initiated_;
      // If this is a response on a stream initiated by this message loop, we
      // will have the proper stream ID in a map, otherwise this is a request
      // from the remote host and we have to remap stream ID.
      auto remap = event_loop_->stream_router_.RemapInboundStream(
          this, local, do_insert, &global);

      // Proceed with a message only if remapping succeeded.
      if (remap == StreamRouter::RemapStatus::kNotInserted) {
        LOG_WARN(event_loop_->GetLog(),
                 "Failed to remap stream ID (%llu)",
                 local);
        continue;
      }

      // Log a new inbound stream.
      if (remap == StreamRouter::RemapStatus::kInserted) {
        LOG_INFO(event_loop_->GetLog(),
                 "New stream (%llu) was associated with socket fd(%d)",
                 global,
                 fd_);
      }

      if (do_insert && event_loop_->heartbeat_enabled_) {
        event_loop_->heartbeat_.Add(global);
      }

      const MessageType msg_type = msg->GetMessageType();
      if (msg_type == MessageType::mGoodbye) {
        MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
        LOG_INFO(event_loop_->GetLog(),
                 "Received goodbye message (code %d) for stream (%llu)",
                 static_cast<int>(goodbye->GetCode()),
                 global);
        // Update stream router.
        StreamRouter::RemovalStatus removed;
        SocketEvent* sev;
        std::tie(removed, sev, std::ignore) =
            event_loop_->stream_router_.RemoveStream(global);
        assert(StreamRouter::RemovalStatus::kNotRemoved != removed);
        if (sev) {
          assert(sev == this);
          LOG_INFO(event_loop_->GetLog(),
                   "Socket fd(%d) has no more streams on it.",
                   sev->fd_);
        }
      }

      assert(ValidateEnum(msg_type));
      event_loop_->stats_.messages_received[size_t(msg_type)]->Add(1);

      // Invoke the callback for this message.
      event_loop_->Dispatch(std::move(msg), global);
    }
    return Status::OK();
  }

  size_t hdr_idx_;
  char hdr_buf_[MessageHeader::encoding_size];
  size_t msg_idx_;
  size_t msg_size_;
  std::unique_ptr<char[]> msg_buf_;  // receive buffer
  evutil_socket_t fd_;
  std::unique_ptr<EventCallback> read_ev_;
  std::unique_ptr<EventCallback> write_ev_;
  EventLoop* event_loop_;
  bool write_ev_added_;    // is the write event added?
  bool was_initiated_;   // was this connection initiated by us?
  bool timeout_cancelled_;   // have we removed from EventLoop connect_timeout_?

  /**
   * A remote destination, if non-empty the socket can be reused by anyone, who
   * wants to talk the remote host.
   */
  HostId destination_;

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
  const HostId& destination = spec.destination;

  // Find global -> (connection, local).
  if (open_streams_.FindLocalAndContext(global, out_sev, out_local)) {
    return Status::OK();
  }
  // We don't know about the stream, so if the destination was not provided, we
  // have to drop the message.
  if (!destination) {
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
  SocketEvent* new_sev = event_loop->setup_connection(destination);
  if (!new_sev) {
    return Status::InternalError("Failed to create a new connection.");
  }
  // Open the stream, using the new socket.
  open_streams_.InsertGlobal(global, new_sev);
  *out_sev = new_sev;
  *out_local = global;
  return Status::OK();
}

StreamRouter::RemapStatus StreamRouter::RemapInboundStream(
    SocketEvent* sev,
    StreamID local,
    bool insert,
    StreamID* out_global) {
  assert(out_global);
  thread_check_.Check();

  // Insert into global <-> (connection, local) map and allocate global if
  // requested.
  auto result = open_streams_.GetGlobal(sev, local, insert, out_global);
  if (result == RemapStatus::kNotInserted) {
    // Do not insert into connection cache if we cannot open the input stream.
    return result;
  }
  // Insert into destination -> connection cache.
  const HostId& destination = sev->GetDestination();
  if (!!destination) {
    // This connection has a known remote endpoint, we can reuse it later on.
    // In case of any conflicts, just remove the old connection mapping, it
    // will not disturb existing streams, but can only affect future choice
    // of connection for that destination.
    open_connections_[destination] = sev;
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

void EventLoop::HandleSendCommand(std::unique_ptr<Command> command) {
  // Need using otherwise SendCommand is confused with the member function.
  using rocketspeed::SendCommand;
  SendCommand* send_cmd = static_cast<SendCommand*>(command.get());

  auto now = env_->NowMicros();
  auto msg = std::make_shared<TimestampedString>();
  send_cmd->GetMessage(&msg->string);
  msg->issued_time = now;
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

      if (spec.stream != local) {
        LOG_DEBUG(info_log_,
                  "Stream ID (%llu) converted to local (%llu)",
                  spec.stream,
                  local);
      }

      // Enqueue data to SocketEvent queue. This message will be sent out
      // when the output socket is ready to write.
      auto destinations = std::make_shared<TimestampedString>();
      EncodeOrigin(&destinations->string, local);
      destinations->issued_time = now;

      size_t frame_size = destinations->string.size() + msg->string.size();
      MessageHeader header { ROCKETSPEED_CURRENT_MSG_VERSION,
                             static_cast<uint32_t>(frame_size) };
      auto hdr = std::make_shared<TimestampedString>();
      hdr->string = header.ToString();
      hdr->issued_time = now;

      // Add message header, destinations, and contents.
      st = sev->Enqueue(std::move(hdr));
      if (st.ok()) {
        st = sev->Enqueue(std::move(destinations));
      }
      if (st.ok()) {
        st = sev->Enqueue(msg);
      }
    }
    // No else, so we catch error on adding to queue as well.

    if (!st.ok()) {
      LOG_WARN(info_log_,
               "Failed to send message on stream (%llu) to host '%s': %s",
               spec.stream,
               spec.destination.ToString().c_str(),
               st.ToString().c_str());
      info_log_->Flush();
    } else {
      LOG_DEBUG(info_log_,
                "Enqueued message on stream (%llu) to host '%s': %s",
                spec.stream,
                spec.destination.ToString().c_str(),
                st.ToString().c_str());
    }
  }
}

void EventLoop::HandleAcceptCommand(std::unique_ptr<Command> command) {
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
  obj->running_ = true;
  obj->start_signal_.Post();
}

void
EventLoop::do_timerevent(evutil_socket_t listener, short event, void *arg) {
  Timer* obj = static_cast<Timer*>(arg);
  obj->callback();
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
  LOG_FATAL(obj->info_log_,
    "Got an error %d (%s) on the listener. "
    "Shutting down.\n", err, evutil_socket_error_to_string(err));
  obj->internal_status_ = Status::InternalError("Accept error -- check logs");
  event_base_loopexit(base, nullptr);
}

Status
EventLoop::Initialize() {
  if (base_) {
    assert(false);
    return Status::InvalidArgument("EventLoop already initialized.");
  }

  base_ = event_base_new();
  if (!base_) {
    return Status::InternalError(
      "Failed to create an event base for an EventLoop thread");
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
      return Status::InternalError(
        "Failed to create connection listener on port " +
          std::to_string(port_number_));
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
    return Status::InternalError("Failed to create first startup event");
  }
  timeval zero_seconds = {0, 0};
  int rv = evtimer_add(startup_event_, &zero_seconds);
  if (rv != 0) {
    return Status::InternalError("Failed to add startup event to event base");
  }

  // An event that signals the shutdown of the event loop.
  if (shutdown_eventfd_.status() < 0) {
    return Status::InternalError(
      "Failed to create eventfd for shutdown commands");
  }

  // Create a shutdown event that will run when we want to stop the loop.
  // It creates an eventfd that the loop listens for reads on. When a read
  // is available, that indicates that the loop should stop.
  // This allows us to communicate to the event loop from another thread
  // safely without locks.
  shutdown_event_ =
    EventCallback::CreateFdReadCallback(
      this,
      shutdown_eventfd_.readfd(),
      [this] () {
        event_base_loopexit(base_, nullptr);
      });
  if (shutdown_event_ == nullptr) {
    return Status::InternalError("Failed to create shutdown event");
  }
  shutdown_event_->Enable();

  control_command_queue_ =
    std::make_shared<CommandQueue>(info_log_,
                                   queue_stats_,
                                   default_command_queue_size_);
  Status st = AddIncomingQueue(control_command_queue_);
  if (!st.ok()) {
    LOG_FATAL(info_log_, "Failed to add control command queue");
  }
  return st;
}

void EventLoop::Run() {
  if (!base_) {
    LOG_FATAL(info_log_, "EventLoop not initialized before use.");
    assert(false);
    return;
  }
  LOG_VITAL(info_log_, "Starting EventLoop at port %d", port_number_);
  info_log_->Flush();

  // Register a timer for checking expired connections.
  RegisterTimerCallback(
    [this] () {
      connect_timeout_.ProcessExpired(
        options_.connect_timeout,
        [](SocketEvent* sev) { SocketEvent::Disconnect(sev, true); },
        -1);
    },
    options_.connect_timeout);

  // Start the event loop.
  // This will not exit until Stop is called, or some error
  // happens within libevent.
  thread_check_.Reset();
  event_base_dispatch(base_);

  // Shutdown everything
  if (listener_) {
    evconnlistener_free(listener_);
  }
  if (startup_event_) {
    event_free(startup_event_);
  }
  for (auto& timer : timers_) {
    event_free(timer->loop_event);
  }
  incoming_queues_.clear();
  shutdown_event_.reset();
  teardown_all_connections();
  event_base_free(base_);

  if (!internal_status_.ok()) {
    LOG_ERROR(info_log_,
      "EventLoop loop stopped with error: %s",
      internal_status_.ToString().c_str());
  }

  stream_router_.CloseAll();
  LOG_VITAL(info_log_, "Stopped EventLoop at port %d", port_number_);
  info_log_->Flush();
  base_ = nullptr;

  running_ = false;
}

void EventLoop::Stop() {
  // Write to the shutdown event FD to signal the event loop thread
  // to shutdown and stop looping.
  LOG_VITAL(info_log_, "Stopping EventLoop");
  int result;
  do {
    result = shutdown_eventfd_.write_event(1);
  } while (result < 0 && errno == EAGAIN);
}

Status EventLoop::RegisterTimerCallback(TimerCallbackType callback,
                                        std::chrono::microseconds period) {
  assert(base_);
  assert(!IsRunning());

  std::unique_ptr<Timer> timer(new Timer(std::move(callback)));
  timer->loop_event = event_new(
    base_,
    -1,
    EV_PERSIST,
    static_cast<EventLoop*>(nullptr)->do_timerevent,
    reinterpret_cast<void*>(timer.get()));

  if (timer->loop_event == nullptr) {
    LOG_ERROR(info_log_, "Failed to create timer event");
    info_log_->Flush();
    return Status::InternalError("event_new returned error creating timer");
  }

  timeval timer_seconds;
  timer_seconds.tv_sec = period.count() / 1000000ULL;
  timer_seconds.tv_usec = period.count() % 1000000ULL;

  int rv = event_add(timer->loop_event, &timer_seconds);
  timers_.emplace_back(std::move(timer));

  if (rv != 0) {
    LOG_ERROR(info_log_, "Failed to add timer event to event base");
    info_log_->Flush();
    return Status::InternalError("event_add returned error while adding timer");
  }
  return Status::OK();
}

StreamSocket EventLoop::CreateOutboundStream(HostId destination) {
  return StreamSocket(std::move(destination), outbound_allocator_.Next());
}

const std::shared_ptr<CommandQueue>& EventLoop::GetThreadLocalQueue() {
  // Get the thread local command queue.
  std::shared_ptr<CommandQueue>* command_queue_ptr =
    static_cast<std::shared_ptr<CommandQueue>*>(command_queues_.Get());

  if (!command_queue_ptr) {
    // Doesn't exist yet, so create a new one.
    std::shared_ptr<CommandQueue> command_queue =
      CreateCommandQueue(default_command_queue_size_);

    // Set this as the thread local queue.
    command_queue_ptr = new std::shared_ptr<CommandQueue>(command_queue);
    command_queues_.Reset(command_queue_ptr);
  }
  return *command_queue_ptr;
}

std::shared_ptr<CommandQueue> EventLoop::CreateCommandQueue(size_t size) {
  if (size == 0) {
    // Use default size when size == 0.
    size = default_command_queue_size_;
  }
  auto command_queue =
      std::make_shared<CommandQueue>(info_log_, queue_stats_, size);
  Status st = AttachQueue(command_queue);
  if (!st.ok()) {
    LOG_ERROR(info_log_, "Failed to attach command queue to EventLoop");
  }
  return command_queue;
}

Status EventLoop::AttachQueue(std::shared_ptr<CommandQueue> command_queue) {
  // Attach the new command queue to the event loop.
  std::unique_ptr<Command> attach_command(
    MakeExecuteCommand([this, command_queue] () mutable {
      Status st = AddIncomingQueue(std::move(command_queue));
      if (!st.ok()) {
        LOG_FATAL(info_log_, "Failed to attach command queue to EventLoop: %s",
          st.ToString().c_str());
        internal_status_ = st;
        event_base_loopexit(base_, nullptr);
      }
    }));

  bool ok;
  {
    // Need to lock when writing to control_command_queue_ since it is shared.
    MutexLock lock(&control_command_mutex_);
    const bool check_thread = false;
    ok = control_command_queue_->Write(attach_command, check_thread);
  }

  if (!ok) {
    LOG_FATAL(info_log_, "Failed to add command queue to EventLoop");
    return Status::InternalError("Failed to add command queue to EventLoop");
  }
  return Status::OK();
}

Status EventLoop::AddIncomingQueue(
    std::shared_ptr<CommandQueue> command_queue) {
  // An event that signals new commands in the command queue.
  std::unique_ptr<IncomingQueue> incoming_queue(new IncomingQueue());
  incoming_queue->queue = std::move(command_queue);

  CommandQueue* queue = incoming_queue->queue.get();
  queue->RegisterReadCallback(
    this,
    [this] (std::unique_ptr<Command> cmd) {
      // Call registered callback.
      Dispatch(std::move(cmd));
      return true;
    });
  queue->SetReadEnabled(true);

  LOG_INFO(info_log_, "Added new command queue to EventLoop");
  incoming_queues_.emplace_back(std::move(incoming_queue));
  return Status::OK();
}

static void EventShim(int fd, short what, void* event) {
  assert(event);
  if (what & (EV_READ|EV_WRITE)) {
    static_cast<EventCallback*>(event)->Invoke();
  }
}

event* EventLoop::CreateFdReadEvent(int fd,
                                    void (*cb)(int, short, void*),
                                    void* arg) {
  return event_new(base_, fd, EV_PERSIST|EV_READ, cb, arg);
}

event* EventLoop::CreateFdWriteEvent(int fd,
                                     void (*cb)(int, short, void*),
                                     void* arg) {
  return event_new(base_, fd, EV_PERSIST|EV_WRITE, cb, arg);
}

Status EventLoop::SendCommand(std::unique_ptr<Command>& command) {
  // Send command using thread local queue.
  return GetThreadLocalQueue()->TryWrite(command) ?
    Status::OK() : Status::NoBuffer();
}

Status EventLoop::SendRequest(const Message& msg, StreamSocket* socket) {
  std::string serial;
  msg.SerializeToString(&serial);
  std::unique_ptr<Command> command(
      SerializedSendCommand::Request(std::move(serial), {socket}));
  Status st = SendCommand(command);
  if (st.ok()) {
    socket->Open();
  }
  return st;
}

Status EventLoop::SendResponse(const Message& msg, StreamID stream_id) {
  std::string serial;
  msg.SerializeToString(&serial);
  std::unique_ptr<Command> command(
      SerializedSendCommand::Response(std::move(serial), {stream_id}));
  return SendCommand(command);
}

void EventLoop::Accept(int fd) {
  // May be called from another thread, so must add to the command queue.
  std::unique_ptr<Command> command(new AcceptCommand(fd));
  SendCommand(command);
}

void EventLoop::Dispatch(std::unique_ptr<Message> message, StreamID origin) {
  event_callback_(std::move(message), origin);
}

void EventLoop::Dispatch(std::unique_ptr<Command> command) {
  stats_.commands_processed->Add(1);

  // Search for callback registered for this command type.
  // Command ownership will be passed along to the callback.
  const auto type = command->GetCommandType();
  auto iter = command_callbacks_.find(type);
  if (iter != command_callbacks_.end()) {
    iter->second(std::move(command));
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
  return Status::OK();
}

// Removes an socket event created by setup_connection.
void EventLoop::teardown_connection(SocketEvent* sev, bool timed_out) {
  thread_check_.Check();
  if (!timed_out) {
    connect_timeout_.Erase(sev);
  }
  all_sockets_.erase(sev->GetListHandle());
  active_connections_.fetch_sub(1, std::memory_order_acq_rel);
}

// Clears out the connection cache
void EventLoop::teardown_all_connections() {
  while (!all_sockets_.empty()) {
    teardown_connection(all_sockets_.front().get(), false);
  }
}

// Creates a socket connection to specified host, returns null on error.
SocketEvent*
EventLoop::setup_connection(const HostId& destination) {
  thread_check_.Check();
  int fd;
  Status status = create_connection(destination, &fd);
  if (!status.ok()) {
    LOG_WARN(info_log_,
             "create_connection to %s failed: %s",
             destination.ToString().c_str(),
             status.ToString().c_str());
    return nullptr;
  }

  // This object is managed by the event that it creates, and will destroy
  // itself during an EOF callback.
  std::unique_ptr<SocketEvent> sev = SocketEvent::Create(this,
                                                         fd,
                                                         destination);
  if (!sev) {
    return nullptr;
  }
  connect_timeout_.Add(sev.get());
  all_sockets_.emplace_front(std::move(sev));
  all_sockets_.front()->SetListHandle(all_sockets_.begin());
  active_connections_.fetch_add(1, std::memory_order_acq_rel);

  LOG_INFO(info_log_,
           "Connect to %s scheduled on socket fd(%d)",
           destination.ToString().c_str(),
           fd);
  return all_sockets_.front().get();
}

Status EventLoop::create_connection(const HostId& host, int* fd) {
  thread_check_.Check();
  int sockfd;

  const sockaddr* addr = host.GetSockaddr();
  if ((sockfd = socket(addr->sa_family, SOCK_STREAM, 0)) == -1) {
    goto abort_clean;
  }

  {  // Set into non-blocking mode before attempt to connect.
    auto flags = fcntl(sockfd, F_GETFL, 0);
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK)) {
      goto abort_socket;
    }
  }

  {  // Enable address reuse.
    int one = 1;
    socklen_t sizeof_one = static_cast<socklen_t>(sizeof(one));
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof_one);
  }

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

  if (connect(sockfd, addr, host.GetSocklen()) == -1) {
    if (errno != EINPROGRESS) {
      goto abort_socket;
    }
    // On non-blocking socket connect might not be successful immediately.
    // This is not a problem because it can still be added to select or poll.
  }

  *fd = sockfd;
  return Status::OK();

abort_socket:
  close(sockfd);

abort_clean:
  return Status::IOError("Failed to connect to: " + host.ToString() +
                         " errno: " + std::to_string(errno));
}

void EventLoop::EnableDebugThreadUnsafe(DebugCallback log_cb) {
#if LIBEVENT_VERSION_NUMBER >= 0x02010300
  event_enable_debug_logging(EVENT_DBG_ALL);
  event_set_log_callback(log_cb);
  event_enable_debug_mode();
#endif
}

static void CommandQueueUnrefHandler(void* ptr) {
  std::shared_ptr<CommandQueue>* command_queue =
    static_cast<std::shared_ptr<CommandQueue>*>(ptr);
  delete command_queue;
}

EventLoop::EventLoop(BaseEnv* env,
                     EnvOptions env_options,
                     int port_number,
                     const std::shared_ptr<Logger>& info_log,
                     EventCallbackType event_callback,
                     AcceptCallbackType accept_callback,
                     StreamAllocator allocator,
                     EventLoop::Options options)
    : options_(std::move(options))
    , env_(env)
    , env_options_(env_options)
    , port_number_(port_number)
    , running_(false)
    , base_(nullptr)
    , info_log_(info_log)
    , event_callback_(std::move(event_callback))
    , accept_callback_(std::move(accept_callback))
    , listener_(nullptr)
    , shutdown_eventfd_(rocketspeed::port::Eventfd(true, true))
    , command_queues_(CommandQueueUnrefHandler)
    , stream_router_(allocator.Split())
    , outbound_allocator_(std::move(allocator))
    , active_connections_(0)
    , stats_(options_.stats_prefix)
    , queue_stats_(std::make_shared<QueueStats>(options_.stats_prefix +
                                                ".queues"))
    , default_command_queue_size_(options_.command_queue_size) {
  // Setup callbacks.
  command_callbacks_[CommandType::kAcceptCommand] = [this](
      std::unique_ptr<Command> command) {
    HandleAcceptCommand(std::move(command));
  };
  command_callbacks_[CommandType::kSendCommand] = [this](
      std::unique_ptr<Command> command) {
    HandleSendCommand(std::move(command));
  };
  command_callbacks_[CommandType::kExecuteCommand] = [](
      std::unique_ptr<Command> command) {
    static_cast<ExecuteCommand*>(command.get())->Execute();
  };

  heartbeat_enabled_ = options_.heartbeat_enabled;
  heartbeat_timeout_ = options_.heartbeat_timeout;
  heartbeat_expire_batch_ = options_.heartbeat_expire_batch;
  heartbeat_expired_callback_ =
    [this](StreamID global) {
      std::unique_ptr<Message> msg(
          new MessageGoodbye(Tenant::InvalidTenant,
                             MessageGoodbye::Code::HeartbeatTimeout,
                             MessageGoodbye::OriginType::Client));
      // handle the goodbye message by the server
      Dispatch(std::move(msg), global);
      // send the goodbye to the client, it should close the stream after the
      // t6778565 is completed
      // TODO(rpetrovic): update the comment after t6778565
      std::string serial;
      msg->SerializeToString(&serial);
      // note that we do not check the command queue size in HandleSendCommand
      // right now, as we would do in SendCommand
      HandleSendCommand(
        SerializedSendCommand::Response(std::move(serial), {global}));
    };

  LOG_INFO(info_log, "Created a new Event Loop at port %d", port_number);
}

EventLoop::Stats::Stats(const std::string& prefix) {
  write_latency = all.AddLatency(prefix + ".write_latency");
  write_size_bytes =
    all.AddHistogram(prefix + ".write_size_bytes", 0, kMaxIovecs, 1, 1.1);
  write_size_iovec =
    all.AddHistogram(prefix + ".write_size_iovec", 0, kMaxIovecs, 1, 1.1);
  write_succeed_bytes =
    all.AddHistogram(prefix + ".write_succeed_bytes", 0, kMaxIovecs, 1, 1.1);
  write_succeed_iovec =
    all.AddHistogram(prefix + ".write_succeed_iovec", 0, kMaxIovecs, 1, 1.1);
  commands_processed = all.AddCounter(prefix + ".commands_processed");
  accepts = all.AddCounter(prefix + ".accepts");
  queue_count = all.AddCounter(prefix + ".queue_count");
  full_queue_errors = all.AddCounter(prefix + ".full_queue_errors");
  socket_writes = all.AddCounter(prefix + ".socket_writes");
  partial_socket_writes = all.AddCounter(prefix + ".partial_socket_writes");
  for (int i = 0; i < int(MessageType::max) + 1; ++i) {
    messages_received[i] = all.AddCounter(
      prefix + ".messages_received." + MessageTypeName(MessageType(i)));
  }
}

Statistics EventLoop::GetStatistics() const {
  stats_.queue_count->Set(incoming_queues_.size());
  Statistics stats = stats_.all;
  stats.Aggregate(queue_stats_->all);
  return stats;
}

EventLoop::~EventLoop() {
  // Event loop should already be stopped by this point, and the running
  // thread should be joined.
  assert(!running_);
  shutdown_eventfd_.closefd();
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

size_t EventLoop::GetQueueSize() const {
  return const_cast<EventLoop*>(this)->GetThreadLocalQueue()->GetSize();
}

EventLoop::IncomingQueue::~IncomingQueue() {
}

EventCallback::EventCallback(EventLoop* event_loop, std::function<void()> cb)
: event_loop_(event_loop)
, cb_(std::move(cb))
, enabled_(false) {
}

std::unique_ptr<EventCallback> EventCallback::CreateFdReadCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb) {
  std::unique_ptr<EventCallback> callback(
    new EventCallback(event_loop, std::move(cb)));
  callback->event_ =
    event_loop->CreateFdReadEvent(fd, &EventShim, callback.get());
  return callback;
}

std::unique_ptr<EventCallback> EventCallback::CreateFdWriteCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb) {
  std::unique_ptr<EventCallback> callback(
    new EventCallback(event_loop, std::move(cb)));
  callback->event_ =
    event_loop->CreateFdWriteEvent(fd, &EventShim, callback.get());
  return callback;
}

EventCallback::~EventCallback() {
  if (event_) {
    event_free(event_);
  }
}

void EventCallback::Invoke() {
  event_loop_->ThreadCheck();
  cb_();
}

void EventCallback::Enable() {
  event_loop_->ThreadCheck();
  if (!enabled_) {
    if (event_add(event_, nullptr)) {
      exit(137);
    }
    enabled_ = true;
  }
}

void EventCallback::Disable() {
  event_loop_->ThreadCheck();
  if (enabled_) {
    if (event_del(event_)) {
      exit(137);
    }
    enabled_ = false;
  }
}

}  // namespace rocketspeed
