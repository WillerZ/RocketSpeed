//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "socket_event.h"

#include <sys/uio.h>
#include <memory>

#include "include/Logger.h"
#include "src/port/port.h"
#include "src/messages/queues.h"
#include "src/messages/serializer.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/coding.h"
#include "src/util/common/flow_control.h"
#include "src/util/common/host_id.h"

namespace rocketspeed {

namespace {

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

  static_assert(encoding_size == kMessageHeaderEncodedSize,
                "Message header size mismatch.");
};

}  // namespace

std::unique_ptr<SocketEvent> SocketEvent::Create(EventLoop* event_loop,
                                                 int fd,
                                                 HostId destination) {
  std::unique_ptr<SocketEvent> sev(
      new SocketEvent(event_loop, fd, !!destination));

  if (!sev->read_ev_ || !sev->write_ev_) {
    LOG_ERROR(
        event_loop->GetLog(), "Failed to create socket event for fd(%d)", fd);
    return nullptr;
  }

  sev->destination_ = std::move(destination);
  return sev;
}

void SocketEvent::Disconnect(SocketEvent* sev, bool timed_out) {
  // Inform MsgLoop that clients have disconnected.
  auto origin_type = sev->was_initiated_ ? MessageGoodbye::OriginType::Server
                                         : MessageGoodbye::OriginType::Client;

  // Remove and close streams that were assigned to this connection.
  EventLoop* event_loop = sev->event_loop_;  // make a copy, sev is destroyed.
  auto globals = event_loop->stream_router_.RemoveConnection(sev);
  // Delete the socket event.
  event_loop->teardown_connection(sev, timed_out);
  for (StreamID global : globals) {
    // We send goodbye using the global stream IDs, as these are only
    // known by the entity using the loop.
    std::unique_ptr<Message> msg(new MessageGoodbye(
        Tenant::InvalidTenant, MessageGoodbye::Code::SocketError, origin_type));
    SourcelessFlow no_flow;
    event_loop->Dispatch(&no_flow, std::move(msg), global);
  }
}

SocketEvent::~SocketEvent() {
  thread_check_.Check();

  LOG_INFO(event_loop_->GetLog(), "Closing fd(%d)", fd_);
  event_loop_->GetLog()->Flush();
  read_ev_.reset();
  write_ev_.reset();
  close(fd_);
}

Status SocketEvent::Enqueue(StreamID local,
                            std::shared_ptr<TimestampedString> message_ser) {
  thread_check_.Check();

  auto now = event_loop_->env_->NowMicros();

  // Serialise stream metadata.
  auto stream_ser = std::make_shared<TimestampedString>();
  EncodeOrigin(&stream_ser->string, local);
  stream_ser->issued_time = now;

  // Serialise message header.
  size_t frame_size = stream_ser->string.size() + message_ser->string.size();
  MessageHeader header{ROCKETSPEED_CURRENT_MSG_VERSION,
                       static_cast<uint32_t>(frame_size)};
  auto header_ser = std::make_shared<TimestampedString>();
  header_ser->string = header.ToString();
  header_ser->issued_time = now;

  // Add chunks carrying the message header, destinations, and serialised
  // message to the send queue.
  send_queue_.emplace_back(std::move(header_ser));
  send_queue_.emplace_back(std::move(stream_ser));
  send_queue_.emplace_back(std::move(message_ser));

  // Enable write event, as we have stuff to write.
  if (!write_ev_added_) {
    write_ev_->Enable();
    write_ev_added_ = true;
  }

  return Status::OK();
}

SocketEvent::SocketEvent(EventLoop* event_loop, int fd, bool initiated)
: hdr_idx_(0)
, msg_idx_(0)
, msg_size_(0)
, fd_(fd)
, event_loop_(event_loop)
, write_ev_added_(false)
, was_initiated_(initiated)
, timeout_cancelled_(false) {
  thread_check_.Check();

  // Create read and write events
  read_ev_ = EventCallback::CreateFdReadCallback(event_loop,
                                                 fd,
                                                 [this]() {
                                                   if (!ReadCallback().ok()) {
                                                     Disconnect(this, false);
                                                   } else {
                                                     ProcessHeartbeats();
                                                   }
                                                 });

  write_ev_ =
      EventCallback::CreateFdWriteCallback(event_loop,
                                           fd,
                                           [this]() {
                                             if (!WriteCallback().ok()) {
                                               Disconnect(this, false);
                                             } else {
                                               ProcessHeartbeats();
                                             }
                                           });
}

Status SocketEvent::WriteCallback() {
  thread_check_.Check();

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
        LOG_WARN(
            event_loop_->info_log_,
            "Wanted to write %zu bytes to remote host fd(%d) but encountered "
            "errno(%d) \"%s\".",
            total,
            fd_,
            e,
            strerror(e));
        event_loop_->stats_.write_succeed_bytes->Record(0);
        event_loop_->stats_.write_succeed_iovec->Record(0);
        event_loop_->info_log_->Flush();
        if (e != EAGAIN && e != EWOULDBLOCK) {
          // write error, close connection.
          return Status::IOError("write call failed: " + std::to_string(e));
        }
        return Status::OK();
      }
      event_loop_->stats_.write_succeed_bytes->Record(count);
      if (static_cast<size_t>(count) != total) {
        event_loop_->stats_.partial_socket_writes->Add(1);
        LOG_WARN(event_loop_->info_log_,
                 "Wanted to write %zu bytes to remote host fd(%d) but only "
                 "%zd bytes written successfully.",
                 total,
                 fd_,
                 count);
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

Status SocketEvent::ReadCallback() {
  thread_check_.Check();

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
          return Status::IOError("read call failed: " + std::to_string(errno));
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
      LOG_WARN(
          event_loop_->GetLog(), "Failed to remap stream ID (%llu)", local);
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

    if (!DrainOne({global, std::move(msg)})) {
      return Status::OK();
    }
  }
  return Status::OK();
}

}  // namespace rocketspeed
