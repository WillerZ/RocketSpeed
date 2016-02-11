//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/messages/socket_event.h"

#include <sys/uio.h>
#include <memory>

#include "include/Logger.h"
#include "src/port/port.h"
#include "src/messages/queues.h"
#include "src/messages/serializer.h"
#include "src/messages/stream.h"
#include "src/messages/types.h"
#include "src/util/common/coding.h"
#include "src/util/common/flow_control.h"
#include "include/HostId.h"
#include "src/util/memory.h"

namespace rocketspeed {

static constexpr uint8_t kCurrentMsgVersion = 1;

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

SocketEventStats::SocketEventStats(const std::string& prefix) {
  write_latency = all.AddLatency(prefix + ".write_latency");
  write_size_bytes =
      all.AddHistogram(prefix + ".write_size_bytes", 0, kMaxIovecs, 1, 1.1f);
  write_size_iovec =
      all.AddHistogram(prefix + ".write_size_iovec", 0, kMaxIovecs, 1, 1.1f);
  write_succeed_bytes =
      all.AddHistogram(prefix + ".write_succeed_bytes", 0, kMaxIovecs, 1, 1.1f);
  write_succeed_iovec =
      all.AddHistogram(prefix + ".write_succeed_iovec", 0, kMaxIovecs, 1, 1.1f);
  socket_writes = all.AddCounter(prefix + ".socket_writes");
  partial_socket_writes = all.AddCounter(prefix + ".partial_socket_writes");
  for (int i = 0; i < int(MessageType::max) + 1; ++i) {
    messages_received[i] = all.AddCounter(prefix + ".messages_received." +
                                          MessageTypeName(MessageType(i)));
  }
}

std::unique_ptr<SocketEvent> SocketEvent::Create(EventLoop* event_loop,
                                                 const int fd,
                                                 HostId destination) {
  std::unique_ptr<SocketEvent> sev(
      new SocketEvent(event_loop, fd, std::move(destination)));

  if (!sev->read_ev_ || !sev->write_ev_) {
    LOG_ERROR(
        event_loop->GetLog(), "Failed to create SocketEvent for fd(%d)", fd);
    // File descriptior is owned by the SocketEvent at this point, noe need to
    // destroy.
    return nullptr;
  }
  LOG_INFO(event_loop->GetLog(),
           "Created SocketEvent(%d, %s)",
           fd,
           sev->GetDestination().ToString().c_str());
  return sev;
}

void SocketEvent::Close(ClosureReason reason) {
  thread_check_.Check();

  // Abort if closing or already closed.
  if (closing_) {
    return;
  }
  closing_ = true;

  LOG_INFO(GetLogger(),
           "Closing SocketEvent(%d, %s), reason: %d",
           fd_,
           destination_.ToString().c_str(),
           static_cast<int>(reason));

  // Unregister this socket from flow control.
  flow_control_.UnregisterSource(this);
  flow_control_.UnregisterSink(this);

  // Disable read and write events.
  read_ev_->Disable();
  write_ev_->Disable();

  // Unregister from the EventLoop.
  // This will perform a deferred destruction of the socket.
  event_loop_->CloseFromSocketEvent(access::EventLoop(), this);

  // Close all streams one by one.
  // Once the last stream gets unregistered, an attempt to recurse into this
  // method will be made. Since we've marked the socket as closing, that won't
  // do any harm.
  while (!remote_id_to_stream_.empty()) {
    Stream* stream = remote_id_to_stream_.begin()->second;
    // Unregister the stream.
    UnregisterStream(stream->GetRemoteID(), true);

    if (reason == ClosureReason::Graceful) {
      // Close the socket silently if shutting down connection gracefully.
      stream->CloseFromSocketEvent(access::Stream());
    } else {
      // Otherwise prepare and deliver a goodbye message as if it originated
      // from the remote host.
      std::unique_ptr<Message> goodbye(
          new MessageGoodbye(Tenant::GuestTenant,
                             MessageGoodbye::Code::SocketError,
                             IsInbound() ? MessageGoodbye::OriginType::Client
                                         : MessageGoodbye::OriginType::Server));
      // We can afford not to throttle goodbye messages, as for every message
      // received we remove one entry on socket's internal structures, so
      // overrall
      // memory utilisation does not grow significantly (if at all).
      SourcelessFlow no_flow(event_loop_->GetFlowControl());
      stream->Receive(access::Stream(), &no_flow, std::move(goodbye));
    }
  }
}

SocketEvent::~SocketEvent() {
  thread_check_.Check();
  RS_ASSERT(remote_id_to_stream_.empty());
  RS_ASSERT(owned_streams_.empty());

  LOG_INFO(GetLogger(),
           "Destroying SocketEvent(%d, %s)",
           fd_,
           destination_.ToString().c_str());

  read_ev_.reset();
  write_ev_.reset();
  close(fd_);
}

std::unique_ptr<Stream> SocketEvent::OpenStream(StreamID stream_id) {
  RS_ASSERT(!closing_);
  thread_check_.Check();

  std::unique_ptr<Stream> stream(new Stream(this, stream_id, stream_id));
  auto result = remote_id_to_stream_.emplace(stream_id, stream.get());
  RS_ASSERT(result.second);
  (void)result;
  // TODO(t8971722)
  stream->SetReceiver(event_loop_->GetDefaultReceiver());
  return stream;
}

void SocketEvent::RegisterReadEvent(EventLoop* event_loop) {
  RS_ASSERT(event_loop_ == event_loop);
  thread_check_.Check();

  // We create the event while constructing the socket, as we cannot propagate
  // or handle errors in this method.
}

void SocketEvent::SetReadEnabled(EventLoop* event_loop, bool enabled) {
  RS_ASSERT(event_loop_ == event_loop);
  thread_check_.Check();

  if (enabled) {
    read_ev_->Enable();
  } else {
    read_ev_->Disable();
  }
}

bool SocketEvent::Write(SerializedOnStream& value) {
  thread_check_.Check();

  LOG_DEBUG(GetLogger(),
            "Writing %zd bytes to SocketEvent(%d, %s)",
            value.serialised->string.size(),
            fd_,
            destination_.ToString().c_str());

  // Sneak-peak message type, we will handle MessageGoodbye differently.
  auto type = Message::ReadMessageType(value.serialised->string);
  RS_ASSERT(type != MessageType::NotInitialized);

  auto now = event_loop_->GetEnv()->NowMicros();
  // Serialise stream metadata.
  auto stream_ser = std::make_shared<TimestampedString>();
  const auto remote_id = value.stream_id;
  EncodeOrigin(&stream_ser->string, remote_id);
  stream_ser->issued_time = now;
  // Serialise message header.
  size_t frame_size =
      stream_ser->string.size() + value.serialised->string.size();
  MessageHeader header{kCurrentMsgVersion, static_cast<uint32_t>(frame_size)};
  auto header_ser = std::make_shared<TimestampedString>();
  header_ser->string = header.ToString();
  header_ser->issued_time = now;

  // Add chunks carrying the message header, destinations, and serialised
  // message to the send queue.
  send_queue_.emplace_back(std::move(header_ser));
  send_queue_.emplace_back(std::move(stream_ser));
  send_queue_.emplace_back(std::move(value.serialised));
  // Signal overflow if size limit was matched or exceeded.
  const bool has_room =
      send_queue_.size() < event_loop_->GetOptions().send_queue_limit;
  if (!has_room) {
    event_loop_->Unnotify(write_ready_);
  }

  // Enable write event, as we have stuff to write.
  write_ev_->Enable();

  if (type == MessageType::mGoodbye) {
    // If it was a goodbye the stream will be closed once this call returns.
    // We need to unregister it from the loop.
    UnregisterStream(remote_id);
  }
  return has_room;
}

bool SocketEvent::FlushPending() {
  thread_check_.Check();
  return true;
}

std::unique_ptr<EventCallback> SocketEvent::CreateWriteCallback(
    EventLoop* event_loop, std::function<void()> callback) {
  RS_ASSERT(event_loop_ == event_loop);
  thread_check_.Check();
  return event_loop_->CreateEventCallback(std::move(callback), write_ready_);
}

const std::shared_ptr<Logger>& SocketEvent::GetLogger() const {
  return event_loop_->GetLog();
}

SocketEvent::SocketEvent(EventLoop* event_loop, int fd, HostId destination)
: stats_(event_loop->GetSocketStats())
, hdr_idx_(0)
, msg_idx_(0)
, msg_size_(0)
, fd_(fd)
, write_ready_(event_loop->CreateEventTrigger())
, flow_control_("socket_event.flow_control", event_loop)
, event_loop_(event_loop)
, timeout_cancelled_(false)
, destination_(std::move(destination)) {
  thread_check_.Check();

  // Create read and write events
  read_ev_ =
      EventCallback::CreateFdReadCallback(event_loop,
                                          fd,
                                          [this]() {
                                            if (!ReadCallback().ok()) {
                                              Close(ClosureReason::Error);
                                            }
                                          });

  write_ev_ =
      EventCallback::CreateFdWriteCallback(event_loop,
                                           fd,
                                           [this]() {
                                             if (!WriteCallback().ok()) {
                                               Close(ClosureReason::Error);
                                             }
                                           });

  // Register the socket with flow control.
  flow_control_.Register<MessageOnStream>(
      this,
      [this](Flow* flow, MessageOnStream message) {
        message.stream->Receive(
            access::Stream(), flow, std::move(message.message));
      });

  // Socket's send_queue is empty, so the sink is writable.
  event_loop_->Notify(write_ready_);
}

void SocketEvent::UnregisterStream(StreamID remote_id, bool force) {
  Stream* stream;
  {  // Remove the stream from the routing data structures, so that all incoming
    // messages on it will be dropped.
    auto it = remote_id_to_stream_.find(remote_id);
    if (it == remote_id_to_stream_.end()) {
      return;
    }
    stream = it->second;
    remote_id_to_stream_.erase(it);
  }

  LOG_INFO(GetLogger(),
           "Unregistering Stream(%llu, %llu)",
           stream->GetLocalID(),
           stream->GetRemoteID());

  // TODO(t8971722)
  event_loop_->CloseFromSocketEvent(access::EventLoop(), stream);

  // Defer destruction of the stream object.
  auto it = owned_streams_.find(stream);
  if (it != owned_streams_.end()) {
    auto owned_stream = std::move(it->second);
    owned_streams_.erase(it);
    RS_ASSERT(owned_stream.get() == stream);
    event_loop_->AddTask(MakeDeferredDeleter(owned_stream));
  }

  if (remote_id_to_stream_.empty()) {
    // We've closed the last stream on this connection
    if (force ||
        (!IsInbound() &&
         event_loop_->GetOptions().
         connection_without_streams_keepalive.count() == 0)) {
      Close(ClosureReason::Graceful);
    } else {
      // Keep track of how long it will remain without any associated streams
      // so as to close it once the the keepalive timeout expires.
      without_streams_since_ = std::chrono::steady_clock::now();
    }
  }
}

bool SocketEvent::IsWithoutStreamsForLongerThan(
  std::chrono::milliseconds mil) const {
  thread_check_.Check();
  if (!remote_id_to_stream_.empty() || closing_) {
    return false;
  }
  auto now = std::chrono::steady_clock::now();
  RS_ASSERT(now > without_streams_since_);
  std::chrono::nanoseconds diff = now - without_streams_since_;
  return diff > mil;
}

Status SocketEvent::WriteCallback() {
  thread_check_.Check();

  if (!timeout_cancelled_) {
    // This socket is now writable, so we can cancel the connect timeout.
    event_loop_->MarkConnected(access::EventLoop(), this);
    timeout_cancelled_ = true;
  }

  RS_ASSERT(send_queue_.size() > 0);

  // Sanity check stats.
  // write_succeed_* should have a record for all write_size_*
  RS_ASSERT(stats_->write_size_bytes->GetNumSamples() ==
         stats_->write_succeed_bytes->GetNumSamples());
  RS_ASSERT(stats_->write_size_iovec->GetNumSamples() ==
         stats_->write_succeed_iovec->GetNumSamples());

  while (send_queue_.size() > 0) {
    // if there is any pending data from the previously sent
    // partial-message, then send it.
    if (partial_.size() > 0) {
      RS_ASSERT(send_queue_.size() > 0);

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

      stats_->write_size_bytes->Record(total);
      stats_->write_size_iovec->Record(iovcnt);
      stats_->socket_writes->Add(1);
      ssize_t count = writev(fd_, iov, iovcnt);
      if (count == -1) {
        auto e = errno;
        LOG_WARN(
            GetLogger(),
            "Wanted to write %zu bytes to remote host fd(%d) but encountered "
            "errno(%d) \"%s\".",
            total,
            fd_,
            e,
            strerror(e));
        stats_->write_succeed_bytes->Record(0);
        stats_->write_succeed_iovec->Record(0);
        if (e != EAGAIN && e != EWOULDBLOCK) {
          // write error, close connection.
          return Status::IOError("write call failed: " + std::to_string(e));
        }
        return Status::OK();
      }
      stats_->write_succeed_bytes->Record(count);
      if (static_cast<size_t>(count) != total) {
        stats_->partial_socket_writes->Add(1);
        LOG_WARN(GetLogger(),
                 "Wanted to write %zu bytes to remote host fd(%d) but only "
                 "%zd bytes written successfully.",
                 total,
                 fd_,
                 count);
      }

      size_t written = static_cast<size_t>(count);
      for (int i = 0; i < iovcnt; ++i) {
        RS_ASSERT(!send_queue_.empty());
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
          stats_->write_succeed_iovec->Record(i);
          return Status::OK();
        }
        stats_->write_latency->Record(event_loop_->GetEnv()->NowMicros() -
                                      item->issued_time);
        send_queue_.pop_front();

        // We've taken one element from the send queue, now check whether we can
        // enable the sink.
        if (send_queue_.size() ==
            event_loop_->GetOptions().send_queue_limit / 2) {
          event_loop_->Notify(write_ready_);
        }
      }
      stats_->write_succeed_iovec->Record(iovcnt);
      RS_ASSERT(written == 0);
      partial_.clear();

      LOG_DEBUG(GetLogger(),
                "Successfully wrote %zd bytes to remote host fd(%d)",
                count,
                fd_);
    }

    // No more partial data to be sent out.
    if (send_queue_.size() > 0) {
      // If there are any new pending messages, start processing it.
      partial_ = send_queue_.front()->string;
      RS_ASSERT(partial_.size() > 0);
    } else {
      // No more queued messages. Switch off ready-to-write event on socket.
      write_ev_->Disable();
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
    RS_ASSERT(msg_idx_ < msg_size_);

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
    StreamID remote_id = 0;
    if (!DecodeOrigin(&in, &remote_id)) {
      continue;
    }

    // Decode the rest of the message.
    std::unique_ptr<Message> msg =
        Message::CreateNewInstance(std::move(msg_buf_), in);
    if (!msg) {
      LOG_WARN(GetLogger(), "Failed to decode message");
      continue;
    }

    if (!Receive(remote_id, std::move(msg))) {
      // We should not read more in the same batch.
      break;
    }
  }
  return Status::OK();
}

bool SocketEvent::Receive(StreamID remote_id, std::unique_ptr<Message> msg) {
  const auto msg_type = msg->GetMessageType();
  RS_ASSERT(ValidateEnum(msg_type));

  // Find a stream for this message or create one if missing.
  auto it = remote_id_to_stream_.find(remote_id);
  if (it == remote_id_to_stream_.end()) {
    // Allow accepting new streams on inbound connections only.
    // The special case is MessageGoodbye, for which we do not create a stream
    // if it doesn't exist.
    if (IsInbound() && msg_type != MessageType::mGoodbye) {
      std::unique_ptr<Stream> owned_stream(new Stream(
          this,
          remote_id,
          event_loop_->GetInboundAllocator(access::EventLoop())->Next()));
      // Set the default receiver provided by EventLoop for inbound streams.
      owned_stream->SetReceiver(event_loop_->GetDefaultReceiver());
      // TODO(t8971722)
      event_loop_->AddInboundStream(access::EventLoop(), owned_stream.get());
      // Register a new inbound stream.
      auto result = remote_id_to_stream_.emplace(remote_id, owned_stream.get());
      RS_ASSERT(result.second);
      it = result.first;
      // Make the SocketEvent own it.
      auto result1 =
          owned_streams_.emplace(owned_stream.get(), std::move(owned_stream));
      RS_ASSERT(result1.second);
      (void)result1;
    } else {
      // Drop the message.
      LOG_WARN(GetLogger(),
               "Failed to remap StreamID(%llu), dropping message: %s",
               remote_id,
               MessageTypeName(msg_type));
      return true;
    }
  }
  RS_ASSERT(it != remote_id_to_stream_.end());
  Stream* stream = it->second;

  // Update stats.
  stats_->messages_received[size_t(msg_type)]->Add(1);

  // Unregister the stream if we've received a goodbye message.
  if (msg_type == MessageType::mGoodbye) {
    UnregisterStream(remote_id);
  }

  // We shouldn't process any more in this batch if we hit overflow.
  return DrainOne({stream, std::move(msg)});
}

}  // namespace rocketspeed
