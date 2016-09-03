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

#include <algorithm>
#include <deque>
#include <functional>
#include <iterator>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include "src/messages/event2_version.h"
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include "external/folly/move_wrapper.h"

#include "src/messages/serializer.h"
#include "src/messages/socket_event.h"
#include "src/messages/stream.h"
#include "src/messages/stream_socket.h"
#include "src/messages/timed_callback.h"
#include "src/messages/triggerable_callback.h"
#include "src/messages/unbounded_mpsc_queue.h"
#include "src/port/port.h"
#include "src/util/common/autovector.h"
#include "src/util/common/client_env.h"
#include "src/util/common/coding.h"
#include "src/util/common/observable_set.h"
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/common/select_random.h"

#ifdef OS_LINUX
#include <netinet/tcp.h>
#endif

static_assert(std::is_same<evutil_socket_t, int>::value,
  "EventLoop assumes evutil_socket_t is int.");

namespace rocketspeed {

const int EventLoop::kLogSeverityDebug = _EVENT_LOG_DEBUG;
const int EventLoop::kLogSeverityMsg = _EVENT_LOG_MSG;
const int EventLoop::kLogSeverityWarn = _EVENT_LOG_WARN;
const int EventLoop::kLogSeverityErr = _EVENT_LOG_ERR;

class AcceptCommand : public Command {
 public:
  explicit AcceptCommand(int fd)
      : fd_(fd) {}
  virtual ~AcceptCommand() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  CommandType GetCommandType() const { return kAcceptCommand; }

  int DetachFD() {
    int fd = fd_;
    fd_ = -1;
    return fd;
  }

 private:
  int fd_;
};

// TODO(t8971722)
void EventLoop::HandleSendCommand(Flow* flow,
                                  std::unique_ptr<Command> command) {
  // Need using otherwise SendCommand is confused with the member function.
  using rocketspeed::SendCommand;
  SendCommand* send_cmd = static_cast<SendCommand*>(command.get());

  auto now = env_->NowMicros();
  const auto msg = std::make_shared<TimestampedString>();
  send_cmd->GetMessage(&msg->string);
  msg->issued_time = now;
  RS_ASSERT(!msg->string.empty());

  for (const SendCommand::StreamSpec& spec : send_cmd->GetDestinations()) {
    // Find or create a stream.
    auto it = stream_id_to_stream_.find(spec.stream);
    if (it == stream_id_to_stream_.end()) {
      if (!spec.destination) {
        LOG_WARN(info_log_,
                 "Failed to send on closed stream (%llu), no destination",
                 spec.stream);
        continue;
      }
      // Create a new stream.
      auto new_stream = OpenStream(spec.destination, spec.stream);
      if (!new_stream) {
        LOG_WARN(info_log_,
                 "Failed to create stream for sending to: %s",
                 spec.destination.ToString().c_str());
        continue;
      }
      auto result = stream_id_to_stream_.emplace(new_stream->GetLocalID(),
                                                 new_stream.get());
      RS_ASSERT(result.second);
      it = result.first;
      auto result1 =
          owned_streams_.emplace(new_stream.get(), std::move(new_stream));
      (void)result1;
      RS_ASSERT(result1.second);
    }

    auto message = msg;
    flow->Write(it->second, message);
  }
}

void EventLoop::HandleAcceptCommand(std::unique_ptr<Command> command) {
  thread_check_.Check();

  AcceptCommand* accept_cmd = static_cast<AcceptCommand*>(command.get());
  // Create SocketEvent and pass ownership to the loop.
  int fd = accept_cmd->DetachFD();
  auto owned_socket = SocketEvent::Create(this, fd, options_.protocol_version);
  const auto socket = owned_socket.get();
  if (!socket) {
    LOG_ERROR(info_log_,
              "Failed to create SocketEvent for accepted connection fd(%d)",
              fd);
    // Close the socket.
    close(fd);
  }
  owned_connections_.emplace(socket, std::move(owned_socket));

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
  obj->start_signal_.Post();
}

void
EventLoop::do_timerevent(evutil_socket_t listener, short event, void *arg) {
  TimedCallback* obj = static_cast<TimedCallback*>(arg);
  obj->Invoke();
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
  if (event_loop->accept_callback_) {
    event_loop->accept_callback_(fd);
  } else {
    event_loop->Accept(fd);
  }
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

  setup_keepalive(fd, event_loop->env_options_);
  return status;
}

void EventLoop::setup_keepalive(int sockfd, const EnvOptions& env_options) {
  if (env_options.use_tcp_keep_alive) {
    int keepalive = 1;
    socklen_t sizeof_int = static_cast<socklen_t>(sizeof(int));
    setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof_int);

#ifdef OS_LINUX
    setsockopt(sockfd, SOL_TCP, TCP_KEEPIDLE,
        &env_options.keepalive_time, sizeof_int);

    setsockopt(sockfd, SOL_TCP, TCP_KEEPINTVL,
        &env_options.keepalive_intvl, sizeof_int);

    setsockopt(sockfd, SOL_TCP, TCP_KEEPCNT,
        &env_options.keepalive_probes, sizeof_int);
#endif
  }
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
  obj->shutting_down_ = true;
  event_base_loopbreak(base);
}

Status
EventLoop::Initialize() {
  if (base_) {
    RS_ASSERT(false);
    return Status::InvalidArgument("EventLoop already initialized.");
  }

  base_ = event_base_new();
  if (!base_) {
    return Status::InternalError(
      "Failed to create an event base for an EventLoop thread");
  }

  // Port == 0 indicates that the actual port should be auto-allocated,
  // while port < 0 -- that there is no accept loop.
  if (port_number_ >= 0) {
    sockaddr_in6 sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin6_family = AF_INET6;
    sin.sin6_addr = in6addr_any;
    sin.sin6_port = htons(static_cast<uint16_t>(port_number_));
    auto sin_len = static_cast<socklen_t>(sizeof(sin));

    // Create libevent connection listener.
    listener_ =
        evconnlistener_new_bind(base_,
                                &EventLoop::do_accept,
                                reinterpret_cast<void*>(this),
                                LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE,
                                -1,  // backlog
                                reinterpret_cast<sockaddr*>(&sin),
                                static_cast<int>(sin_len));

    if (listener_ == nullptr) {
      return Status::InternalError(
        "Failed to create connection listener on port " +
          std::to_string(port_number_));
    }

    if (port_number_ == 0) {
      // Grab the actual port number, if auto allocated.
      if (getsockname(evconnlistener_get_fd(listener_),
                      reinterpret_cast<sockaddr*>(&sin),
                      &sin_len)) {
        return Status::InternalError("Failed to obtain listener port");
      }
      port_number_ = ntohs(sin.sin6_port);
      LOG_INFO(info_log_, "EventLoop bound to port %d", port_number_);
    }

    // Setup host ID.
    host_id_ = HostId::CreateLocal(static_cast<uint16_t>(port_number_));

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

  // An event that signals there is a pending notification from some trigger.
  if (notified_triggers_fd_.status() < 0) {
    return Status::InternalError(
        "Failed to create eventfd for pending notifications");
  }

  notified_triggers_event_ = EventCallback::CreateFdReadCallback(
      this,
      notified_triggers_fd_.readfd(),
      std::bind(&EventLoop::HandlePendingTriggers, this));
  if (notified_triggers_event_ == nullptr) {
    return Status::InternalError(
        "Failed to create pending notifications event");
  }
  notified_triggers_event_->Enable();

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
  shutdown_event_ = EventCallback::CreateFdReadCallback(
      this,
      shutdown_eventfd_.readfd(),
      [this]() {
        LOG_VITAL(info_log_, "Stopping EventLoop at port %d", port_number_);
        shutting_down_ = true;
        event_base_loopbreak(base_);
      });
  if (shutdown_event_ == nullptr) {
    return Status::InternalError("Failed to create shutdown event");
  }
  shutdown_event_->Enable();

  control_command_queue_ =
    std::make_shared<UnboundedMPSCCommandQueue>(info_log_, queue_stats_);
  AddControlCommandQueue(control_command_queue_);

  if (options_.enable_heartbeats) {
    auto send_heartbeats = [this]() {
      for (const auto& kv : stream_id_to_stream_) {
        heartbeats_to_send_->Add(kv.first);
      }
    };

    InstallSource<StreamID>(
      this,
      heartbeats_to_send_.get(),
      [this](Flow* flow, StreamID stream_id) {
        auto* stream = GetInboundStream(stream_id);
        if (!stream) {
          return;               // stream closed since add
        }
        MessageHeartbeat hb(SystemTenant);
        auto ts = Stream::ToTimestampedString(hb);
        flow->Write(stream, ts);
        stats_.hbs_sent->Add(1);
      });

    hb_timer_ = RegisterTimerCallback(send_heartbeats,
                                      options_.heartbeat_period);
    RS_ASSERT(hb_timer_);
  }

  return Status::OK();
}

void EventLoop::Run() {
  thread_check_.Reset();

  if (!base_) {
    LOG_FATAL(info_log_, "EventLoop not initialized before use.");
    RS_ASSERT(false);
    return;
  }
  LOG_VITAL(info_log_, "Starting EventLoop at port %d", port_number_);
  info_log_->Flush();

  // Register a timer for checking expired connections.
  std::unique_ptr<EventCallback> expired_connections_timer =
    RegisterTimerCallback([this]() {
      // The timeout list might be modified during the procedure
      // that closes the socket.
      std::vector<SocketEvent*> expired;
      connect_timeout_.GetExpired(options_.connect_timeout,
                                  std::back_inserter(expired));
      for (auto socket : expired) {
        socket->Close(SocketEvent::ClosureReason::Error);
      }
    }, options_.connect_timeout);


  std::unique_ptr<EventCallback> connection_gc_timer;
  if (options_.connection_without_streams_keepalive.count() > 0) {
    // Garbage collect periodically connections without associated streams
    connection_gc_timer = RegisterTimerCallback([this]() {
        // We can't loop through the original outbound_connections_ object
        // because SocketEvent::Close() removes the given SocketEvent from
        // outbound_connections_ and would invalidate our iterator.
        std::vector<SocketEvent*> outbound_connections_copy;
        outbound_connections_copy.reserve(outbound_connections_.size());
        for (auto entry: outbound_connections_) {
          outbound_connections_copy.push_back(entry.second);
        }
        for (auto s_ptr: outbound_connections_copy) {
          if (s_ptr->IsWithoutStreamsForLongerThan(
                options_.connection_without_streams_keepalive)) {
            s_ptr->Close(SocketEvent::ClosureReason::Graceful);
          }
        }
      },
      std::max(options_.connection_without_streams_keepalive / 10,
               std::chrono::milliseconds(100)));
    if (connection_gc_timer == nullptr) {
      LOG_FATAL(info_log_, "Error creating timed event.");
      RS_ASSERT(false);
    }
  }

  // Start the event loop.
  // This will not exit until Stop is called, or some error
  // happens within libevent.
  while (!RunOnce()) {
    // Once more.
  }

  // Shutdown everything
  if (listener_) {
    evconnlistener_free(listener_);
  }
  if (startup_event_) {
    event_free(startup_event_);
  }

  fd_read_events_.clear();
  incoming_queues_.clear();
  notified_triggers_event_.reset();
  shutdown_event_.reset();
  CloseAllSocketEvents();
  flow_control_.reset();
  heartbeats_to_send_.reset();
  expired_connections_timer.reset();

  if (!internal_status_.ok()) {
    LOG_ERROR(info_log_,
      "EventLoop loop stopped with error: %s",
      internal_status_.ToString().c_str());
  }

  LOG_VITAL(info_log_, "Stopped EventLoop at port %d", port_number_);
  info_log_->Flush();

  running_ = false;
}

bool EventLoop::RunOnce() {
  // Handle libevent events.
  bool stopped = event_base_loop(base_, EVLOOP_ONCE);
  stopped |= shutting_down_;
  // Execute some scheduled tasks.
  ExecuteTasks();
  return stopped;
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

void EventLoop::AddTask(std::function<void()> task) {
  thread_check_.Check();
  RS_ASSERT(task);
  scheduled_tasks_.emplace_back(std::move(task));
}

bool EventLoop::ExecuteTasks() {
  while (!scheduled_tasks_.empty()) {
    auto task = std::move(scheduled_tasks_.front());
    scheduled_tasks_.pop_front();
    task();
  }
  return true;
}

void EventLoop::HandlePendingTriggers() {
  thread_check_.Check();

  // Clear the notification, we will add it back if there is work to be done.
  eventfd_t value;
  notified_triggers_fd_.read_event(&value);

  auto& prng = ThreadLocalPRNG();
  size_t attempts = 32;
  while (!notified_enabled_callbacks_.empty() && attempts-- > 0) {
    TriggerableCallback* cb = SelectRandom(&notified_enabled_callbacks_, &prng);
    RS_ASSERT(cb->IsEnabled());
    RS_ASSERT(notified_triggers_.count(cb->GetTriggerID()));

    if (cb->IsEnabled() && notified_triggers_.count(cb->GetTriggerID())) {
      // Invoke the callback.
      cb->Invoke();
    }
  }

  if (!notified_enabled_callbacks_.empty()) {
    // More work to be done.
    notified_triggers_fd_.write_event(1);
  }
}

std::unique_ptr<EventCallback> EventLoop::RegisterTimerCallback(
  TimerCallbackType callback, std::chrono::microseconds period, bool enabled) {
  RS_ASSERT(base_);

  auto timed_event = std::unique_ptr<TimedCallback>(
    new TimedCallback(this, std::move(callback), period));

  auto loop_event = event_new(
    base_,
    -1,
    EV_PERSIST,
    EventLoop::do_timerevent,
    reinterpret_cast<void*>(timed_event.get()));

  if (loop_event == nullptr) {
    LOG_ERROR(info_log_, "Failed to create timer event");
    info_log_->Flush();
    return nullptr;
  }

  timed_event->AddEvent(loop_event, enabled);

  return std::move(timed_event);
}

std::unique_ptr<EventCallback> EventLoop::CreateTimedEventCallback(
    std::function<void()> cb, std::chrono::microseconds duration) {
  thread_check_.Check();

  return RegisterTimerCallback(std::move(cb), duration, false);
}

EventTrigger EventLoop::CreateEventTrigger() {
  return EventTrigger(next_trigger_id_.fetch_add(1));
}

void EventLoop::Notify(const EventTrigger& trigger) {
  thread_check_.Check();

  notified_triggers_.emplace(trigger.id_);
  auto it = trigger_to_enabled_callbacks_.find(trigger.id_);
  if (it == trigger_to_enabled_callbacks_.end()) {
    return;
  }
  for (auto* event : it->second) {
    notified_enabled_callbacks_.emplace(event);
  }
  notified_triggers_fd_.write_event(1);
}

void EventLoop::Unnotify(const EventTrigger& trigger) {
  thread_check_.Check();

  notified_triggers_.erase(trigger.id_);
  auto it = trigger_to_enabled_callbacks_.find(trigger.id_);
  if (it == trigger_to_enabled_callbacks_.end()) {
    return;
  }
  for (auto* event : it->second) {
    notified_enabled_callbacks_.erase(event);
  }
}

std::unique_ptr<EventCallback> EventLoop::CreateEventCallback(
    std::function<void()> cb, const EventTrigger& trigger) {
  thread_check_.Check();

  auto event = new TriggerableCallback(this, trigger.id_, std::move(cb));
  return std::unique_ptr<EventCallback>(event);
}

void EventLoop::TriggerableCallbackEnable(access::EventLoop,
                                          TriggerableCallback* event) {
  thread_check_.Check();

  auto trigger = event->GetTriggerID();
  trigger_to_enabled_callbacks_[trigger].emplace(event);
  if (notified_triggers_.count(trigger)) {
    notified_enabled_callbacks_.emplace(event);
  }

  notified_triggers_fd_.write_event(1);
}

void EventLoop::TriggerableCallbackDisable(access::EventLoop,
                                           TriggerableCallback* event) {
  thread_check_.Check();

  auto trigger = event->GetTriggerID();
  trigger_to_enabled_callbacks_[trigger].erase(event);
  if (trigger_to_enabled_callbacks_[trigger].empty()) {
    trigger_to_enabled_callbacks_.erase(trigger);
  }
  notified_enabled_callbacks_.erase(event);
}

void EventLoop::TriggerableCallbackClose(access::EventLoop access,
                                         TriggerableCallback* event) {
  thread_check_.Check();

  TriggerableCallbackDisable(access, event);
}

// TODO(t8971722)
std::unique_ptr<Stream> EventLoop::OpenStream(const HostId& destination) {
  return OpenStream(destination, outbound_allocator_.Next());
}

Stream* EventLoop::GetInboundStream(StreamID stream_id) {
  auto it = stream_id_to_stream_.find(stream_id);
  if (it == stream_id_to_stream_.end()) {
    return nullptr;
  }
  return it->second;
}

std::unique_ptr<Stream> EventLoop::OpenStream(const HostId& destination,
                                              StreamID stream_id) {
  thread_check_.Check();
  if (!destination) {
    RS_ASSERT(false);
    return nullptr;
  }

  // Attempt to reuse a connection.
  SocketEvent* socket;
  auto it = outbound_connections_.find(destination);
  if (it == outbound_connections_.end()) {
    // Open a new one if we don't have one already.
    socket = OpenSocketEvent(destination);
    if (!socket) {
      LOG_ERROR(info_log_,
                "Failed to open Stream to: %s, failed to open connection",
                destination.ToString().c_str());
      return nullptr;
    }
  } else {
    socket = it->second;
  }

  // Create a new stream.
  auto stream = socket->OpenStream(stream_id);
  RS_ASSERT(stream);
  // It will not be owned by this EventLoop.
  return stream;
}

SocketEvent* EventLoop::OpenSocketEvent(const HostId& destination) {
  RS_ASSERT(!!destination);
  thread_check_.Check();

  int fd;
  Status st = create_connection(destination, &fd);
  if (!st.ok()) {
    LOG_ERROR(info_log_,
              "Failed to connect socket to: %s failed, %s",
              destination.ToString().c_str(),
              st.ToString().c_str());
    return nullptr;
  }

  // Create SocketEvent and pass ownership to the loop.
  auto owned_socket =
      SocketEvent::Create(this, fd, options_.protocol_version, destination);
  const auto socket = owned_socket.get();
  if (!socket) {
    LOG_ERROR(info_log_,
              "Failed to create SocketEvent for fd(%d) to: %s",
              fd,
              destination.ToString().c_str());
    // Close the socket.
    close(fd);
    return nullptr;
  }
  owned_connections_.emplace(socket, std::move(owned_socket));

  // Record the connection in the cache.
  outbound_connections_.emplace(destination, socket);
  // Setup a connect timeout.
  connect_timeout_.Add(socket);

  // Update the number of active connections.
  active_connections_.store(owned_connections_.size(),
                            std::memory_order_release);

  LOG_INFO(info_log_,
           "Connect to: %s scheduled on socket fd(%d)",
           destination.ToString().c_str(),
           fd);
  return socket;
}

void EventLoop::CloseFromSocketEvent(access::EventLoop, SocketEvent* socket) {
  RS_ASSERT(socket);
  thread_check_.Check();

  // Cancel connect timeout.
  connect_timeout_.Erase(socket);

  // Remove the socket from internal routing structures.
  size_t removed = outbound_connections_.erase(socket->GetDestination());
  RS_ASSERT(removed == 1 || !socket->GetDestination());
  (void)removed;

  // Defer destruction of the socket.
  auto it = owned_connections_.find(socket);
  if (it != owned_connections_.end()) {
    auto owned_socket = std::move(it->second);
    owned_connections_.erase(it);
    RS_ASSERT(owned_socket.get() == socket);
    AddTask(MakeDeferredDeleter(owned_socket));
  }

  // Update the number of active connections.
  active_connections_.store(owned_connections_.size(),
                            std::memory_order_release);
}

void EventLoop::CloseAllSocketEvents() {
  thread_check_.Check();

  // We just close all sockets owned by the loop one by one.
  while (!owned_connections_.empty()) {
    SocketEvent* socket = owned_connections_.begin()->second.get();
    socket->Close(SocketEvent::ClosureReason::Graceful);
  }

  // Destructions of stream and socket control structured will be deferred. Make
  // sure that they are performed before we exit this method.
  // Execute all remaining tasks.
  while (!ExecuteTasks()) {
    // Once more.
  }
}

void EventLoop::AddInboundStream(access::EventLoop, Stream* stream) {
  RS_ASSERT(stream);
  thread_check_.Check();

  {
    auto result = stream_id_to_stream_.emplace(stream->GetLocalID(), stream);
    RS_ASSERT(result.second);
    (void)result;
  }

  if (options_.enable_heartbeats) {
    // send immediately on connection
    stream->Write(MessageHeartbeat(SystemTenant));
  }
}

// TODO(t8971722)
void EventLoop::CloseFromSocketEvent(access::EventLoop, Stream* stream) {
  RS_ASSERT(stream);
  thread_check_.Check();

  // Remove stream from internal routing structures.
  stream_id_to_stream_.erase(stream->GetLocalID());
  // The stream might not have been managed by the old API.

  // Defer destruction of the stream.
  auto it = owned_streams_.find(stream);
  if (it != owned_streams_.end()) {
    auto owned_stream = std::move(it->second);
    owned_streams_.erase(it);
    RS_ASSERT(owned_stream.get() == stream);
    AddTask(MakeDeferredDeleter(owned_stream));
  }

  heartbeats_to_send_->Remove(stream->GetLocalID());
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
    auto tid = env_->GetCurrentThreadId();
    std::string suffix = "-[tid=" + std::to_string(tid) + "]";
    std::shared_ptr<CommandQueue> command_queue =
      CreateCommandQueue(default_command_queue_size_, suffix);

    // Set this as the thread local queue.
    command_queue_ptr = new std::shared_ptr<CommandQueue>(command_queue);
    command_queues_.Reset(command_queue_ptr);
  }
  return *command_queue_ptr;
}

std::shared_ptr<CommandQueue> EventLoop::CreateCommandQueue(
    size_t size, const std::string& suffix) {
  if (size == 0) {
    // Use default size when size == 0.
    size = default_command_queue_size_;
  }
  std::string name = "loop_queue" + suffix;
  auto command_queue =
      std::make_shared<CommandQueue>(info_log_, queue_stats_, size, name);
  AttachQueue(command_queue);
  return command_queue;
}

void EventLoop::AttachQueue(std::shared_ptr<CommandQueue> command_queue) {
  // Attach the new command queue to the event loop.
  std::unique_ptr<Command> attach_command(
    MakeExecuteCommand([this, command_queue] () mutable {
      AddIncomingQueue(std::move(command_queue));
    }));
  SendControlCommand(std::move(attach_command));
}

void EventLoop::AddIncomingQueue(
    std::shared_ptr<CommandQueue> command_queue) {
  // An event that signals new commands in the command queue.
  GetFlowControl()->Register<std::unique_ptr<Command>>(
      command_queue.get(), [this](Flow* flow, std::unique_ptr<Command> cmd) {
        Dispatch(flow, std::move(cmd));
      });

  LOG_DEBUG(info_log_, "Added new command queue to EventLoop");
  incoming_queues_.emplace_back(std::move(command_queue));
}

void EventLoop::AddControlCommandQueue(
    std::shared_ptr<UnboundedMPSCCommandQueue> control_command_queue) {
  // An event that signals new commands in the command queue.
  control_command_queue->RegisterReadCallback(
    this,
    [this] (std::unique_ptr<Command> cmd) {
      // Call registered callback.
      SourcelessFlow no_flow(GetFlowControl());
      Dispatch(&no_flow, std::move(cmd));
      return true;
    });
  control_command_queue->SetReadEnabled(this, true);

  LOG_DEBUG(info_log_, "Added control command queue to EventLoop");
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

void EventLoop::SendControlCommand(std::unique_ptr<Command> command) {
  control_command_queue_->Write(command);
}

void EventLoop::Accept(int fd) {
  // May be called from another thread, so must add to the command queue.
  std::unique_ptr<Command> command(new AcceptCommand(fd));
  SendCommand(command);
}

void EventLoop::Dispatch(Flow* flow, std::unique_ptr<Command> command) {
  stats_.commands_processed->Add(1);

  // Search for callback registered for this command type.
  // Command ownership will be passed along to the callback.
  const auto type = command->GetCommandType();
  auto iter = command_callbacks_.find(type);
  if (iter != command_callbacks_.end()) {
    iter->second(flow, std::move(command));
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

  setup_keepalive(sockfd, env_options_);

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

////////////////////////////////////////////////////////////////////////////////
EventLoop::Options::Options()
: env(ClientEnv::Default())
, info_log(std::make_shared<NullLogger>())
, protocol_version(kCurrentMsgVersion) {}

////////////////////////////////////////////////////////////////////////////////
EventLoop::Runner::Runner(EventLoop* event_loop)
: event_loop_(event_loop), thread_id_(0) {
  status_ = event_loop_->Initialize();
  if (!status_.ok()) {
    return;
  }
  thread_id_ =
      event_loop_->GetEnv()->StartThread([this]() { event_loop_->Run(); });
  if (!thread_id_) {
    status_ = Status::InternalError("Failed to start EventLoop::Runner thread");
  }
}

EventLoop::Runner::~Runner() {
  event_loop_->Stop();
  if (thread_id_) {
    event_loop_->GetEnv()->WaitForJoin(thread_id_);
  }
}

////////////////////////////////////////////////////////////////////////////////
EventLoop::EventLoop(EventLoop::Options options, StreamAllocator allocator)
: options_(std::move(options))
, env_(options_.env)
, env_options_(options_.env_options)
, port_number_(options_.listener_port)
, running_(false)
, base_(nullptr)
, info_log_(options_.info_log)
, notified_triggers_fd_(port::Eventfd(true, true))
, accept_callback_(std::move(options_.accept_callback))
, listener_(nullptr)
, shutdown_eventfd_(port::Eventfd(true, true))
, command_queues_(CommandQueueUnrefHandler)
, event_callback_receiver_(std::move(options_.event_callback))
, inbound_allocator_(allocator.Split())
, outbound_allocator_(std::move(allocator))
, active_connections_(0)
, stats_(options_.stats_prefix)
, queue_stats_(std::make_shared<QueueStats>(options_.stats_prefix + ".queues"))
, socket_stats_(std::make_shared<SocketEventStats>(options_.stats_prefix))
, default_command_queue_size_(options_.command_queue_size)
, heartbeats_to_send_(new ObservableSet<StreamID>(this, "heartbeats"))
, flow_control_(new FlowControl(options_.stats_prefix, this)) {
  // Setup callbacks.
  command_callbacks_[CommandType::kAcceptCommand] = [this](
      Flow* flow, std::unique_ptr<Command> command) {
    HandleAcceptCommand(std::move(command));
  };
  command_callbacks_[CommandType::kSendCommand] = [this](
      Flow* flow, std::unique_ptr<Command> command) {
    HandleSendCommand(flow, std::move(command));
  };
  command_callbacks_[CommandType::kExecuteCommand] = [](
      Flow* flow, std::unique_ptr<Command> command) {
    static_cast<ExecuteCommand*>(command.get())->Execute(flow);
  };

  LOG_INFO(info_log_, "Created a new Event Loop at port %d", port_number_);
}

EventLoop::Stats::Stats(const std::string& prefix) {
  commands_processed = all.AddCounter(prefix + ".commands_processed");
  accepts = all.AddCounter(prefix + ".accepts");
  queue_count = all.AddCounter(prefix + ".queue_count");
  full_queue_errors = all.AddCounter(prefix + ".full_queue_errors");
  known_streams = all.AddCounter(prefix + ".known_streams");
  owned_streams = all.AddCounter(prefix + ".owned_streams");
  outbound_connections = all.AddCounter(prefix + ".outbound_connections");
  all_connections = all.AddCounter(prefix + ".all_connections");
  hbs_sent = all.AddCounter(prefix + ".hbs_sent");
}

Statistics EventLoop::GetStatistics() const {
  stats_.queue_count->Set(incoming_queues_.size());
  stats_.known_streams->Set(stream_id_to_stream_.size());
  stats_.owned_streams->Set(owned_streams_.size());
  stats_.outbound_connections->Set(outbound_connections_.size());
  stats_.all_connections->Set(owned_connections_.size());
  Statistics stats = stats_.all;
  stats.Aggregate(queue_stats_->all);
  stats.Aggregate(socket_stats_->all);
  stats.Aggregate(flow_control_->GetStatistics());
  return stats;
}

EventLoop::~EventLoop() {
  // Event loop should already be stopped by this point, and the running
  // thread should be joined.
  RS_ASSERT(!running_);
  shutdown_eventfd_.closefd();
  notified_triggers_fd_.closefd();
  event_base_free(base_);
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
    return "???";  // never reached
  }
}

void EventLoop::GlobalShutdown() {
  libevent_global_shutdown();
}

int EventLoop::GetNumClients() const {
  return static_cast<int>(stream_id_to_stream_.size());
}

size_t EventLoop::GetQueueSize() const {
  return const_cast<EventLoop*>(this)->GetThreadLocalQueue()->GetSize();
}

void EventLoop::RegisterFdReadEvent(int fd, std::function<void()> callback) {
  auto event_callback =
      EventCallback::CreateFdReadCallback(this, fd, std::move(callback));
  fd_read_events_.emplace(fd, std::move(event_callback));
}

void EventLoop::SetFdReadEnabled(int fd, bool enabled) {
  auto it = fd_read_events_.find(fd);
  RS_ASSERT(it != fd_read_events_.end());
  if (enabled) {
    it->second->Enable();
  } else {
    it->second->Disable();
  }
}

}  // namespace rocketspeed
