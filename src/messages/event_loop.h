// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <inttypes.h>
#include <sys/types.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "external/folly/producer_consumer_queue.h"

#include "include/BaseEnv.h"
#include "include/Logger.h"
#include "src/messages/commands.h"
#include "src/messages/event_callback.h"
#include "src/messages/serializer.h"
#include "src/messages/stream_allocator.h"
#include "src/messages/unique_stream_map.h"
#include "src/port/port.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/statistics.h"
#include "src/util/common/thread_check.h"
#include "src/util/common/thread_local.h"
#include "src/util/timeout_list.h"

// libevent2 forward declarations.
struct event;
struct event_base;
struct evconnlistener;
struct sockaddr;

namespace rocketspeed {

class CommandQueue;
class UnboundedMPSCCommandQueue;
class EventCallback;
class EventLoop;
class EventTrigger;
class Flow;
class FlowControl;
class SocketEvent;
class TimedCallback;
using TriggerID = uint64_t;
class TriggerableCallback;
class SocketEventStats;
class Stream;
class QueueStats;
template <typename T>
class ObservableSet;

typedef std::function<void(
    Flow* flow, std::unique_ptr<Message> message, StreamID origin)>
    EventCallbackType;

typedef std::function<void(int fd)> AcceptCallbackType;

// Callback registered for a command type is invoked for all commands of the
// type.
typedef std::function<void(Flow* flow, std::unique_ptr<Command> command)>
    CommandCallbackType;

typedef std::function<void()> TimerCallbackType;

namespace access {
/**
 * This grants access to a certain subset of methods of EventLoop.
 * We don't want to do it for every class, but only for those that constitute
 * the public API. Friends stink, we have a tradition of abusing them.
 */
class EventLoop {
 private:
  friend class rocketspeed::SocketEvent;
  friend class rocketspeed::TriggerableCallback;
  EventLoop() = default;
};
}  // namespace access

class EventLoop {
 public:
  static void EnableDebug();

  class Options {
   public:
    Options();

    /** An environment to use. */
    BaseEnv* env;
    /** Environment options. */
    EnvOptions env_options;
    /** Default logger. */
    std::shared_ptr<Logger> info_log;
    /** Statistics prefix. */
    std::string stats_prefix;
    /** Default size of the command queue. */
    uint32_t command_queue_size = 50000;
    /** Timeout for asynchronous ::connect calls. */
    std::chrono::milliseconds connect_timeout{10000};
    /**
     * The port on which the loop is listening.
     * Set to 0 to have auto-allocated port.
     * Set to < 0 to have no accept loop.
     */
    int32_t listener_port = -1;
    /** A callback for receiving messages. */
    EventCallbackType event_callback;
    /** A callback for handling new incoming connections. */
    AcceptCallbackType accept_callback;
    /** A bound on the size of socket's send queue. */
    size_t send_queue_limit = 30000;
    /**
     * For how long should we keep a connection without streams alive.
     */
    std::chrono::milliseconds connection_without_streams_keepalive{0};
    /** Preferred protocol version. */
    uint8_t protocol_version;
    /** Should we send out heartbeats? */
    bool enable_heartbeats = true;
    /** Send heartbeats every X milliseconds */
    std::chrono::milliseconds heartbeat_period = std::chrono::seconds(60);
    /** How long a source must be blocked before logging warnings. */
    std::chrono::milliseconds flow_control_blocked_warn_period =
      std::chrono::seconds(10);
  };

  /**
   * A helper class which initiates provided EventLoop and drives it from a
   * specifically created thread.
   */
  class Runner : public NonCopyable {
   public:
    /** Initializes the EventLoop and drives it from a dedicated thread. */
    explicit Runner(EventLoop* event_loop);

    ~Runner();

    const Status& GetStatus() const { return status_; }

   private:
    EventLoop* const event_loop_;
    Status status_;
    BaseEnv::ThreadId thread_id_;
  };

  /**
   * Create an EventLoop with provided options.
   *
   * @param options General configuration of the EventLoop.
   * @param allocator An allocator which limits the space of StreamIDs the loop
   *                  can use.
   */
  EventLoop(Options options, StreamAllocator allocator);

  ~EventLoop();

  /**
   * Initialize the event loop.
   * This allows commands to be sent, but won't be processed until the event
   * loop starts running.
   */
  Status Initialize();

  /** Drives the event loop until it is stopped. */
  void Run();

  /**
   * Runs one iteration of this EventLoop.
   *
   * @return True if the loop has been stopped.
   */
  bool RunOnce();

  /** Is the EventLoop running. */
  bool IsRunning() const { return running_; }

  /** Stops the event loop. */
  void Stop();

  /**
   * Executes provided closure on EventLoop thread but after this method
   * returns. Tasks are executed in the order they were added.
   * Can only be called from EventLoop thread.
   *
   * This should not be overused on critical path, but it is encouraged in all
   * places where invoking callbacks inline might lead to bugs or complicated
   * corner cases.
   *
   * @param task A task to queue and execute later on.
   */
  void AddTask(std::function<void()> task);

  std::unique_ptr<EventCallback> RegisterTimerCallback(
    TimerCallbackType callback, std::chrono::microseconds period,
    bool enabled = true);

  /**
   * Creates a new trigger, which can be used to notify EventCallbacks.
   * This method is thread safe.
   */
  EventTrigger CreateEventTrigger();

  /**
   * Marks provided trigger as notified.
   * EventLoop will execute enabled callbacks registered with provided trigger
   * until the notification is cleared.
   * Must be called from the thread that drives the loop.
   *
   * @param trigger An EventTrigger to notify.
   */
  void Notify(const EventTrigger& trigger);

  /**
   * Clears any notifications from provided trigger.
   * Must be called from the thread that drives the loop.
   *
   * @param trigger An EventTrigger to clear notification from.
   */
  void Unnotify(const EventTrigger& trigger);

  /**
   * Creates an EventCallback that will be invoked inline when provided trigger
   * is notified.
   *
   * @param cb Callback to invoke when the trigger is notified.
   * @param trigger A trigger to bind to.
   * @return EventCallback object.
   */
  std::unique_ptr<EventCallback> CreateEventCallback(
      std::function<void()> cb, const EventTrigger& trigger);

  /**
   * Creates an EventCallback that will be invoked after every time the
   * duration passes.
   *
   * @param cb Callback to invoke after timer fires
   * @param duration the time after which callback should run.
   * @return EventCallback object, nullptr only if allocation failed.
   */
  std::unique_ptr<EventCallback> CreateTimedEventCallback(
      std::function<void()> cb, std::chrono::microseconds duration);

  /** Notified the loop that the callback was enabled. */
  void TriggerableCallbackEnable(access::EventLoop, TriggerableCallback* event);

  /** Notified the loop that the callback was disabled. */
  void TriggerableCallbackDisable(access::EventLoop,
                                  TriggerableCallback* event);

  /** Cleans up all the state associated with the callback. */
  void TriggerableCallbackClose(access::EventLoop, TriggerableCallback* event);

  const HostId& GetHostId() const { return host_id_; }

  /**
   * Opens a new stream to provided destination.
   *
   * @param destination A destination to connect to.
   * @return A stream, null if failed.
   */
  std::unique_ptr<Stream> OpenStream(const HostId& destination);

  /**
   * Detaches provided socket from the loop and schedules its destruction if
   * it's an inbound one. Provided socket must be closed beforehand.
   * This method should be called exclusively by the SocketEvent.
   *
   * @param An access parameter to control who can call this method.
   * @param socket A socket to detach.
   */
  void CloseFromSocketEvent(access::EventLoop, SocketEvent* socket);

  StreamAllocator* GetInboundAllocator(access::EventLoop) {
    return &inbound_allocator_;
  }

  void MarkConnected(access::EventLoop, SocketEvent* socket) {
    connect_timeout_.Erase(socket);
  }

  // TODO(t8971722)
  void AddInboundStream(access::EventLoop, Stream* stream);

  // TODO(t8971722)
  Stream* GetInboundStream(StreamID stream_id);

  // TODO(t8971722)
  void CloseFromSocketEvent(access::EventLoop, Stream* stream);

  /**
   * Returns stream ID allocator used by this event loop to create outbound
   * streams.
   *
   * @return Stream allocator which represents outbound stream ID space.
   */
  StreamAllocator* GetOutboundStreamAllocator() {
    return &outbound_allocator_;
  }

  /**
   * Returns true if the global stream ID is for an outbound stream.
   *
   * This call is thread safe.
   */
  bool IsOutboundStream(StreamID global) const {
    return outbound_allocator_.IsSourceOf(global);
  }

  /**
   * Returns true if the global stream ID is for an inbound stream.
   *
   * This call is thread safe.
   */
  bool IsInboundStream(StreamID global) const {
    return inbound_allocator_.IsSourceOf(global);
  }

  /**
   * Returns a new outbound socket. Returned socket is closed (not yet opened)
   * and its stream is allocated using stream ID space available to this loop.
   * This call is not thread-safe.
   * @param destination A destination for the stream.
   * @return A brand new stream socket.
   */
  StreamSocket CreateOutboundStream(HostId destination);

  /**
   * Send a command to the event loop for processing.
   *
   * If the command is successfully queued then it will be moved from its
   * source location, otherwise an error will be returned and command will
   * be left intact, in case the caller wishes to retry later.
   *
   * This call is thread-safe.
   */
  Status SendCommand(std::unique_ptr<Command>& command);

  Status SendRequest(const Message& msg, StreamSocket* socket);

  Status SendResponse(const Message& msg, StreamID stream_id);

  /**
   * Sends a command through the control queue. The control queue is used
   * for one-off initialization, so should be be used at all in steady state.
   * For this reason, it is shared and unbounded to ensure that control
   * commands never fail. This call is thread safe.
   *
   * @param command The command to send.
   */
  void SendControlCommand(std::unique_ptr<Command> command);

  // Start communicating on a fd.
  // This call is thread-safe.
  void Accept(int fd);

  /**
   * Invokes callback for provided command in the calling thread.
   * Can only be called from the message loop thread.
   *
   * @param command A command to be executed
   */
  void Dispatch(Flow* flow, std::unique_ptr<Command> command);

  // Get the info log.
  const std::shared_ptr<Logger>& GetLog() { return info_log_; }

  Statistics GetStatistics() const;

  void ThreadCheck() const {
    thread_check_.Check();
  }

  // Returns a proxy for the amount of load on this thread.
  // This is used for load balancing new connections.
  // This call is thread-safe.
  uint64_t GetLoadFactor() const {
    // As a simple approximation, use number of connections as load proxy.
    // A better implementation may be to count the number of messages processed
    // in the last N seconds.
    return active_connections_.load(std::memory_order_acquire);
  }

  /**
   * Returns the number of active clients on this event loop.
   * Not thread safe,
   */
  int GetNumClients() const;

  /** @return Current size of this thread's command queue. */
  size_t GetQueueSize() const;

  /**
   * Creates a new queue that will be read by the EventLoop.
   *
   * @param size Size of the queue (number of commands). Defaults to whatever
   *             the EventLoop default command queue size is.
   * @param suffix Suffix to append to queue name for debugging.
   * @return The created queue.
   */
  std::shared_ptr<CommandQueue> CreateCommandQueue(
      size_t size = 0,
      const std::string& suffix = "");

  /**
   * Attaches the command queue to the EventLoop for processing.
   *
   * @param command_queue The CommandQueue to begin reading and processing.
   */
  void AttachQueue(std::shared_ptr<CommandQueue> command_queue);

  /**
   * Waits until the event loop is running.
   *
   * @param timeout Maximum time to wait.
   * @return OK if the loop is running, otherwise an error if the loop failed
   *         to start.
   */
  Status WaitUntilRunning(std::chrono::seconds timeout =
                            std::chrono::seconds(10));

  // Debug logging severity levels.
  static const int kLogSeverityDebug;
  static const int kLogSeverityMsg;
  static const int kLogSeverityWarn;
  static const int kLogSeverityErr;

  // A type of a function that, provided with severity level and log message,
  // will print or dicard it appopriately.
  typedef void (*DebugCallback)(int, const char*);

  // Enables debugging of all instances of EventLoop in this application.
  // Messages are handled by provided callback.
  // Debugging is not thread safe in current implementation (we compile
  // libevent without threading support).
  static void EnableDebugThreadUnsafe(DebugCallback log_cb);

  static const char* SeverityToString(int severity);

  // Shutdown libevent. Should be called at end of main().
  static void GlobalShutdown();

  /**
   * Create a read callback for an fd. Once enabled, whenever fd is ready for
   * reading, cb will be invoked from the EventLoop thread.
   *
   * @param fd File descriptor to listen on.
   * @param cb Callback to invoke when read is ready.
   * @param arg Argument for callback.
   * @return Handle for enabling and disabling the event.
   */
  event* CreateFdReadEvent(int fd, void (*cb)(int, short, void*), void* arg);

  /**
   * Create a write callback for an fd. Once enabled, whenever fd is ready for
   * writing, cb will be invoked from the EventLoop thread.
   *
   * @param fd File descriptor to listen on.
   * @param cb Callback to invoke when write is ready.
   * @param arg Argument for callback.
   * @return Handle for enabling and disabling the event.
   */
  event* CreateFdWriteEvent(int fd, void (*cb)(int, short, void*), void* arg);

  /**
   * Registers a callback for read events on a file descriptor.
   *
   * @param fd File descriptor to listen on.
   * @param callback Callback to invoke when write is ready.
   */
  void RegisterFdReadEvent(int fd, std::function<void()> callback);

  /**
   * Enables/disables read events on a previously registered fd.
   *
   * @param fd File descriptor to enable/disable read events for.
   * @param enabled True enabled, false disabled.
   */
  void SetFdReadEnabled(int fd, bool enabled);

  /**
   * Get the queue stats object for this event loop.
   */
  const std::shared_ptr<QueueStats>& GetQueueStats() { return queue_stats_; }

  /**
   * Get the socket stats object for this event loop.
   */
  const std::shared_ptr<SocketEventStats>& GetSocketStats() {
    return socket_stats_;
  }

  StreamReceiver* GetDefaultReceiver() { return &event_callback_receiver_; }

  BaseEnv* GetEnv() { return env_; }

  const Options& GetOptions() const { return options_; }

  FlowControl* GetFlowControl() { return flow_control_.get(); }

 private:
  Options options_;

  // Internal status of the EventLoop.
  // If something fatally fails internally then the event loop will stop
  // running and Run will return this status.
  Status internal_status_;

  BaseEnv* env_;

  EnvOptions env_options_;

  // Port nuber of accept loop (in network byte order)
  int port_number_ = -1;
  HostId host_id_;

  // Is the EventLoop all setup and running?
  std::atomic<bool> running_;
  port::Semaphore start_signal_;

  // The event loop base.
  event_base *base_ = nullptr;

  // debug message go here
  const std::shared_ptr<Logger> info_log_;

  /**
   * A queue of commands to execute.
   * These commands were sent from EventLoop thread and will be executed on the
   * same thread. This enables to trigger certain events to be executed in the
   * loop but not "inline".
   */
  std::deque<std::function<void()>> scheduled_tasks_;

  /**
   * Executes scheduled tasks in order.
   * Might exit before executes all pending tasks.
   *
   * @return True iff all tasks have been executed.
   */
  bool ExecuteTasks();

  /** Next free ID of an EventTrigger. */
  std::atomic<TriggerID> next_trigger_id_{0};

  /** Maps a trigger ID to all enabled callbacks registered with the trigger. */
  std::unordered_map<TriggerID, std::unordered_set<TriggerableCallback*>>
      trigger_to_enabled_callbacks_;
  /** A set of notified triggers. */
  std::unordered_set<TriggerID> notified_triggers_;
  /** A set of enabled callbacks on notified triggers. */
  std::unordered_set<TriggerableCallback*> notified_enabled_callbacks_;

  /** An EventCallback invoked when there is any notified trigger. */
  std::unique_ptr<EventCallback> notified_triggers_event_;
  /** An eventfd that is readable whenever there is a pending trigger. */
  port::Eventfd notified_triggers_fd_;

  /**
   * Handles an event that there are some pending triggers.
   */
  void HandlePendingTriggers();

  // The callbacks
  AcceptCallbackType accept_callback_;
  std::map<CommandType, CommandCallbackType> command_callbacks_;

  // The connection listener
  evconnlistener* listener_ = nullptr;

  // Shutdown event and flag.
  bool shutting_down_ = false;
  std::unique_ptr<EventCallback> shutdown_event_;
  rocketspeed::port::Eventfd shutdown_eventfd_;

  // Startup event
  event* startup_event_ = nullptr;

  // Each thread has its own command queue to communicate with the EventLoop.
  ThreadLocalPtr command_queues_;

  // Shared command queue for sending control commands.
  // This should only be used for creating new queues.
  std::shared_ptr<UnboundedMPSCCommandQueue> control_command_queue_;

  /**
   * A receiver that receives messages and forward them to the callback, which
   * is a part of the old API.
   * This should disappear once we get rid of the old API.
   */
  // TODO(t8971722)
  class EventCallbackReceiver : public StreamReceiver {
   public:
    explicit EventCallbackReceiver(EventCallbackType event_callback)
    : event_callback_(std::move(event_callback)) {}

    void operator()(StreamReceiveArg<Message> arg) override {
      event_callback_(arg.flow, std::move(arg.message), arg.stream_id);
    }

   private:
    EventCallbackType event_callback_;
  } event_callback_receiver_;
  /**
   * A map of all open streams that were created using the old, deprecated
   * stream API.
   * This should disappear once we get rid of the old API.
   */
  // TODO(t8971722)
  std::unordered_map<StreamID, Stream*> stream_id_to_stream_;

  std::unique_ptr<EventCallback> hb_timer_;

  /**
   * A map of all streams owned by the loop.
   * This should disappear once we get rid of the old API.
   */
  // TODO(t8971722)
  std::unordered_map<Stream*, std::unique_ptr<Stream>> owned_streams_;

  /** A map of all established outbound connections. */
  std::unordered_map<HostId, SocketEvent*> outbound_connections_;
  /** A list of all inbound connections. */
  std::unordered_map<SocketEvent*, std::unique_ptr<SocketEvent>>
      owned_connections_;

  /** Internal method to open stream with provided StreamID. */
  std::unique_ptr<Stream> OpenStream(const HostId& destination,
                                     StreamID stream_id);

  /**
   * Creates a new SocketEvent to provided remote host.
   * The returned SocketEvent is added to the cache of outbound connections and
   * owned by the EventLoop.
   *
   * @param destination An address of the remote destination.
   * @return A socket, null if failed.
   */
  SocketEvent* OpenSocketEvent(const HostId& destination);

  /**
   * Detaches and schedules destruction of all SocketEvents owned by the loop.
   */
  void CloseAllSocketEvents();

  /** Allocator for inbound streams. */
  StreamAllocator inbound_allocator_;
  /** Allocator for outboung streams. */
  StreamAllocator outbound_allocator_;

  // Number of open connections, including accepted connections, that we haven't
  // received any data on.
  std::atomic<uint64_t> active_connections_;

  // Timeouts for connect calls.
  // Non-blocking connects do eventually timeout after ~2 minutes, but this
  // is too long, and generally non-configurable, so we actively close the
  // socket if it doesn't become writable after some time.
  TimeoutList<SocketEvent*> connect_timeout_;

  // Thread check
  rocketspeed::ThreadCheck thread_check_;

  struct Stats {
    explicit Stats(const std::string& prefix);

    Statistics all;
    Counter* commands_processed;
    Counter* accepts;             // number of connection accepted
    Counter* queue_count;         // number of queues attached this loop
    Counter* full_queue_errors;   // number of times SendCommand into full queue
    Counter* known_streams;  // number of active stream the loop knows about
    Counter* owned_streams;  // number of stream the loop owns
    Counter* outbound_connections;  // number of outbound connections
    Counter* all_connections;       // number of all connections
    Counter* hbs_sent;              // number of heartbeats sent
  } stats_;

  const std::shared_ptr<QueueStats> queue_stats_;
  const std::shared_ptr<SocketEventStats> socket_stats_;

  const uint32_t default_command_queue_size_;

  std::vector<std::shared_ptr<CommandQueue>> incoming_queues_;

  std::unordered_map<int, std::unique_ptr<EventCallback>> fd_read_events_;

  std::unique_ptr<ObservableSet<StreamID>> heartbeats_to_send_;

  std::unique_ptr<FlowControl> flow_control_;

  // Send a command using a particular command queue.
  Status SendCommand(std::unique_ptr<Command>& command,
                     CommandQueue* command_queue);

  const std::shared_ptr<CommandQueue>& GetThreadLocalQueue();

  void AddIncomingQueue(std::shared_ptr<CommandQueue> command_queue);

  void AddControlCommandQueue(
    std::shared_ptr<UnboundedMPSCCommandQueue> control_command_queue);

  void HandleSendCommand(Flow* flow, std::unique_ptr<Command> command);
  void HandleAcceptCommand(std::unique_ptr<Command> command);

  Status create_connection(const HostId& host, int* fd);

  // callbacks needed by libevent
  static void do_accept(evconnlistener *listener,
    int fd, sockaddr *address, int socklen,
    void *arg);
  static Status setup_fd(int fd, EventLoop* event_loop);
  static void setup_keepalive(int sockfd, const EnvOptions& env_options);
  static void accept_error_cb(evconnlistener *listener, void *arg);
  static void do_startevent(int listener, short event, void *arg);
  static void do_timerevent(int listener, short event, void *arg);
};

}  // namespace rocketspeed
