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
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "external/folly/producer_consumer_queue.h"

#include "include/Logger.h"
#include "src/messages/commands.h"
#include "src/messages/serializer.h"
#include "src/messages/stream_allocator.h"
#include "src/messages/unique_stream_map.h"
#include "src/port/port.h"
#include "src/util/common/base_env.h"
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

typedef std::function<void(std::unique_ptr<Message> msg, StreamID origin)>
  EventCallbackType;

typedef std::function<void(int fd)> AcceptCallbackType;

// Callback registered for a command type is invoked for all commands of the
// type.
typedef std::function<void(std::unique_ptr<Command> command)>
  CommandCallbackType;

typedef std::function<void()> TimerCallbackType;

class CommandQueue;
class EventCallback;
class EventLoop;
struct QueueStats;
class SocketEvent;

/**
 * Maintains open streams and connections and mapping between them.
 * Performs remapping of stream IDs bewteen IDs which are unique per connection
 * (local) and unique per instance of this class (global). All stream IDs used
 * by this class are the global ones unless otherwise noted.
 */
class StreamRouter {
 public:
  /**
  * Creates an empty router.
  * @param inbound_alloc An allocator to be used when building a mapping
  *                      between connection-local and global stream IDs.
  */
  explicit StreamRouter(StreamAllocator inbound_alloc)
      : open_streams_(std::move(inbound_alloc)) {}

  /**
   * Finds a local stream ID and connection for given stream (identified by
   * global stream ID from the spec), if none is assigned, assigns or creates
   * one based on destination provided in the spec.
   * If a new connection needs to be created, delegates this responsibility back
   * to event loop via provided non-owning pointer.
   */
  Status GetOutboundStream(const SendCommand::StreamSpec& spec,
                           EventLoop* event_loop,
                           SocketEvent** out_sev,
                           StreamID* out_local);

  typedef UniqueStreamMap<SocketEvent*>::GetGlobalStatus RemapStatus;

  /**
   * Remaps provided local stream ID into global stream ID and associates it
   * with provided connection, if requested.
   * If association exists, returns kFound and sets out parameter found
   * global stream ID.
   * If no association exists and caller requested insertion, returns
   * kInserted and sets out parameter to newly assigned global stream ID.
   * If no association exists and caller did not request insertion, returns
   * kNotInserted.
   */
  RemapStatus RemapInboundStream(SocketEvent* sev,
                                 StreamID local,
                                 bool insert,
                                 StreamID* out_global);

  typedef UniqueStreamMap<SocketEvent*>::RemovalStatus RemovalStatus;

  /**
   * Called when stream (identified by global stream ID) shall be closed for
   * whatever reason. Returns a tuple:
   * - RemovalStatus, which indicates whether removal was performed and whether
   * it was the last stream on the connection,
   * - connection that this stream was using,
   * - local stream ID for this global stream ID.
   */
  std::tuple<RemovalStatus, SocketEvent*, StreamID> RemoveStream(
      StreamID global);

  /**
   * Called when connection fails, returns a set of all streams (identified by
   * global stream IDs) assigned to the connection and marks them as
   * closed-broken.
   */
  std::vector<StreamID> RemoveConnection(SocketEvent* sev);

  /** Returns mappings for all connections. */
  void CloseAll() {
    // This will typically be called after the event loop thread terminates.
    thread_check_.Reset();
    open_connections_.clear();
    open_streams_.Clear();
  }

  /** Returns number of open streams. */
  size_t GetNumStreams() const {
    thread_check_.Check();
    return open_streams_.GetNumStreams();
  }

 private:
  ThreadCheck thread_check_;
  /** A mapping from destination to corresponding connection. */
  std::unordered_map<HostId, SocketEvent*> open_connections_;
  /** Maps global <-> (connection, local) for all open streams. */
  UniqueStreamMap<SocketEvent*> open_streams_;
};

class EventLoop {
 public:
  class Options;

  static void EnableDebug();

  /**
   * Create an EventLoop at the specified port.
   * @param port The port on which the EventLoop is running.
   *             Set to <= 0 to have no accept loop.
   * @param info_log Write informational messages to this log
   * @param event_callback Callback invoked when Dispatch is called
   * @param accept_callback Callback invoked when a new client connects
   * @param command_queue_size The size of the internal command queue
   * @param allocator Represents a set of stream IDs available to this loop.
   * @param options All the arbitrary options
   */
  EventLoop(BaseEnv* env,
            EnvOptions env_options,
            int port,
            const std::shared_ptr<Logger>& info_log,
            EventCallbackType event_callback,
            AcceptCallbackType accept_callback,
            StreamAllocator allocator,
            Options options);

  virtual ~EventLoop();

  /**
   * Initialize the event loop.
   * This allows commands to be sent, but won't be processed until the event
   * loop starts running.
   */
  Status Initialize();

  // Start this instance of the Event Loop
  void Run(void);

  // Is the EventLoop up and running?
  bool IsRunning() const { return running_; }

  // Stop the event loop.
  void Stop();

  Status RegisterTimerCallback(TimerCallbackType callback,
                             std::chrono::microseconds period);

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

  // Start communicating on a fd.
  // This call is thread-safe.
  void Accept(int fd);

  // Dispatches a message to the event callback.
  void Dispatch(std::unique_ptr<Message> message, StreamID origin);

  /**
   * Invokes callback for provided command in the calling thread.
   * Can only be called from the message loop thread.
   *
   * @param command A command to be executed
   */
  void Dispatch(std::unique_ptr<Command> command);

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
   * @return The created queue.
   */
  std::shared_ptr<CommandQueue> CreateCommandQueue(size_t size = 0);


  /**
   * Attaches the command queue to the EventLoop for processing.
   *
   * @param command_queue The CommandQueue to begin reading and processing.
   * @return ok() if successful, otherwise error.
   */
  Status AttachQueue(std::shared_ptr<CommandQueue> command_queue);

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
    * Option is a helper class used for passing the additional arguments to the
    * EventLoop constructor.
    */
  class Options {
   public:
    // prefix used for statistics
    std::string stats_prefix;
    // initial size of the command queue
    uint32_t command_queue_size = 50000;
    // timeout after which all inactive streams should be considered expired
    std::chrono::seconds heartbeat_timeout{900};
    // since we expire the streams in the blocking call, limit the number of
    // streams expired at once. the rest will be processed in the next call.
    int heartbeat_expire_batch = -1;
    // whether the stream heartbeat check is enabled
    bool heartbeat_enabled = false;
    // timeout for asynchronous ::connect calls
    std::chrono::milliseconds connect_timeout{10000};
  };

 private:
  friend class SocketEvent;
  friend class StreamRouter;

  const Options options_;

  // Internal status of the EventLoop.
  // If something fatally fails internally then the event loop will stop
  // running and Run will return this status.
  Status internal_status_;

  BaseEnv* env_;

  EnvOptions env_options_;

  // Port nuber of accept loop (in network byte order)
  int port_number_ = -1;

  // Is the EventLoop all setup and running?
  std::atomic<bool> running_;
  port::Semaphore start_signal_;

  // The event loop base.
  event_base *base_ = nullptr;

  // debug message go here
  const std::shared_ptr<Logger> info_log_;

  // The callbacks
  EventCallbackType event_callback_;
  AcceptCallbackType accept_callback_;
  std::map<CommandType, CommandCallbackType> command_callbacks_;

  // The connection listener
  evconnlistener* listener_ = nullptr;

  // Shutdown event
  std::unique_ptr<EventCallback> shutdown_event_;
  rocketspeed::port::Eventfd shutdown_eventfd_;

  // Startup event
  event* startup_event_ = nullptr;

  // Each thread has its own command queue to communicate with the EventLoop.
  ThreadLocalPtr command_queues_;

  // Shared command queue for sending control commands.
  // This should only be used for creating new queues.
  port::Mutex control_command_mutex_;
  std::shared_ptr<CommandQueue> control_command_queue_;

  StreamRouter stream_router_;
  /** Allocator for outboung streams. */
  StreamAllocator outbound_allocator_;

  // List of all sockets.
  std::list<std::unique_ptr<SocketEvent>> all_sockets_;

  // Number of open connections, including accepted connections, that we haven't
  // received any data on.
  std::atomic<uint64_t> active_connections_;

  // weather the stream heartbeat check is enabled
  bool heartbeat_enabled_;
  // the timed list used for tracking the stream activity & expire unused ones
  TimeoutList<StreamID> heartbeat_;
  // timeout after which all inactive streams should be considered expired
  std::chrono::seconds heartbeat_timeout_;
  // since we expire the streams in the blocking call, limit the number of
  // streams expired at once. the rest will be processed in the next call.
  int heartbeat_expire_batch_;
  // the callback invoked on the expired streams in the heartbeat_. it should
  // be responsible for closing the stream properly
  std::function<void(StreamID)> heartbeat_expired_callback_;

  // Timeouts for connect calls.
  // Non-blocking connects do eventually timeout after ~2 minutes, but this
  // is too long, and generally non-configurable, so we actively close the
  // socket if it doesn't become writable after some time.
  TimeoutList<SocketEvent*> connect_timeout_;

  // Timer callbacks.
  struct Timer {
    explicit Timer(TimerCallbackType _callback)
    : callback(std::move(_callback)) {
    }

    Timer(const Timer&) = delete;
    Timer(Timer&&) = delete;
    Timer& operator=(const Timer&) = delete;
    Timer& operator=(Timer&&) = delete;

    TimerCallbackType callback;
    event* loop_event = nullptr;
  };
  std::vector<std::unique_ptr<Timer>> timers_;

  // Thread check
  rocketspeed::ThreadCheck thread_check_;

  struct Stats {
    explicit Stats(const std::string& prefix);

    Statistics all;
    Histogram* write_latency;     // time from SendCommand to socket write
    Histogram* write_size_bytes;  // total bytes in write calls
    Histogram* write_size_iovec;  // total iovecs in write calls.
    Histogram* write_succeed_bytes; // successful bytes written in write calls.
    Histogram* write_succeed_iovec; // successful iovecs written in write calls.
    Counter* commands_processed;
    Counter* accepts;             // number of connection accepted
    Counter* queue_count;         // number of queues attached this loop
    Counter* full_queue_errors;   // number of times SendCommand into full queue
    Counter* messages_received[size_t(MessageType::max) + 1];
    Counter* socket_writes;       // number of calls to write(v)
    Counter* partial_socket_writes; // number of writes that partially succeeded
  } stats_;

  const std::shared_ptr<QueueStats> queue_stats_;

  const uint32_t default_command_queue_size_;

  // libevent read notification events for
  struct IncomingQueue {
    IncomingQueue() {}
    ~IncomingQueue();

    std::shared_ptr<CommandQueue> queue;
  };
  std::vector<std::unique_ptr<IncomingQueue>> incoming_queues_;

  // Send a command using a particular command queue.
  Status SendCommand(std::unique_ptr<Command>& command,
                     CommandQueue* command_queue);

  const std::shared_ptr<CommandQueue>& GetThreadLocalQueue();

  Status AddIncomingQueue(std::shared_ptr<CommandQueue> command_queue);

  void HandleSendCommand(std::unique_ptr<Command> command);
  void HandleAcceptCommand(std::unique_ptr<Command> command);

  // connection cache updates
  void remove_host(const HostId& host);
  SocketEvent* setup_connection(const HostId& destination);
  Status create_connection(const HostId& host, int* fd);
  void teardown_connection(SocketEvent* ev);
  void teardown_all_connections();

  // callbacks needed by libevent
  static void do_accept(evconnlistener *listener,
    int fd, sockaddr *address, int socklen,
    void *arg);
  static Status setup_fd(int fd, EventLoop* event_loop);
  static void accept_error_cb(evconnlistener *listener, void *arg);
  static void do_startevent(int listener, short event, void *arg);
  static void do_timerevent(int listener, short event, void *arg);
};

class EventCallback {
 public:
  ~EventCallback();

  /**
   * Creates an EventCallback that will be invoked when fd becomes readable.
   * cb will be invoked on the event_loop thread.
   *
   * @param event_loop The EventLoop to add the event to.
   * @param fd File descriptor to listen for reads.
   * @param cb Callback to invoke when fd is readable.
   * @return EventCallback object.
   */
  static std::unique_ptr<EventCallback> CreateFdReadCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb);

  /**
   * Creates an EventCallback that will be invoked when fd becomes writable.
   * cb will be invoked on the event_loop thread.
   *
   * @param event_loop The EventLoop to add the event to.
   * @param fd File descriptor to listen for writes.
   * @param cb Callback to invoke when fd is writable.
   * @return EventCallback object.
   */
  static std::unique_ptr<EventCallback> CreateFdWriteCallback(
    EventLoop* event_loop,
    int fd,
    std::function<void()> cb);

  // non-copyable, non-moveable
  EventCallback(const EventCallback&) = delete;
  EventCallback(EventCallback&&) = delete;
  EventCallback& operator=(const EventCallback&) = delete;
  EventCallback& operator=(EventCallback&&) = delete;

  /** Invokes the callback */
  void Invoke();

  /** Enables the event */
  void Enable();

  /** Disables the event */
  void Disable();

  /** @return true iff currently enabled. */
  bool IsEnabled() const {
    return enabled_;
  }

 private:
  explicit EventCallback(EventLoop* event_loop, std::function<void()> cb);

  EventLoop* event_loop_;
  event* event_;
  std::function<void()> cb_;
  bool enabled_;
};

}  // namespace rocketspeed
