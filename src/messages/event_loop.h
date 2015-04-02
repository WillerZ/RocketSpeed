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
#include <functional>
#include <memory>
#include <map>
#include <list>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "include/Logger.h"
#include "src/messages/commands.h"
#include "src/messages/serializer.h"
#include "src/messages/stream_allocator.h"
#include "src/messages/unique_stream_map.h"
#include "src/util/common/base_env.h"
#include "src/util/common/statistics.h"
#include "src/util/common/object_pool.h"
#include "src/util/common/thread_check.h"
#include "src/util/common/multi_producer_queue.h"

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
typedef std::function<void(std::unique_ptr<Command> command,
                           uint64_t issued_time)>
  CommandCallbackType;

class EventLoop;
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

  /**
   * Remaps provided local stream ID into global stream ID and associates it
   * with provided connection.
   * If no association for the stream in given context exists, inserts it and
   * returns (true, new global ID), otherwise returns (false, currently assigned
   * global ID).
   */
  std::pair<bool, StreamID> InsertInboundStream(SocketEvent* new_sev,
                                                StreamID local);

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
  std::unordered_map<ClientID, SocketEvent*> open_connections_;
  /** Maps global <-> (connection, local) for all open streams. */
  UniqueStreamMap<SocketEvent*> open_streams_;
};

class EventLoop {
 public:
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
   */
  EventLoop(BaseEnv* env,
            EnvOptions env_options,
            int port,
            const std::shared_ptr<Logger>& info_log,
            EventCallbackType event_callback,
            AcceptCallbackType accept_callback,
            StreamAllocator allocator,
            const std::string& stats_prefix = "",
            uint32_t command_queue_size = 1000000);

  virtual ~EventLoop();

  // Start this instance of the Event Loop
  void Run(void);

  // Is the EventLoop up and running?
  bool IsRunning() const { return running_; }

  // Stop the event loop.
  void Stop();

  // Registers callback for certain command types.
  void RegisterCallback(CommandType type, CommandCallbackType callbacks);

  /**
   * Returns a new outbound socket. Returned socket is closed (not yet opened)
   * and its stream is allocated using stream ID space available to this loop.
   * This call is not thread-safe.
   * @param destination A destination for the stream.
   * @return A brand new stream socket.
   */
  StreamSocket CreateOutboundStream(ClientID destination);

  // Send a command to the event loop for processing.
  // This call is thread-safe.
  Status SendCommand(std::unique_ptr<Command> command);

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
  void Dispatch(std::unique_ptr<Command> command, int64_t issued_time);

  // Get the info log.
  const std::shared_ptr<Logger>& GetLog() { return info_log_; }

  // Get event loop statistics
  const Statistics& GetStatistics() const {
    return stats_.all;
  }

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

 private:
  friend class SocketEvent;
  friend class StreamRouter;

  BaseEnv* env_;

  EnvOptions env_options_;

  // Port nuber of accept loop (in network byte order)
  int port_number_ = -1;

  // Is the EventLoop all setup and running?
  std::atomic<bool> running_;
  port::Semaphore start_signal_;
  Status start_status_;

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
  event* shutdown_event_ = nullptr;
  rocketspeed::port::Eventfd shutdown_eventfd_;

  // Startup event
  event* startup_event_ = nullptr;

  // Command event
  event* command_ready_event_ = nullptr;

  // Command queue and its associated event
  struct TimestampedCommand {
    std::unique_ptr<Command> command;
    uint64_t issued_time;
  };
  MultiProducerQueue<TimestampedCommand> command_queue_;
  rocketspeed::port::Eventfd command_ready_eventfd_;

  StreamRouter stream_router_;
  /** Allocator for outboung streams. */
  StreamAllocator outbound_allocator_;

  // List of all sockets.
  std::list<std::unique_ptr<SocketEvent>> all_sockets_;

  // Number of open connections, including accepted connections, that we haven't
  // received any data on.
  std::atomic<uint64_t> active_connections_;

  // Thread check
  rocketspeed::ThreadCheck thread_check_;

  struct Stats {
    explicit Stats(const std::string& prefix) {
      command_latency = all.AddLatency(prefix + ".command_latency");
      write_latency = all.AddLatency(prefix + ".write_latency");
      commands_processed = all.AddCounter(prefix + ".commands_processed");
      accepts = all.AddCounter(prefix + ".accepts");
    }

    Statistics all;
    Histogram* command_latency;   // time from SendCommand to do_command
    Histogram* write_latency;     // time from SendCommand to socket write
    Counter* commands_processed;
    Counter* accepts;             // number of connection accepted
  } stats_;

  // A callback for handling SendCommands.
  void HandleSendCommand(std::unique_ptr<Command> command,
                         uint64_t issued_time);
  // A callback for handling AcceptCommands.
  void HandleAcceptCommand(std::unique_ptr<Command> command,
                           uint64_t issued_time);

  // connection cache updates
  void remove_host(const ClientID& host);
  SocketEvent* setup_connection(const HostId& host, const ClientID& clientid);
  Status create_connection(const HostId& host, bool block, int* fd);
  void teardown_connection(SocketEvent* ev);
  void teardown_all_connections();

  // callbacks needed by libevent
  static void do_accept(evconnlistener *listener,
    int fd, sockaddr *address, int socklen,
    void *arg);
  static Status setup_fd(int fd, EventLoop* event_loop);
  static void accept_error_cb(evconnlistener *listener, void *arg);
  static void do_startevent(int listener, short event, void *arg);
  static void do_shutdown(int listener, short event, void *arg);
  static void do_command(int listener, short event, void *arg);
};

}  // namespace rocketspeed
