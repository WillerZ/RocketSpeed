// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <inttypes.h>
#include <sys/types.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>
#include <atomic>
#include <functional>
#include <memory>
#include <map>
#include <mutex>
#include "external/folly/producer_consumer_queue.h"
#include "src/port/Env.h"
#include "src/messages/commands.h"
#include "src/messages/serializer.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/statistics.h"
#include "src/util/object_pool.h"

namespace rocketspeed {

typedef void* EventCallbackContext;
typedef std::function<void(EventCallbackContext, std::unique_ptr<Message> msg)>
                                            EventCallbackType;

class SocketEvent;

// A refcounted, pooled version of a serialized message string
struct SharedString : public PooledObject<SharedString> {
 public:
  explicit SharedString(std::string s, int c)
  : store(std::move(s))
  , refcount(c) {}

  std::string store;
  int refcount;
};

class EventLoop {
 public:
  /*
   * Create an EventLoop at the specified port.
   * @param port The port on which the EventLoop is running.
   *             Set to <= 0 to have no accept loop.
   * @param info_log Write informational messages to this log
   * @param event_callback Callback invoked when Dispatch is called
   * @param command_callback Callback invoked for every msg received
   * @param command_queue_size The size of the internal command queue
   */
  EventLoop(Env* env,
            EnvOptions env_options,
            int port,
            const std::shared_ptr<Logger>& info_log,
            EventCallbackType event_callback,
            const std::string& stats_prefix = "",
            uint32_t command_queue_size = 65536);

  virtual ~EventLoop();

  /**
   *  Set the callback context
   * @param arg A opaque blob that is passed back to every invocation of
   *            event_callback_.
   */
  void SetEventCallbackContext(EventCallbackContext ctx) {
    event_callback_context_ = ctx;
  }

  // Start this instance of the Event Loop
  void Run(void);

  // Is the EventLoop up and running?
  bool IsRunning() const { return running_; }

  // Stop the event loop.
  void Stop();

  // Send a command to the event loop for processing.
  // This call is thread-safe.
  Status SendCommand(std::unique_ptr<Command> command);

  // Dispatches a message to the event callback.
  void Dispatch(std::unique_ptr<Message> message);

  // Get the info log.
  const std::shared_ptr<Logger>& GetLog() { return info_log_; }

  // Get event loop statistics
  const Statistics& GetStatistics() const {
    return stats_.all;
  }

 private:
  friend class SocketEvent;

  SharedString* AllocString(std::string s, int c) {
    return string_pool_.Allocate(s, c);
  }

  void FreeString(SharedString* s) {
    string_pool_.Deallocate(s);
  }

  // Env
  Env* env_;

  // Env options
  EnvOptions env_options_;

  // the port nuber of
  int port_number_;

  // Is the EventLoop all setup and running?
  std::atomic<bool> running_;

  // The event loop base.
  struct event_base *base_;

  // debug message go here
  const std::shared_ptr<Logger> info_log_;

  // The callbacks
  EventCallbackType event_callback_;

  // The callback context
  EventCallbackContext event_callback_context_;

  // The connection listener
  evconnlistener* listener_ = nullptr;

  // Shutdown event
  struct event* shutdown_event_ = nullptr;

  // Startup event
  struct event* startup_event_ = nullptr;

  // Command event
  struct event* command_ready_event_ = nullptr;

  // Command queue and its associated event
  folly::ProducerConsumerQueue<std::unique_ptr<Command>> command_queue_;
  int command_ready_eventfd_ = -1;
  std::mutex command_queue_write_mutex_;

  // a cache of HostId to connections
  std::map<HostId, std::vector<SocketEvent*>> connection_cache_;

  // Object pool of SharedStrings
  PooledObjectList<SharedString> string_pool_;

  struct Stats {
    explicit Stats(const std::string& prefix) {
      command_latency = all.AddLatency(prefix + ".command_latency");
      commands_processed = all.AddCounter(prefix + ".commands_processed");
    }

    Statistics all;
    Histogram* command_latency;
    Counter* commands_processed;
  } stats_;

  // connection cache updates
  bool insert_connection_cache(const HostId& host, SocketEvent* ev);
  bool remove_connection_cache(const HostId& host, SocketEvent* ev);
  SocketEvent* lookup_connection_cache(const HostId& host) const;
  SocketEvent* setup_connection(const HostId& host);
  Status create_connection(const HostId& host, bool block, int* fd);
  void clear_connection_cache();

  // callbacks needed by libevent
  static void do_accept(struct evconnlistener *listener,
    evutil_socket_t fd, struct sockaddr *address, int socklen,
    void *arg);
  static Status setup_fd(evutil_socket_t fd, EventLoop* event_loop);
  static void accept_error_cb(struct evconnlistener *listener, void *arg);
  static void do_startevent(evutil_socket_t listener, short event, void *arg);
  static void do_shutdown(evutil_socket_t listener, short event, void *arg);
  static void do_command(evutil_socket_t listener, short event, void *arg);
  static void dump_libevent_cb(int severity, const char* msg);
};

}  // namespace rocketspeed
