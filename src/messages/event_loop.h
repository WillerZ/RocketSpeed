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
#include "include/Env.h"
#include "src/messages/commands.h"
#include "src/messages/serializer.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/auto_roll_logger.h"

namespace rocketspeed {

typedef void* EventCallbackContext;
typedef std::function<void(EventCallbackContext, std::unique_ptr<Message> msg)>
                                            EventCallbackType;
typedef std::function<void(std::unique_ptr<Command>)> CommandCallbackType;

class SocketEvent;

class EventLoop {
 public:
  /*
   * Create an EventLoop at the specified port.
   * @param port The port on which the EventLoop is running.
   *             Set to <= 0 to have no accept loop.
   * @param info_log Write informational messages to this log
   * @param callback The callback method that is invoked for every msg received
   */
  EventLoop(EnvOptions env_options,
            int port,
            const std::shared_ptr<Logger>& info_log,
            EventCallbackType event_callback,
            CommandCallbackType command_callback = nullptr);

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

 private:
  friend class SocketEvent;

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
  CommandCallbackType command_callback_;

  // The callback context
  EventCallbackContext event_callback_context_;

  // The connection listener
  evconnlistener* listener_ = nullptr;

  // Shutdown event
  struct event* shutdown_event_ = nullptr;

  // Command pipe and event
  int command_pipe_fds_[2];  // read, write
  struct event* command_pipe_event_ = nullptr;

  // a cache of HostId to connections
  std::map<HostId, SocketEvent*> connection_cache_;

  // connection cache updates
  bool insert_connection_cache(const HostId& host, SocketEvent* ev);
  bool remove_connection_cache(const HostId& host, SocketEvent* ev);
  SocketEvent* lookup_connection_cache(const HostId& host) const;
  SocketEvent* setup_connection(const HostId& host);
  Status create_connection(const HostId& host, bool block, int* fd);

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
