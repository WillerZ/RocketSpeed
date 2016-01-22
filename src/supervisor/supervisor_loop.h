// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#include <sys/socket.h>
#include <netinet/in.h>

#include "src/port/port.h"
#include "include/HostId.h"
#include "src/util/common/thread_check.h"
#include "src/supervisor/options.h"
#include <map>

struct event;
struct event_base;
struct evconnlistener;
struct sockaddr;
struct bufferevent;
struct evbuffer;

namespace rocketspeed {

class Status;
class SupervisorCommand;

/**
 * SupervisorLoop's task is to allow external access to the running RS instance
 * for some functionalities like fetching stats, changing the log level, ...
 * We're going to listen on the port passed in options and execute the commands.
 *
 * Example usage on the running local instance:
 *  nc localhost 58800
 *  help
 *  stats pilot
 *  set_log_level info
 *  ^C
 */
class SupervisorLoop {
 public:

  // The map containing all the supported commands & their callbacks
  static std::map<std::string, SupervisorCommand> commands_;

  static const uint32_t DEFAULT_PORT = 58800;

  static Status CreateNewInstance(SupervisorOptions options,
                                  std::unique_ptr<SupervisorLoop>* supervisor);
  // Initialize  supervisor event loop
  Status Initialize();
  // Blocking call to start the supervisor event loop
  void Run();
  // Returns whether or not the supervisor event loop is running
  bool IsRunning() const;
  // Stops supervisor event loop
  void Stop();
  // Retrieves bound address.
  const HostId& GetHostId() const { return host_id_; }

  Status WaitUntilRunning(std::chrono::seconds timeout =
                            std::chrono::seconds(10));

  std::string ExecuteCommand(std::string cmd);

 private:
  explicit SupervisorLoop(SupervisorOptions opts);

  // libevent event callbacks
  static void AcceptCallback(evconnlistener* listener,
                             int fd,
                             sockaddr* address,
                             int socklen,
                             void* arg);
  static void StartCallback(int listener, short event, void* arg);
  static void ShutdownCallback(int listener, short event, void* arg);
  static void CommandCallback(bufferevent *bev, void *arg);
  static void ErrorCallback(bufferevent *bev, short error, void *arg);

  // Passed options
  SupervisorOptions options_;

  // The supervisor's listener address.
  HostId host_id_;

  // used for timed wait until supervisor starts
  port::Semaphore start_signal_;

  // Thread check
  ThreadCheck thread_check_;

  // Is supervisor loop set up and running
  std::atomic<bool> running_;

  // The event loop base.
  event_base* base_ = nullptr;

  // The connection listener
  evconnlistener* listener_ = nullptr;

  // Startup event
  event* startup_event_ = nullptr;

  // Shutdown event
  event* shutdown_event_ = nullptr;
  rocketspeed::port::Eventfd shutdown_eventfd_;
};

}
