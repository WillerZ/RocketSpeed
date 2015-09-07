// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "src/supervisor/supervisor_loop.h"
#include "include/Status.h"
#include "include/Logger.h"

#include "src/util/common/parsing.h"

#include "src/controltower/tower.h"
#include "src/copilot/copilot.h"
#include "src/pilot/pilot.h"

#include "src/messages/event2_version.h"
#include "src/messages/msg_loop.h"
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>
#include <thread>

namespace rocketspeed {

Status SupervisorLoop::CreateNewInstance(SupervisorOptions options,
  std::unique_ptr<SupervisorLoop>* supervisor) {
  // Need to check if we can successfully bind to port
  *supervisor =
    std::unique_ptr<SupervisorLoop>(new SupervisorLoop(std::move(options)));
  return Status::OK();
}

SupervisorLoop::SupervisorLoop(SupervisorOptions opts) :
  options_(opts),
  shutdown_eventfd_(rocketspeed::port::Eventfd(true, true)) {
}

void SupervisorLoop::Run() {
   if (!base_) {
    LOG_FATAL(options_.info_log, "SupervisorLoop not initialized before use.");
    assert(false);
    return;
  }
  LOG_VITAL(options_.info_log,
            "Starting SupervisorLoop at port %d",
            options_.port);
  options_.info_log->Flush();

  thread_check_.Reset();
  // Start event loop.
  // Will not exit until Stop is called, or some error occurs in libevent.
  event_base_dispatch(base_);
  running_ = false;
}

Status SupervisorLoop::Initialize() {
  if (base_) {
    assert(false);
    return Status::InvalidArgument("SupervisorLoop already initialized.");
  }

  // Create event base
  base_ = event_base_new();
  if (!base_) {
    return Status::InternalError("Event base could not be created");
  }

  // Create listener
  sockaddr_in6 sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin6_family = AF_INET6;
  sin.sin6_addr = in6addr_any;
  sin.sin6_port = htons(static_cast<uint16_t>(options_.port));

  listener_ = evconnlistener_new_bind(
    base_,
    SupervisorLoop::AcceptCallback,
    reinterpret_cast<void*>(this),
    LEV_OPT_CLOSE_ON_FREE|LEV_OPT_REUSEABLE,
    -1, //backlog
    reinterpret_cast<sockaddr*>(&sin),
    static_cast<int>(sizeof(sin)));

  if (!listener_) {
    LOG_ERROR(options_.info_log,
              "Supervisor failed to be started on port %d",
              options_.port);
    return Status::InternalError("Listener could not be created");
  }

  // Create events

  // Create a non-persistent event that will run as soon as the dispatch
  // loop is run. This is the first event to ever run on the dispatch loop.
  // The firing of this artificial event indicates that the event loop
  // is up and running.
  startup_event_ = evtimer_new(
    base_,
    SupervisorLoop::StartCallback,
    reinterpret_cast<void*>(this));

  if (!startup_event_) {
    return Status::InternalError("Startup event could not be created");
  }

  timeval zero_seconds = {0, 0};
  int rv = evtimer_add(startup_event_, &zero_seconds);
  if (rv) {
    return Status::InternalError("Failed to add startup event to event base");
  }

  // An event that signals the shutdown of the supervisor loop
  if (shutdown_eventfd_.status() < 0) {
    return Status::InternalError(
      "Failed to create eventfd for shutdown commands");
  }

  shutdown_event_ = event_new(
    base_,
    shutdown_eventfd_.readfd(),
    EV_PERSIST|EV_READ,
    SupervisorLoop::ShutdownCallback,
    reinterpret_cast<void*>(this));

  if (!shutdown_event_) {
    return Status::InternalError("Shutdown event could not be created");
  }
  rv = event_add(shutdown_event_, nullptr);
  if (rv != 0) {
    return Status::InternalError("Failed to add shutdown event to event base");
  }

  return Status::OK();
}

bool SupervisorLoop::IsRunning() const {
  return running_;
}

void SupervisorLoop::Stop() {
  if (base_ != nullptr) {
    if (running_) {
      // Write to the shutdown event FD to signal the event loop thread
      // to shutdown and stop looping.
      int result;
      do {
        result = shutdown_eventfd_.write_event(1);
      } while (running_ && (result < 0 || errno == EAGAIN));

      // Wait for event loop to exit on the loop thread.
      while (running_) {
        std::this_thread::yield();
      }
    }

    // Shutdown everything
    if (listener_) {
      evconnlistener_free(listener_);
    }
    if (startup_event_) event_free(startup_event_);
    if (shutdown_event_) event_free(shutdown_event_);
    event_base_free(base_);
    shutdown_eventfd_.closefd();

    // Reset the thread checker since the event loop thread is exiting.
    thread_check_.Reset();
    LOG_VITAL(options_.info_log,
              "Stopped Supervisor at port %d",
              options_.port);
    options_.info_log->Flush();
    base_ = nullptr;
  }
}

void SupervisorLoop::CommandCallback(bufferevent *bev, void *arg) {
  SupervisorLoop* obj = static_cast<SupervisorLoop *>(arg);
  evbuffer *input, *output;
  char *line;
  size_t n;
  input = bufferevent_get_input(bev);
  output = bufferevent_get_output(bev);

  line = evbuffer_readln(input, &n, EVBUFFER_EOL_LF);
  if (!line) {
    return;
  }
  std::string cmd(line);
  free(line);
  std::string response = obj->ExecuteCommand(cmd);
  evbuffer_add_printf(output, "%s\n", response.c_str());
}

class SupervisorCommand {
 public:
  SupervisorCommand(std::string key,
                    std::string desc,
                    std::function<std::string(std::vector<std::string>,
                                              SupervisorLoop*)> f) :
    key_(key),
    desc_(desc),
    f_(f) {
  }

  std::string operator()(const std::vector<std::string>& args,
                         SupervisorLoop* supervisor) const {
    assert(supervisor != nullptr);
    assert(args.size() > 0 && args[0] == key_);
    return f_(args, supervisor);
  }
  std::string GetDesc() const { return key_ + " : " + desc_; }

 private:
  std::string key_;
  std::string desc_;
  std::function<std::string(std::vector<std::string>, SupervisorLoop*)> f_;
};

std::map<std::string, SupervisorCommand> SupervisorLoop::commands_ = {
  {
    "help",
    SupervisorCommand(
      "help",
      "Print the help about using the Supervisor",

      [](std::vector<std::string> args, SupervisorLoop* supervisor)
        -> std::string {
        std::string result;
        for (const auto& cmd: commands_) {
          result += cmd.second.GetDesc() + '\n';
        }
        return result;
      }
    )
  },
  {
    "stats",
    SupervisorCommand(
      "stats",
      "Get the stats for pilot/copilot/tower. Example: 'stats pilot'",

      [](std::vector<std::string> args, SupervisorLoop* supervisor)
        -> std::string {

        if (args.size() != 2) {
          return "Invalid command";
        } else {
          std::string response("Invalid command");
          const SupervisorOptions& options = supervisor->options_;
          if (args[1] == "pilot" && options.pilot != nullptr) {
            Statistics stats = options.pilot->GetStatisticsSync();
            stats.Aggregate(options.pilot->GetMsgLoop()->GetStatisticsSync());
            response = stats.Report();
          } else if (args[1] == "copilot" && options.copilot != nullptr) {
            Statistics stats = options.copilot->GetStatisticsSync();
            stats.Aggregate(options.copilot->GetMsgLoop()->GetStatisticsSync());
            response = stats.Report();
          } else if (args[1] == "tower" && options.tower != nullptr) {
            Statistics stats = options.tower->GetStatisticsSync();
            stats.Aggregate(options.tower->GetMsgLoop()->GetStatisticsSync());
            response = stats.Report();
          }
          return response;
        }
      }
    )
  },
  {
    "set_log_level",
    SupervisorCommand(
      "set_log_level",
      "Sets the log level on the running instance. Example: set_log_level info",

      [](std::vector<std::string> args, SupervisorLoop* supervisor)
        -> std::string {

        if (args.size() != 2) {
          return "Invalid command";
        } else {
          if (IsValidLogLevel(args[1])) {
            auto log_level = StringToLogLevel(args[1].c_str());
            supervisor->options_.info_log->SetInfoLogLevel(log_level);
          } else {
            return "Trying to set invalid log level";
          }
          return "Set log level to " + args[1];
        }
      }
    )
  },
  {
    "info",
    SupervisorCommand(
      "info",
      "info tower cache usage \n"
      "info tower cache capacity\n"
      "info tower log N\n"
      "info tower logs\n"
      "info tower tail_seqno N\n"
      "info copilot subscriptions FILTER [MAX]\n"
      "info copilot towers_for_log N\n"
      "info copilot log_for_topic NAMESPACE TOPIC\n",
      [](std::vector<std::string> args, SupervisorLoop* supervisor)
        -> std::string {

        if (args.size() < 2) {
          return "Invalid command";
        }
        std::string handler = args[1];
        args.erase(args.begin(), args.begin() + 2);
        if (handler == "pilot" && supervisor->options_.pilot) {
          return supervisor->options_.pilot->GetInfoSync(std::move(args));
        } else if (handler == "copilot" && supervisor->options_.copilot) {
          return supervisor->options_.copilot->GetInfoSync(std::move(args));
        } else if (handler == "tower" && supervisor->options_.tower) {
          return supervisor->options_.tower->GetInfoSync(std::move(args));
        } else {
          return "Invalid command";
        }
      }
    )
  },
  {
    "set",
    SupervisorCommand(
      "set",
      "set tower cache clear \n"
      "set tower cache capacity\n",
      [](std::vector<std::string> args, SupervisorLoop* supervisor)
        -> std::string {

        if (args.size() < 2) {
          return "Invalid command";
        }
        std::string handler = args[1];
        args.erase(args.begin(), args.begin() + 2);
        if (handler == "tower" && supervisor->options_.tower) {
          return supervisor->options_.tower->SetInfoSync(std::move(args));
        } else {
          return "Invalid command";
        }
      }
    )
  }
};

std::string SupervisorLoop::ExecuteCommand(std::string cmd) {
  std::vector<std::string> args = SplitString(cmd, ' ');
  if (args.empty()) {
    return "Invalid command";
  }
  auto it = commands_.find(args[0]);
  if (it != commands_.end()) {
    return (it->second)(args, this);
  }
  return "Invalid command";
}

// Libevent callbacks

// This callback is fired from the first aritificial timer event
// in the dispatch loop.
void SupervisorLoop::StartCallback(int listener, short event, void* arg) {
  SupervisorLoop* obj = static_cast<SupervisorLoop *>(arg);
  obj->thread_check_.Check();
  obj->running_ = true;
  obj->start_signal_.Post();
}

void SupervisorLoop::ShutdownCallback(evutil_socket_t listener,
                                      short event,
                                      void *arg) {
  SupervisorLoop* obj = static_cast<SupervisorLoop *>(arg);
  obj->thread_check_.Check();
  event_base_loopexit(obj->base_, nullptr);
}

void SupervisorLoop::AcceptCallback(evconnlistener *listener,
                                   evutil_socket_t fd,
                                   sockaddr *address,
                                   int socklen,
                                   void *arg) {
  SupervisorLoop* supervisor_loop = static_cast<SupervisorLoop *>(arg);
  supervisor_loop->thread_check_.Check();

  // make socket non-blocking
  if (evutil_make_socket_nonblocking(fd)) {
    LOG_WARN(supervisor_loop->options_.info_log,
             "Unable to make socket non-blocking for the Supervisor");
  }
  bufferevent* bev = bufferevent_socket_new(supervisor_loop->base_,
                                            fd,
                                            BEV_OPT_CLOSE_ON_FREE);
  bufferevent_setcb(bev,
                    SupervisorLoop::CommandCallback,
                    nullptr,
                    SupervisorLoop::ErrorCallback,
                    supervisor_loop);
  bufferevent_setwatermark(bev, EV_READ, 0, 1 << 12);
#if LIBEVENT_VERSION_NUMBER >= 0x02010300
  bufferevent_set_max_single_write(bev, 1 << 20);  // 1MB ought to be enough
#endif
  bufferevent_enable(bev, EV_READ|EV_WRITE);
}

void SupervisorLoop::ErrorCallback(bufferevent *bev, short error, void *arg) {
  SupervisorLoop* obj = static_cast<SupervisorLoop *>(arg);
  obj->thread_check_.Check();

  if (error & BEV_EVENT_EOF) {
    LOG_INFO(obj->options_.info_log, "Supervisor: connection closed");
  } else if (error & BEV_EVENT_ERROR) {
    LOG_INFO(obj->options_.info_log, "Supervisor: bev event error");
  } else if (error & BEV_EVENT_TIMEOUT) {
    LOG_INFO(obj->options_.info_log, "Supervisor: timeout event");
  }

  bufferevent_free(bev);
}

Status SupervisorLoop::WaitUntilRunning(std::chrono::seconds timeout) {
  if (!running_) {
    if (!start_signal_.TimedWait(timeout)) {
      return Status::TimedOut();
    }
  }
  return Status::OK();
}

}  // namespace rocketspeed
