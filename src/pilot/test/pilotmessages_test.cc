//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <unistd.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/thread.h>
#include <event2/util.h>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "src/pilot/pilot.h"
#include "src/util/logdevice.h"
#include "src/util/testharness.h"

namespace rocketspeed {

class PilotTest {
 public:
  // Create a new instance of the pilot
  PilotTest():
    env_(Env::Default()), pilot_(nullptr), started_(false) {
    // Create Pilot
    LogDeviceStorage* storage = nullptr;
    std::unique_ptr<facebook::logdevice::ClientSettings> clientSettings(
      facebook::logdevice::ClientSettings::create());
    rocketspeed::LogDeviceStorage::Create(
      "",
      "",
      "",
      std::chrono::milliseconds(5000),
      std::move(clientSettings),
      rocketspeed::Env::Default(),
      &storage);
    options_.log_count = 1;
    options_.log_storage = std::unique_ptr<rocketspeed::LogStorage>(storage);
    st_ = Pilot::CreateNewInstance(std::move(options_), conf_, &pilot_);

    // what is my machine name?
    char myname[1024];
    ASSERT_EQ(gethostname(&myname[0], sizeof(myname)), 0);
    hostname_.assign(myname);

    // enable all kinds of libevent debugging
    ld_event_enable_debug_logging(EVENT_DBG_ALL);
    ld_event_set_log_callback(dump_libevent_cb);
    ld_event_enable_debug_mode();
  }

  virtual ~PilotTest() {
    // deleting the Pilot shuts down the event disptach loop.
    delete pilot_;
    env_->WaitForJoin();  // This is good hygine
  }

  // Setup dispatch thread and ensure that it is running.
  Status PilotRun() {
    // If there was an error in instantiating the Pilot earlier.
    // then return error immediately.
    if (!st_.ok()) {
      return st_;
    }

    // If the pilot has not already been started, then start it
    if (!started_) {
      env_->StartThread(PilotTest::PilotStart, pilot_);
    }
    started_ = true;

    // Wait till the background thread has setup the dispatch loop
    while (!pilot_->IsRunning()) {
      env_->SleepForMicroseconds(1000);
    }
    return Status::OK();
  }

  // Returns the logger that logs into the LOG file
  std::shared_ptr<Logger> GetLogger() {
    return pilot_->GetOptions().info_log;
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  Pilot* pilot_;
  bool started_;
  PilotOptions options_;
  Configuration conf_;
  Status st_;
  std::string hostname_;

  // A static method that is the entry point of a background thread
  static void PilotStart(void* arg) {
    Pilot* pilot = reinterpret_cast<Pilot*>(arg);
    pilot->Run();
  }

  // A static method that is the entry point of a background MsgLoop
  static void MsgLoopStart(void* arg) {
    MsgLoop* loop = reinterpret_cast<MsgLoop*>(arg);
    loop->Run();
  }

  // Dumps libevent info messages to stdout
  static void
  dump_libevent_cb(int severity, const char* msg) {
    const char* s;
    switch (severity) {
      case _EVENT_LOG_DEBUG: s = "dbg"; break;
      case _EVENT_LOG_MSG:   s = "msg"; break;
      case _EVENT_LOG_WARN:  s = "wrn"; break;
      case _EVENT_LOG_ERR:   s = "err"; break;
      default:               s = "?";   break; /* never reached */
    }
    printf("[%s] %s\n", s, msg);
  }
};

TEST(PilotTest, Publish) {
  // create a Pilot (if not already created)
  ASSERT_EQ(PilotRun().ok(), true);

  // create a client to communicate with the ControlTower
  HostId clientId(hostname_, options_.port_number + 1);
  std::map<MessageType, MsgCallbackType> client_callback;
  MsgLoop loop(env_,
               env_options_,
               clientId,
               GetLogger(),
               static_cast<ApplicationCallbackContext>(this),
               client_callback);
  env_->StartThread(PilotTest::MsgLoopStart, &loop);
  while (!loop.IsRunning()) {
    env_->SleepForMicroseconds(1000);
  }

  // send messages to pilot
  HostId pilot(hostname_, options_.port_number);
  for (int i = 0; i < 100; ++i) {
    std::string payload = std::to_string(i);
    std::string topic = "test";
    MessageData msg(Tenant::Guest, Slice(topic), Slice(payload));
    ASSERT_EQ(loop.GetClient().Send(pilot, &msg).ok(), true);
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
