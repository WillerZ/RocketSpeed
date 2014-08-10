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
#include <string>
#include <vector>

#include "src/util/testharness.h"
#include "src/include/controltower_client.h"
#include "src/controltower/controltower.h"

namespace rocketspeed {

class ControlMessages {
 public:
  // Create a new instance of the control tower
  ControlMessages():
    env_(Env::Default()), ct_(nullptr), started_(false) {
    char myname[1024];
    st_ = ControlTower::CreateNewInstance(options_, conf_, &ct_);

    // what is my machine name?
    ASSERT_EQ(gethostname(&myname[0], sizeof(myname)), 0);
    hostname_.assign(myname);

    // enable all kinds of libevent debugging
    ld_event_enable_debug_logging(EVENT_DBG_ALL);
    ld_event_set_log_callback(dump_libevent_cb);
    ld_event_enable_debug_mode();
  }

  virtual ~ControlMessages() {
    // deleting the ControlTower shuts down the event disptach loop.
    delete ct_;
    env_->WaitForJoin();  // This is good hygine
  }

  // Setup dispatch thread and ensure that it is running.
  Status ControlTowerRun() {
    // If there was an error in instantiating the ControlTower earlier.
    // then return error immediately.
    if (!st_.ok()) {
      return st_;
    }

    // If the control tower has not already been started, then start it
    if (!started_) {
      env_->StartThread(ControlMessages::ControlTowerStart, ct_);
    }
    started_ = true;

    // Wait till the background thread has setup the dispatch loop
    while (!ct_->IsRunning()) {
      env_->SleepForMicroseconds(1000);
    }
    return Status::OK();
  }

 protected:
  Env* env_;
  ControlTower* ct_;
  bool started_;
  ControlTowerOptions options_;
  Configuration conf_;
  Status st_;
  std::string hostname_;

  // A static method that is the entry point of a background thread
  static void ControlTowerStart(void* arg) {
    ControlTower* ct = reinterpret_cast<ControlTower*>(arg);
    ct->Run();
  }

  // Dumps libevent info messages to stdout
  static void
  dump_libevent_cb(int severity, const char* msg) {
    const char* s;
    switch (severity) {
      case _EVENT_LOG_DEBUG: s = "dbg"; break;
      case _EVENT_LOG_MSG:   s = "msg";   break;
      case _EVENT_LOG_WARN:  s = "wrn";  break;
      case _EVENT_LOG_ERR:   s = "err"; break;
      default:               s = "?";     break; /* never reached */
    }
    printf("[%s] %s\n", s, msg);
  }
};


TEST(ControlMessages, Subscribe) {
  SequenceNumber seqno = 100;
  ControlTowerOptions ct_options;
  std::vector<TopicPair> topics;
  int num_topics = 5;
  HostId hostid(hostname_, options_.port_number);

  // create a few topics
  for (int i = 0; i < num_topics; i++)  {
    // alternate between types
    MetadataType type = (i % 2 == 0 ? mSubscribe : mUnSubscribe);
    topics.push_back(TopicPair(std::to_string(i), type));
  }
  //
  // create a message
  MessageMetadata meta1(Tenant::Guest, seqno, hostid, topics);

  // create a ControlTower (if not already created)
  ASSERT_EQ(ControlTowerRun().ok(), true);

  // create a client to communicate with the ControlTower
  ControlTowerClient* client = nullptr;
  Status s1 = ControlTowerClient::CreateNewInstance(*env_, conf_,
                hostname_, options_.port_number, nullptr, &client);
  ASSERT_EQ(s1.ok(), true);

  // send message to control tower
  ASSERT_EQ(client->Send(meta1).ok(), true);

  // free up resources
  delete client;
}
}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
