//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include <chrono>
#include <memory>

#include "include/RocketSpeed.h"
#include "src/engine/rocketeer.h"
#include "src/engine/rocketeer_server.h"
#include "src/messages/msg_loop.h"
#include "src/messages/messages.h"
#include "src/port/Env.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class RocketeerTest {
 public:
  RocketeerTest()
  : positive_timeout(1000), negative_timeout(100), env_(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env_, "RocketeerTest", &info_log_));
    RocketeerOptions options;
    options.info_log = info_log_;
    options.env = env_;
    options.port = 0;
    server_.reset(new RocketeerServer(std::move(options)));
  }

  virtual ~RocketeerTest() {
    server_.reset();
    env_->WaitForJoin();
  }

 protected:
  typedef std::chrono::steady_clock TestClock;

  const std::chrono::milliseconds positive_timeout;
  const std::chrono::milliseconds negative_timeout;
  Env* const env_;
  std::shared_ptr<rocketspeed::Logger> info_log_;

  std::unique_ptr<RocketeerServer> server_;

  class ClientMock {
   public:
    std::unique_ptr<MsgLoop> msg_loop;

    // Noncopyable, movable
    ClientMock(ClientMock&&) = default;
    ClientMock& operator=(ClientMock&&) = default;

    ClientMock(std::unique_ptr<MsgLoop> _msg_loop, std::thread msg_loop_thread)
    : msg_loop(std::move(_msg_loop))
    , msg_loop_thread_(std::move(msg_loop_thread)) {}

    ~ClientMock() {
      msg_loop->Stop();
      if (msg_loop_thread_.joinable()) {
        msg_loop_thread_.join();
      }
    }

   private:
    std::thread msg_loop_thread_;
  };

  ClientMock MockClient(
      const std::map<MessageType, MsgCallbackType>& callbacks) {
    std::unique_ptr<MsgLoop> client(
        new MsgLoop(env_, EnvOptions(), 0, 1, info_log_, "client"));
    client->RegisterCallbacks(callbacks);
    ASSERT_OK(client->Initialize());
    std::thread thread([&]() { client->Run(); });
    ASSERT_OK(client->WaitUntilRunning());
    return ClientMock(std::move(client), std::move(thread));
  }
};

struct SubscribeUnsubscribe : public Rocketeer {
  bool is_set_ = false;
  InboundID inbound_id_;
  port::Semaphore terminate_sem_;

  void HandleNewSubscription(InboundID inbound_id,
                             SubscriptionParameters params) {
    ASSERT_TRUE(!is_set_);
    is_set_ = true;
    inbound_id_ = inbound_id;
  }

  void HandleTermination(InboundID inbound_id, TerminationSource source) {
    ASSERT_TRUE(TerminationSource::Subscriber == source);
    ASSERT_TRUE(is_set_);
    ASSERT_TRUE(inbound_id_ == inbound_id);
    terminate_sem_.Post();
  }
};

TEST(RocketeerTest, SubscribeUnsubscribe) {
  SubscribeUnsubscribe rocketeer;
  server_->Register(&rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetMsgLoop()->GetHostId();

  auto client = MockClient(std::map<MessageType, MsgCallbackType>());
  auto socket = client.msg_loop->CreateOutboundStream(server_addr, 0);

  // Subscribe.
  MessageSubscribe subscribe(
      GuestTenant, GuestNamespace, "SubscribeUnsubscribe", 101, 2);
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));
  // Send again, to verify that only one will be delivered.
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));

  // Send some broken unsubscribe, that doesn't match anything.
  MessageUnsubscribe unsubscribe1(
      GuestTenant, 1, MessageUnsubscribe::Reason::kRequested);
  ASSERT_OK(client.msg_loop->SendRequest(unsubscribe1, &socket, 0));

  // Send valid unsubscribe.
  MessageUnsubscribe unsubscribe(
      GuestTenant, 2, MessageUnsubscribe::Reason::kRequested);
  ASSERT_OK(client.msg_loop->SendRequest(unsubscribe, &socket, 0));

  ASSERT_TRUE(rocketeer.terminate_sem_.TimedWait(positive_timeout));

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

struct SubscribeTerminate : public Rocketeer {
  InboundID inbound_id_;
  port::Semaphore terminate_sem_;

  void HandleNewSubscription(InboundID inbound_id,
                             SubscriptionParameters params) {
    inbound_id_ = inbound_id;
    Terminate(inbound_id, Rocketeer::UnsubscribeReason::BackOff);
  }

  void HandleTermination(InboundID inbound_id, TerminationSource source) {
    ASSERT_TRUE(TerminationSource::Rocketeer == source);
    ASSERT_TRUE(inbound_id_ == inbound_id);
    terminate_sem_.Post();
  }
};

TEST(RocketeerTest, SubscribeTerminate) {
  SubscribeTerminate rocketeer;
  server_->Register(&rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetMsgLoop()->GetHostId();

  port::Semaphore unsubscribe_sem;
  auto client = MockClient({
      {MessageType::mUnsubscribe,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto unsubscribe = static_cast<MessageUnsubscribe*>(msg.get());
         ASSERT_TRUE(unsubscribe->GetReason() ==
                     MessageUnsubscribe::Reason::kBackOff);
         unsubscribe_sem.Post();
       }},
  });
  auto socket = client.msg_loop->CreateOutboundStream(server_addr, 0);

  // Subscribe.
  MessageSubscribe subscribe(
      GuestTenant, GuestNamespace, "SubscribeTerminate", 101, 2);
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));

  // Wait for unsubscribe message.
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
  // Rocketeer should also be called.
  ASSERT_TRUE(rocketeer.terminate_sem_.TimedWait(positive_timeout));

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

struct Noop : public Rocketeer {
  Rocketeer* rocketeer_;
  explicit Noop(Rocketeer* rocketeer) {
    rocketeer->SetBelowRocketeer(this);
    rocketeer_ = rocketeer;
  }

  void HandleNewSubscription(InboundID inbound_id,
                             SubscriptionParameters params) {
    rocketeer_->HandleNewSubscription(inbound_id, params);
  }

  void HandleTermination(InboundID inbound_id, TerminationSource source) {
    rocketeer_->HandleTermination(inbound_id, source);
  }
};

struct TopOfStack : public Rocketeer {
  const std::string deliver_msg_ = "RandomMessage";
  const SequenceNumber deliver_msg_seqno_ = 102;
  const SequenceNumber advance_seqno_ = 112;
  port::Semaphore terminate_sem_;
  InboundID inbound_id_;

  void HandleNewSubscription(InboundID inbound_id,
                             SubscriptionParameters params) {
    inbound_id_ = inbound_id;
    Deliver(inbound_id, deliver_msg_seqno_, deliver_msg_);
    Advance(inbound_id, advance_seqno_);
    Terminate(inbound_id, Rocketeer::UnsubscribeReason::BackOff);
  }

  void HandleTermination(InboundID inbound_id, TerminationSource source) {
    ASSERT_TRUE(TerminationSource::Rocketeer == source);
    ASSERT_TRUE(inbound_id_ == inbound_id);
    terminate_sem_.Post();
  }
};

TEST(RocketeerTest, StackRocketeerTest) {
  TopOfStack topRocketeer;
  Noop* rocketeer = new Noop(new Noop(new Noop(&topRocketeer)));
  server_->Register(rocketeer);
  ASSERT_OK(server_->Start());
  auto server_addr = server_->GetMsgLoop()->GetHostId();

  port::Semaphore unsubscribe_sem;
  port::Semaphore deliver_sem;
  port::Semaphore advance_sem;

  // Ensure that HandleSubscription and HandleTermination calls go up the stack
  // and Deliver/Advance/Terminate go down the stack.

  auto client = MockClient({
      {MessageType::mUnsubscribe,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         unsubscribe_sem.Post();
       }},
      {MessageType::mDeliverData,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto data = static_cast<MessageDeliverData*>(msg.get());
         ASSERT_TRUE(topRocketeer.deliver_msg_ ==
                     data->GetPayload().ToString());
         ASSERT_TRUE(topRocketeer.deliver_msg_seqno_ ==
                     data->GetSequenceNumber());
         deliver_sem.Post();
       }},
      {MessageType::mDeliverGap,
       [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream_id) {
         auto data = static_cast<MessageDeliverGap*>(msg.get());
         ASSERT_TRUE(data->GetFirstSequenceNumber() ==
                     topRocketeer.deliver_msg_seqno_);
         ASSERT_TRUE(data->GetLastSequenceNumber() ==
                     topRocketeer.advance_seqno_);
         advance_sem.Post();
       }},

  });
  auto socket = client.msg_loop->CreateOutboundStream(server_addr, 0);

  // Subscribe.
  MessageSubscribe subscribe(GuestTenant, GuestNamespace, "stackable", 101, 2);
  ASSERT_OK(client.msg_loop->SendRequest(subscribe, &socket, 0));

  ASSERT_TRUE(deliver_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(advance_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(unsubscribe_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(topRocketeer.terminate_sem_.TimedWait(positive_timeout));

  // Stop explicitly, as the Rocketeer is destroyed before the Server.
  server_->Stop();
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
