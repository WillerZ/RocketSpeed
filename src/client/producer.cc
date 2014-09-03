// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <chrono>
#include <memory>
#include <set>
#include <thread>
#include "include/RocketSpeed.h"
#include "include/Env.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "src/util/mutexlock.h"

namespace rocketspeed {

Producer::~Producer() {
}

// Internal implementation of the Producer API.
class ProducerImpl : public Producer {
 public:
  virtual ~ProducerImpl();

  virtual ResultStatus Publish(const Topic& name,
                               const TopicOptions& options,
                               const Slice& data);

  ProducerImpl(const HostId& pilot_host_id,
               TenantID tenant_id,
               int port);

 private:
  // Callback for MessageDataAck message.
  static void ProcessDataAck(const ApplicationCallbackContext arg,
                             std::unique_ptr<Message> msg);

  // HostId of this machine, i.e. the one the producer is running on.
  HostId host_id_;

  // HostId of pilot machine to send messages to.
  HostId pilot_host_id_;

  // Tenant ID of this producer.
  TenantID tenant_id_;

  // Incoming message loop object.
  std::unique_ptr<MsgLoop> msg_loop_ = nullptr;
  std::thread msg_loop_thread_;

  // Mutex and condition variable for ack synchronization
  port::Mutex ack_mutex_;
  port::CondVar ack_cv_;

  // Set of messages that we've received acks for.
  std::set<MsgId> acked_msgs_;
};

// Implementation of Producer::Open from RocketSpeed.h
Status Producer::Open(const Configuration* config,
                      Producer** producer) {
  // Validate arguments.
  if (config == nullptr) {
    return Status::InvalidArgument("config must not be null.");
  }
  if (producer == nullptr) {
    return Status::InvalidArgument("producer must not be null.");
  }
  if (config->GetPilotHostIds().empty()) {
    return Status::InvalidArgument("Must have at least one pilot.");
  }

  // Construct new Producer client.
  // TODO(pja) 1 : Just using first pilot for now, should use some sort of map.
  *producer = new ProducerImpl(config->GetPilotHostIds().front(),
                               config->GetTenantID(),
                               config->GetLocalPort());
  return Status::OK();
}

ProducerImpl::ProducerImpl(const HostId& pilot_host_id,
                           TenantID tenant_id,
                           int port)
: pilot_host_id_(pilot_host_id)
, tenant_id_(tenant_id)
, ack_cv_(&ack_mutex_) {
  // Initialise host_id_.
  char myname[1024];
  if (gethostname(&myname[0], sizeof(myname))) {
    assert(false);
  }
  host_id_ = HostId(myname, port);

  // Setup callbacks.
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mDataAck] = MsgCallbackType(ProcessDataAck);

  // Construct message loop.
  msg_loop_.reset(new MsgLoop(Env::Default(),
                              EnvOptions(),
                              host_id_,
                              std::make_shared<NullLogger>(),  // no logging
                              static_cast<ApplicationCallbackContext>(this),
                              callbacks));

  msg_loop_thread_ = std::thread([this] () {
    msg_loop_->Run();
  });

  while (!msg_loop_->IsRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

ProducerImpl::~ProducerImpl() {
  // Set msg_loop_ to null to stop the event loop.
  msg_loop_.reset(nullptr);

  // Wait for thread to join.
  msg_loop_thread_.join();
}

ResultStatus ProducerImpl::Publish(const Topic& name,
                                   const TopicOptions& options,
                                   const Slice& data) {
  // Construct message.
  MessageData message(tenant_id_,
                      host_id_,
                      Slice(name),
                      data,
                      options.retention);

  int retries = 4;
  int64_t timeout_us = 1000000;  // 1 second
  do {
    // Attempt to (re)send.
    Status status = msg_loop_->GetClient().Send(pilot_host_id_, &message);
    if (!status.ok()) {
      return ResultStatus(status, 0);
    }

    // Lambda for convenience.
    auto time_now = []() {
      return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    };

    // Wait for ack message.
    MutexLock lock(&ack_mutex_);
    auto deadline = time_now() + timeout_us;
    do {
      ack_cv_.TimedWait(deadline);

      // Check for ack in acked_msgs_.
      if (acked_msgs_.erase(message.GetMessageId())) {
        return ResultStatus(Status::OK(), 0);
      }

      // If we aren't past the deadline then it was a spurious wakeup,
      // so wait again.
    } while (deadline > time_now());

    timeout_us *= 2;  // exponential backoff.
  } while (retries--);

  return ResultStatus(Status::TimedOut(), 0);
}

void ProducerImpl::ProcessDataAck(const ApplicationCallbackContext arg,
                                  std::unique_ptr<Message> msg) {
  ProducerImpl* self = static_cast<ProducerImpl*>(arg);
  const MessageDataAck* ackMsg = static_cast<const MessageDataAck*>(msg.get());

  // Lock the ack mutex and add msg to the acked_msgs_ set for each
  // successful ack.
  MutexLock lock(&self->ack_mutex_);
  for (const auto& ack : ackMsg->GetAcks()) {
    if (ack.status == MessageDataAck::AckStatus::Success) {
      self->acked_msgs_.insert(ack.msgid);
    } else {
      // Failure ack. We treat this the same as no ack for now.
    }
  }

  // App will be waiting inside Publish for the ack, notify now.
  self->ack_cv_.Signal();
}

}  // namespace rocketspeed
