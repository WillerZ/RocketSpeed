// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/client/client.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "client_commands.h"
#include "message_received.h"

namespace rocketspeed {

Client::~Client() {
}

// Implementation of Client::Open from RocketSpeed.h
Status Client::Open(const Configuration* config,
                    PublishCallback publish_callback,
                    SubscribeCallback subscription_callback,
                    MessageReceivedCallback receive_callback,
                    Client** producer) {
  // Validate arguments.
  if (config == nullptr) {
    return Status::InvalidArgument("config must not be null.");
  }
  if (producer == nullptr) {
    return Status::InvalidArgument("producer must not be null.");
  }
  if (config->GetTenantID() <= 100) {
    return Status::InvalidArgument("TenantId must be greater than 100.");
  }
  if (config->GetPilotHostIds().empty()) {
    return Status::InvalidArgument("Must have at least one pilot.");
  }

  // Construct new Client client.
  // TODO(pja) 1 : Just using first pilot for now, should use some sort of map.
  *producer = new ClientImpl(config->GetPilotHostIds().front(),
                             config->GetCopilotHostIds().front(),
                             config->GetTenantID(),
                             config->GetClientPort(),
                             publish_callback,
                             subscription_callback,
                             receive_callback,
                             std::make_shared<NullLogger>());
  return Status::OK();
}

ClientImpl::ClientImpl(const HostId& pilot_host_id,
                       const HostId& copilot_host_id,
                       TenantID tenant_id,
                       int port,
                       PublishCallback publish_callback,
                       SubscribeCallback subscription_callback,
                       MessageReceivedCallback receive_callback,
                       std::shared_ptr<Logger> info_log)
: env_(Env::Default())
, pilot_host_id_(pilot_host_id)
, copilot_host_id_(copilot_host_id)
, tenant_id_(tenant_id)
, publish_callback_(publish_callback)
, subscription_callback_(subscription_callback)
, receive_callback_(receive_callback) {
  // Initialise host_id_.
  char myname[1024];
  if (gethostname(&myname[0], sizeof(myname))) {
    assert(false);
  }
  host_id_ = HostId(myname, port);

  // Setup callbacks.
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mDeliver] = [this] (std::unique_ptr<Message> msg) {
    ProcessData(std::move(msg));
  };
  callbacks[MessageType::mDataAck] = [this] (std::unique_ptr<Message> msg) {
    ProcessDataAck(std::move(msg));
  };
  callbacks[MessageType::mMetadata] = [this] (std::unique_ptr<Message> msg) {
    ProcessMetadata(std::move(msg));
  };

  // Construct message loop.
  msg_loop_ = new MsgLoop(Env::Default(),
                          EnvOptions(),
                          port,
                          info_log,
                          "client");
  msg_loop_->RegisterCallbacks(callbacks);

  msg_loop_thread_ = std::thread([this] () {
    env_->SetThreadName(env_->GetCurrentThreadId(), "client");
    msg_loop_->Run();
  });

  while (!msg_loop_->IsRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

ClientImpl::~ClientImpl() {
  // Delete the message loop.
  // This stops the event loop, which may block.
  delete msg_loop_;

  // Wait for thread to join.
  msg_loop_thread_.join();
}

PublishStatus ClientImpl::Publish(const Topic& name,
                                  const NamespaceID namespaceId,
                                  const TopicOptions& options,
                                  const Slice& data,
                                  const MsgId messageId) {
  if (namespaceId <= 100) {       // Namespace <= 100 are reserved
    return PublishStatus(Status::InvalidArgument(
                         "NamespaceID must be greater than 100."),
                         messageId);
  }
  // Construct message.
  MessageData message(MessageType::mPublish,
                      tenant_id_,
                      host_id_,
                      Slice(name),
                      namespaceId,
                      data,
                      options.retention);

  // Take note of message ID before we move into the command.
  const MsgId empty_msgid = MsgId();
  if (!(messageId == empty_msgid)) {
    message.SetMessageId(messageId);
  }
  const MsgId msgid = message.GetMessageId();

  // Get a serialized version of the message
  std::string serialized;
  message.SerializeToString(&serialized);

  // Construct command.
  std::unique_ptr<Command> command(new ClientCommand(msgid,
                                                     pilot_host_id_,
                                                     std::move(serialized),
                                                     env_->NowMicros()));

  // Send to event loop for processing (the loop will free it).
  std::unique_lock<std::mutex> lock(message_sent_mutex_);
  bool added = messages_sent_.insert(msgid).second;
  lock.unlock();

  assert(added);
  Status status = msg_loop_->SendCommand(std::move(command));
  if (!status.ok() && added) {
    std::unique_lock<std::mutex> lock1(message_sent_mutex_);
    messages_sent_.erase(msgid);
    lock1.unlock();
  }

  // Return status with the generated message ID.
  return PublishStatus(status, msgid);
}

// Subscribe to a specific topics.
void ClientImpl::ListenTopics(std::vector<SubscriptionPair>& names,
                              const TopicOptions& options) {
  std::vector<TopicPair> list;

  // Construct subscription list
  for (const auto& elem : names) {
    list.push_back(TopicPair(elem.seqno, elem.topic_name,
                             MetadataType::mSubscribe, elem.namespace_id));
  }
  // Construct message.
  MessageMetadata message(tenant_id_,
                          MessageMetadata::MetaType::Request,
                          host_id_,
                          list);

  // Get a serialized version of the message
  std::string serialized;
  message.SerializeToString(&serialized);

  // Construct command.
  std::unique_ptr<Command> command(new ClientCommand(copilot_host_id_,
                                                     std::move(serialized),
                                                     env_->NowMicros()));
  // Send to event loop for processing (the loop will free it).
  Status status = msg_loop_->SendCommand(std::move(command));

  // If there was any error, invoke callback with appropriate status
  if (!status.ok()) {
    SubscriptionStatus error_msg;
    error_msg.status = status;
    subscription_callback_(error_msg);
  }
}

/*
** Process a received data message and deliver it to application.
*/
void ClientImpl::ProcessData(std::unique_ptr<Message> msg) {
  msg_loop_->ThreadCheck();
  const MessageData* data = static_cast<const MessageData*>(msg.get());

  // extract topic name from message
  std::string name = data->GetTopicName().ToString();

  // verify that we are subscribed to this topic
  std::unordered_map<Topic, SequenceNumber>::iterator iter =
                                        topic_map_.find(name);
  // No active subscription to this topic, ignore message
  if (iter == topic_map_.end()) {
    return;
  }
  SequenceNumber last_msg_received = iter->second;

  // Old message, ignore it
  if (data->GetSequenceNumber() < last_msg_received) {
    return;
  }
  // update last seqno received for this topic
  iter->second = data->GetSequenceNumber();

  // Create message wrapper for client (do not copy payload)
  unique_ptr<MessageReceivedClient> newmsg(
                                     new MessageReceivedClient(std::move(msg)));

  // deliver message to application
  receive_callback_(std::move(newmsg));
}

// Process the Ack message for a Data Message
void ClientImpl::ProcessDataAck(std::unique_ptr<Message> msg) {
  msg_loop_->ThreadCheck();
  const MessageDataAck* ackMsg = static_cast<const MessageDataAck*>(msg.get());

  // For each ack'd message, if it was waiting for an ack then remove it
  // from the waiting list and let the application know about the ack.
  for (const auto& ack : ackMsg->GetAcks()) {
    // Attempt to remove sent message from list.
    std::unique_lock<std::mutex> lock1(message_sent_mutex_);
    bool successful_ack = messages_sent_.erase(ack.msgid);
    lock1.unlock();

    // If successful, invoke callback.
    if (successful_ack) {
      ResultStatus rs;
      rs.msgid = ack.msgid;
      if (ack.status == MessageDataAck::AckStatus::Success) {
        rs.status = Status::OK();
        rs.seqno = ack.seqno;
      } else {
        rs.status = Status::IOError("publish failed");
      }

      if (publish_callback_) {
        publish_callback_(rs);
      }
    } else {
      // We've received an ack for a message that has already been acked
      // (or was never sent). This is possible if a message was sent twice
      // before the first ack arrived, so just ignore.
    }
  }
}

// Process Metadata response messages arriving from the Cloud.
void ClientImpl::ProcessMetadata(std::unique_ptr<Message> msg) {
  msg_loop_->ThreadCheck();
  SubscriptionStatus ret;
  const MessageMetadata* meta = static_cast<const MessageMetadata*>(msg.get());

  // The client should receive only responses to subscribe/unsubscribe.
  assert(meta->GetMetaType() == MessageMetadata::MetaType::Response);
  std::vector<TopicPair> pairs = meta->GetTopicInfo();

  // This is the response ack of a subscription request sent earlier

  for (const auto& elem : pairs) {
    // record confirmed subscriptions
    topic_map_[elem.topic_name] = elem.seqno;

    // invoke application-registered callback
    if (subscription_callback_) {
      ret.status = Status::OK();
      ret.seqno = elem.seqno;  // start seqno of this subscription
      ret.subscribed = true;   // subscribed to this topic
      subscription_callback_(ret);
    }
  }
}

MessageReceivedClient::~MessageReceivedClient() {
}

}  // namespace rocketspeed
