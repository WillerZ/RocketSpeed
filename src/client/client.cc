// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <chrono>
#include <memory>
#include <set>
#include <thread>
#include <unordered_map>
#include "include/RocketSpeed.h"
#include "include/Env.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/msg_loop.h"
#include "src/port/port.h"
#include "client_commands.h"
#include "message_received.h"

namespace rocketspeed {

Client::~Client() {
}

// Internal implementation of the Client API.
class ClientImpl : public Client {
 public:
  virtual ~ClientImpl();

  virtual PublishStatus Publish(const Topic& name,
                                const NamespaceID namespaceId,
                                const TopicOptions& options,
                                const Slice& data);
  virtual void ListenTopics(std::vector<SubscriptionPair>& names,
                            const TopicOptions& options);

  ClientImpl(const HostId& pilot_host_id,
               TenantID tenant_id,
               int port,
               PublishCallback publish_callback,
               SubscribeCallback subscription_callback,
               MessageReceivedCallback receive_callback);

 private:
  // Callback for a Data message
  static void ProcessData(const ApplicationCallbackContext arg,
                          std::unique_ptr<Message> msg);

  // Callback for MessageDataAck message.
  static void ProcessDataAck(const ApplicationCallbackContext arg,
                             std::unique_ptr<Message> msg);

  // Callback for MessageMetadata message.
  static void ProcessMetadata(const ApplicationCallbackContext arg,
                              std::unique_ptr<Message> msg);

  // HostId of this machine, i.e. the one the producer is running on.
  HostId host_id_;

  // HostId of pilot machine to send messages to.
  HostId pilot_host_id_;

  // Tenant ID of this producer.
  TenantID tenant_id_;

  // Incoming message loop object.
  MsgLoop* msg_loop_ = nullptr;
  std::thread msg_loop_thread_;

  // Messages sent, awaiting ack.
  std::set<MsgId> messages_sent_;

  // callback for incoming data message
  PublishCallback publish_callback_;

  // callback for incoming ack message for a subscription/unsubscription
  SubscribeCallback subscription_callback_;

  // callback for incoming data messages
  MessageReceivedCallback receive_callback_;

  // Map a subscribed topic name to the last sequence number
  // received for this topic.
  std::unordered_map<Topic, SequenceNumber>  topic_map_;
};

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
                               config->GetTenantID(),
                               config->GetClientPort(),
                               publish_callback,
                               subscription_callback,
                               receive_callback);
  return Status::OK();
}

ClientImpl::ClientImpl(const HostId& pilot_host_id,
                           TenantID tenant_id,
                           int port,
                           PublishCallback publish_callback,
                           SubscribeCallback subscription_callback,
                           MessageReceivedCallback receive_callback)
: pilot_host_id_(pilot_host_id)
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
  callbacks[MessageType::mData] = MsgCallbackType(ProcessData);
  callbacks[MessageType::mDataAck] = MsgCallbackType(ProcessDataAck);
  callbacks[MessageType::mMetadata] = MsgCallbackType(ProcessMetadata);

  auto command_callback = [this] (std::unique_ptr<Command> command) {
    assert(command);
    auto cmd = static_cast<ClientCommand*>(command.get());

    if (cmd->GetType() == ClientCommand::Type::Data) {
      // Send a data message to destination
      bool added = messages_sent_.insert(cmd->GetMessageId()).second;
      if (added) {
        msg_loop_->GetClient().Send(cmd->GetRecipient(),
                                    cmd->GetMessage());
      } else {
        // This means we have already sent a message with this ID, which
        // means two separate messages have been given the same ID.
      }
    } else if (cmd->GetType() == ClientCommand::Type::MetaData) {
      // Send a metadata message to destination
      msg_loop_->GetClient().Send(cmd->GetRecipient(),
                                  cmd->GetMessage());
    }
  };

  // Construct message loop.
  msg_loop_ = new MsgLoop(Env::Default(),
                          EnvOptions(),
                          host_id_,
                          std::make_shared<NullLogger>(),  // no logging
                          static_cast<ApplicationCallbackContext>(this),
                          callbacks,
                          command_callback);

  msg_loop_thread_ = std::thread([this] () {
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
                                  const Slice& data) {
  if (namespaceId <= 100) {       // Namespace <= 100 are reserved
    return PublishStatus(Status::InvalidArgument(
                         "NamespaceID must be greater than 100."),
                         MsgId());
  }
  // Construct message.
  MessageData message(tenant_id_,
                      host_id_,
                      Slice(name),
                      namespaceId,
                      data,
                      options.retention);

  // Take note of message ID before we move into the command.
  const MsgId msgid = message.GetMessageId();

  // Construct command.
  std::unique_ptr<Command> command(new ClientCommand(msgid,
                                                     pilot_host_id_,
                                                     message));

  // Send to event loop for processing (the loop will free it).
  Status status = msg_loop_->SendCommand(std::move(command));

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
  // Construct command.
  std::unique_ptr<Command> command(new ClientCommand(pilot_host_id_,
                                                     message));
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
void ClientImpl::ProcessData(const ApplicationCallbackContext arg,
                             std::unique_ptr<Message> msg) {
  ClientImpl* self = static_cast<ClientImpl*>(arg);
  const MessageData* data = static_cast<const MessageData*>(msg.get());

  // extract topic name from message
  std::string name = data->GetTopicName().ToString();

  // verify that we are subscribed to this topic
  std::unordered_map<Topic, SequenceNumber>::iterator iter =
                                        self->topic_map_.find(name);
  // No active subscription to this topic, ignore message
  if (iter == self->topic_map_.end()) {
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
  self->receive_callback_(std::move(newmsg));
}

// Process the Ack message for a Data Message
void ClientImpl::ProcessDataAck(const ApplicationCallbackContext arg,
                                  std::unique_ptr<Message> msg) {
  ClientImpl* self = static_cast<ClientImpl*>(arg);
  const MessageDataAck* ackMsg = static_cast<const MessageDataAck*>(msg.get());

  // For each ack'd message, if it was waiting for an ack then remove it
  // from the waiting list and let the application know about the ack.
  for (const auto& ack : ackMsg->GetAcks()) {
    if (self->messages_sent_.erase(ack.msgid)) {
      ResultStatus rs;
      rs.msgid = ack.msgid;
      if (ack.status == MessageDataAck::AckStatus::Success) {
        rs.status = Status::OK();
        // TODO(pja) 1: get seqno
      } else {
        rs.status = Status::IOError("publish failed");
      }

      if (self->publish_callback_) {
        self->publish_callback_(rs);
      }
    } else {
      // We've received an ack for a message that has already been acked
      // (or was never sent). This is possible if a message was sent twice
      // before the first ack arrived, so just ignore.
    }
  }
}

// Process Metadata response messages arriving from the Cloud.
void ClientImpl::ProcessMetadata(const ApplicationCallbackContext arg,
                                 std::unique_ptr<Message> msg) {
  SubscriptionStatus ret;
  ClientImpl* self = static_cast<ClientImpl*>(arg);
  const MessageMetadata* meta = static_cast<const MessageMetadata*>(msg.get());

  // The client should receive only responses to subscribe/unsubscribe.
  assert(meta->GetMetaType() == MessageMetadata::MetaType::Response);
  std::vector<TopicPair> pairs = meta->GetTopicInfo();

  // This is the response ack of a subscription request sent earlier

  for (const auto& elem : pairs) {
    // record confirmed subscriptions
    self->topic_map_[elem.topic_name] = elem.seqno;

    // invoke application-registered callback
    if (self->subscription_callback_) {
      ret.status = Status::OK();
      ret.seqno = elem.seqno;  // start seqno of this subscription
      ret.subscribed = true;   // subscribed to this topic
      self->subscription_callback_(ret);
    }
  }
}

MessageReceivedClient::~MessageReceivedClient() {
}

}  // namespace rocketspeed
