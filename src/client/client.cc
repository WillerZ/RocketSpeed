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

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/client/client_commands.h"
#include "src/client/client_env.h"
#include "src/client/message_received.h"
#include "src/messages/msg_loop.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

ClientOptions::ClientOptions(const Configuration& _config,
                             ClientID _client_id)
    : config(_config),
      client_id(std::move(_client_id)),
      username(""),
      access_token(""),
      publish_callback(nullptr),
      subscription_callback(nullptr),
      receive_callback(nullptr),
      storage(nullptr),
      info_log(nullptr) {
}

Client::~Client() {
}

// Implementation of Client::Open from RocketSpeed.h
Status Client::Open(ClientOptions&& options_tmp,
                    Client** producer) {
  ClientOptions options(std::move(options_tmp));
  // Validate arguments.
  if (producer == nullptr) {
    return Status::InvalidArgument("producer must not be null.");
  }
  if (options.config.GetTenantID() <= 100) {
    return Status::InvalidArgument("TenantId must be greater than 100.");
  }
  if (options.config.GetPilotHostIds().empty()) {
    return Status::InvalidArgument("Must have at least one pilot.");
  }
  if (!options.info_log) {
    options.info_log = std::make_shared<NullLogger>();
  }

  // Construct new Client client.
  // TODO(pja) 1 : Just using first pilot for now, should use some sort of map.
  *producer = new ClientImpl(options.client_id,
                             options.config.GetPilotHostIds().front(),
                             options.config.GetCopilotHostIds().front(),
                             options.config.GetTenantID(),
                             options.config.GetNumWorkers(),
                             options.publish_callback,
                             options.subscription_callback,
                             options.receive_callback,
                             std::move(options.storage),
                             options.info_log);
  return Status::OK();
}

Status Client::Open(ClientOptions&& client_options,
                    std::unique_ptr<Client>* client) {
  // Validate arguments that we hide from undelying Open call.
  if (client == nullptr) {
    return Status::InvalidArgument("producer must not be null.");
  }

  Client* client_ptr;
  Status status = Client::Open(std::move(client_options), &client_ptr);
  if (status.ok()) {
    client->reset(client_ptr);
  }
  return status;
}

ClientImpl::ClientImpl(const ClientID& client_id,
                       const HostId& pilot_host_id,
                       const HostId& copilot_host_id,
                       TenantID tenant_id,
                       int num_workers,
                       PublishCallback publish_callback,
                       SubscribeCallback subscription_callback,
                       MessageReceivedCallback receive_callback,
                       std::unique_ptr<SubscriptionStorage> storage,
                       std::shared_ptr<Logger> info_log)
: env_(ClientEnv::Default())
, pilot_host_id_(pilot_host_id)
, copilot_host_id_(copilot_host_id)
, tenant_id_(tenant_id)
, publish_callback_(publish_callback)
, subscription_callback_(subscription_callback)
, receive_callback_(receive_callback)
, storage_(std::move(storage))
, info_log_(info_log)
, next_worker_id_(0) {
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

  worker_data_.reset(new WorkerData[num_workers]);

  // Construct message loop.
  msg_loop_ = new MsgLoop(ClientEnv::Default(),
                          EnvOptions(),
                          0,           // no accept loop
                          num_workers,
                          info_log,
                          "client",
                          client_id);

  msg_loop_->RegisterCallbacks(callbacks);
  msg_loop_thread_ = std::thread([this] () {
    env_->SetCurrentThreadName("client");
    msg_loop_->Run();
  });

  while (!msg_loop_->IsRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  if (storage_) {
    // Initialize subscription storage
    storage_->Initialize([this](
        const std::vector<SubscriptionRequest>& restored) {
      ProcessRestoredSubscription(restored);
    });
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

  // Find the worker ID for this topic.
  int worker_id = GetWorkerForTopic(name);
  auto& worker_data = worker_data_[worker_id];

  // Construct message.
  MessageData message(MessageType::mPublish,
                      tenant_id_,
                      msg_loop_->GetClientId(worker_id),
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
  std::unique_ptr<Command> command(
    new ClientCommand(msgid,
                      pilot_host_id_.ToClientId(),
                      std::move(serialized),
                      env_->NowMicros()));

  // Send to event loop for processing (the loop will free it).
  std::unique_lock<std::mutex> lock(worker_data.message_sent_mutex);
  bool added = worker_data.messages_sent.insert(msgid).second;
  lock.unlock();

  assert(added);
  Status status = msg_loop_->SendCommand(std::move(command), worker_id);
  if (!status.ok() && added) {
    std::unique_lock<std::mutex> lock1(worker_data.message_sent_mutex);
    worker_data.messages_sent.erase(msgid);
    lock1.unlock();
  }

  // Return status with the generated message ID.
  return PublishStatus(status, msgid);
}

// Subscribe to a specific topics.
void ClientImpl::ListenTopics(const std::vector<SubscriptionRequest>& topics) {
  // Vector of subscriptions for each worker loop.
  // (subscriptions are sharded on topic to worker loops).
  std::vector<std::vector<TopicPair>> subscribe(msg_loop_->GetNumWorkers());
  std::vector<SubscriptionRequest> restore;

  // Determine which requests can be executed right away and which
  // subscriptions need to be restored.
  for (const auto& elem : topics) {
    if (storage_) {
      // Store or remove subscription state depending whether we subscribe or
      // unsubscribe.
      if (elem.subscribe) {
        storage_->Store(elem);
      } else {
        storage_->Remove(elem);
      }
    }

    if (elem.start) {
      auto type = elem.subscribe ? MetadataType::mSubscribe
                                 : MetadataType::mUnSubscribe;
      int worker_id = GetWorkerForTopic(elem.topic_name);
      subscribe[worker_id].emplace_back(elem.start.get(),
                                        elem.topic_name,
                                        type,
                                        elem.namespace_id);
    } else {
      restore.push_back(elem);
    }
  }

  if (storage_) {
    storage_->Load(restore);
  }
  for (int worker_id = 0; worker_id < msg_loop_->GetNumWorkers(); ++worker_id) {
    if (!subscribe[worker_id].empty()) {
      IssueSubscriptions(subscribe[worker_id], worker_id);
    }
  }
}

void ClientImpl::IssueSubscriptions(const std::vector<TopicPair> &topics,
                                    int worker_id) {
  // Construct message.
  MessageMetadata message(tenant_id_,
                          MessageMetadata::MetaType::Request,
                          msg_loop_->GetClientId(worker_id),
                          topics);

  // Get a serialized version of the message
  std::string serialized;
  message.SerializeToString(&serialized);

  // Construct command.
  std::unique_ptr<Command> command(new ClientCommand(
                                       copilot_host_id_.ToClientId(),
                                       std::move(serialized),
                                       env_->NowMicros()));
  // Send to event loop for processing (the loop will free it).
  Status status = msg_loop_->SendCommand(std::move(command), worker_id);

  // If there was any error, invoke callback with appropriate status
  if (!status.ok() && subscription_callback_) {
    SubscriptionStatus error_msg;
    error_msg.status = status;
    subscription_callback_(error_msg);
  }
}

void ClientImpl::Acknowledge(const MessageReceived& message) {
  if (storage_) {
    SubscriptionRequest request(message.GetNamespaceId(),
                                message.GetTopicName().ToString(),
                                true,
                                message.GetSequenceNumber());
    storage_->Store(request);
  }
}

/*
** Process a received data message and deliver it to application.
*/
void ClientImpl::ProcessData(std::unique_ptr<Message> msg) {
  msg_loop_->ThreadCheck();
  const MessageData* data = static_cast<const MessageData*>(msg.get());

  LOG_INFO(info_log_, "Received data (%.16s)",
    data->GetPayload().ToString().c_str());

  // extract topic name from message
  std::string name = data->GetTopicName().ToString();

  // Get the topic map for this thread.
  int worker_id = MsgLoop::GetThreadWorkerIndex();
  auto& topic_map = worker_data_[worker_id].topic_map;

  // verify that we are subscribed to this topic
  std::unordered_map<Topic, SequenceNumber>::iterator iter =
                                        topic_map.find(name);
  // No active subscription to this topic, ignore message
  if (iter == topic_map.end()) {
    LOG_INFO(info_log_,
      "Discarded message (%.16s) due to missing subcription for Topic(%s)",
      data->GetPayload().ToString().c_str(),
      name.c_str());
    return;
  }
  SequenceNumber last_msg_received = iter->second;

  // Old message, ignore iter
  if (data->GetSequenceNumber() <= last_msg_received) {
    LOG_INFO(info_log_,
      "Message (%.16s)@%lu received out of order on Topic(%s)@%lu",
      data->GetPayload().ToString().c_str(),
      data->GetSequenceNumber(),
      name.c_str(),
      last_msg_received);
    return;
  }
  // update last seqno received for this topic
  iter->second = data->GetSequenceNumber();

  // Create message wrapper for client (do not copy payload)
  std::unique_ptr<MessageReceivedClient> newmsg(
                                     new MessageReceivedClient(std::move(msg)));

  // deliver message to application
  receive_callback_(std::move(newmsg));
}

// Process the Ack message for a Data Message
void ClientImpl::ProcessDataAck(std::unique_ptr<Message> msg) {
  msg_loop_->ThreadCheck();
  const MessageDataAck* ackMsg = static_cast<const MessageDataAck*>(msg.get());

  int worker_id = MsgLoop::GetThreadWorkerIndex();
  auto& worker_data = worker_data_[worker_id];

  // For each ack'd message, if it was waiting for an ack then remove it
  // from the waiting list and let the application know about the ack.
  for (const auto& ack : ackMsg->GetAcks()) {
    // Attempt to remove sent message from list.
    std::unique_lock<std::mutex> lock1(worker_data.message_sent_mutex);
    bool successful_ack = worker_data.messages_sent.erase(ack.msgid);
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

  // Get the topic map for this thread.
  int worker_id = MsgLoop::GetThreadWorkerIndex();
  auto& topic_map = worker_data_[worker_id].topic_map;

  // This is the response ack of a subscription request sent earlier.
  for (const auto& elem : pairs) {
    // record confirmed subscriptions
    // topic_map stores last received, so use -1 to say we want higher seqnos.
    topic_map[elem.topic_name] = elem.seqno - 1;

    // invoke application-registered callback
    if (subscription_callback_) {
      ret.status = Status::OK();
      ret.seqno = elem.seqno;  // start seqno of this subscription
      ret.subscribed = true;   // subscribed to this topic
      subscription_callback_(ret);
    }
  }
}

void ClientImpl::ProcessRestoredSubscription(
    const std::vector<SubscriptionRequest> &restored) {
  // Vector of subscriptions for each worker loop.
  // (subscriptions are sharded on topic to worker loops).
  std::vector<std::vector<TopicPair>> subscribe(msg_loop_->GetNumWorkers());
  // This is the status returned when we failed to restore subscription.
  SubscriptionStatus failed_restore;
  failed_restore.status = Status::NotFound();

  for (const auto& elem : restored) {
    if (!elem.subscribe) {
      // We shouldn't ever restore unsubscribe request.
      LOG_WARN(info_log_,
               "Restored unsubscribe request for namespace %d topic %s",
               elem.namespace_id,
               elem.topic_name.c_str());
      assert(0);
    } else if (elem.start) {
      int worker_id = GetWorkerForTopic(elem.topic_name);
      subscribe[worker_id].emplace_back(elem.start.get(),
                                        elem.topic_name,
                                        MetadataType::mSubscribe,
                                        elem.namespace_id);
    } else {
      // Inform the user that subscription restoring failed.
      if (subscription_callback_) {
        subscription_callback_(failed_restore);
      }
    }
  }

  for (int worker_id = 0; worker_id < msg_loop_->GetNumWorkers(); ++worker_id) {
    if (!subscribe[worker_id].empty()) {
      IssueSubscriptions(subscribe[worker_id], worker_id);
    }
  }
}

Statistics ClientImpl::GetStatistics() const {
  return msg_loop_->GetStatistics();
}

int ClientImpl::GetWorkerForTopic(const Topic& name) const {
  return MurmurHash2<std::string>()(name) % msg_loop_->GetNumWorkers();
}

MessageReceivedClient::~MessageReceivedClient() {
}

}  // namespace rocketspeed
