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
#include "src/client/client_env.h"
#include "src/client/message_received.h"
#include "src/util/common/hash.h"

#ifndef USE_MQTTMSGLOOP
#include "src/messages/msg_loop.h"
#else
#include "rocketspeed/mqttclient/mqtt_msg_loop.h"
#endif

namespace rocketspeed {

struct ClientResultStatus : public ResultStatus {
 public:
  ClientResultStatus(Status status,
                     std::string serialized_message,
                     SequenceNumber seqno)
  : status_(status)
  , serialized_(std::move(serialized_message))
  , seqno_(seqno) {
    Slice in(serialized_);
    if (!message_.DeSerialize(&in).ok()) {
      // Failed to deserialize a message after it has been serialized?
      assert(false);
      status_ = Status::InternalError("Message corrupt.");
    }
  }

  virtual Status GetStatus() const {
    return status_;
  }

  virtual MsgId GetMessageId() const {
    assert(status_.ok());
    return message_.GetMessageId();
  }

  virtual SequenceNumber GetSequenceNumber() const {
    // Sequence number comes from the ack, not the original message.
    assert(status_.ok());
    return seqno_;
  }

  virtual Slice GetTopicName() const {
    assert(status_.ok());
    return message_.GetTopicName();
  }

  virtual NamespaceID GetNamespaceId() const {
    assert(status_.ok());
    return message_.GetNamespaceId();
  }

  virtual Slice GetContents() const {
    assert(status_.ok());
    return message_.GetPayload();
  }

  ~ClientResultStatus() {}

 private:
  Status status_;
  MessageData message_;
  std::string serialized_;
  SequenceNumber seqno_;
};

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


#ifndef USE_MQTTMSGLOOP
  MsgLoop* msg_loop_ = new MsgLoop(ClientEnv::Default(),
                                   EnvOptions(),
                                   0,
                                   options.config.GetNumWorkers(),
                                   options.info_log,
                                   "client",
                                   options.client_id);
#else
  MQTTMsgLoop* msg_loop_ = new MQTTMsgLoop(
      options.client_id,
      options.config.GetPilotHostIds().front(),
      options.username,
      options.access_token,
      true,
      options.info_log);
#endif

  // Construct new Client client.
  // TODO(pja) 1 : Just using first pilot for now, should use some sort of map.
  *producer = new ClientImpl(options.config.GetPilotHostIds().front(),
                             options.config.GetCopilotHostIds().front(),
                             options.config.GetTenantID(),
                             msg_loop_,
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

ClientImpl::ClientImpl(const HostId& pilot_host_id,
                       const HostId& copilot_host_id,
                       TenantID tenant_id,
                       MsgLoopBase* msg_loop,
                       PublishCallback publish_callback,
                       SubscribeCallback subscription_callback,
                       MessageReceivedCallback receive_callback,
                       std::unique_ptr<SubscriptionStorage> storage,
                       std::shared_ptr<Logger> info_log)
: env_(ClientEnv::Default())
, pilot_host_id_(pilot_host_id)
, copilot_host_id_(copilot_host_id)
, tenant_id_(tenant_id)
, msg_loop_(msg_loop)
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

  worker_data_.reset(new WorkerData[msg_loop_->GetNumWorkers()]);

  msg_loop_->RegisterCallbacks(callbacks);

  if (storage_) {
    // Initialize subscription storage
    auto load_callback = [this](
        const std::vector<SubscriptionRequest>& restored) {
      ProcessRestoredSubscription(restored);
    };
    auto update_callback = [](const SubscriptionRequest& request) {};
    auto snapshot_callback = [](Status status) {};
    storage_->Initialize(load_callback,
                         update_callback,
                         snapshot_callback,
                         msg_loop_);
  }

  msg_loop_thread_ = std::thread([this] () {
    env_->SetCurrentThreadName("client");
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
  const bool is_new_request = true;

  // Get a serialized version of the message
  std::string serialized;
  message.SerializeToString(&serialized);

  // TODO(pja) 1 : Figure out what to do with shared strings.
  std::string dup = serialized;

  // Construct command.
  std::unique_ptr<Command> command(
    new SerializedSendCommand(std::move(serialized),
                              pilot_host_id_.ToClientId(),
                              env_->NowMicros(),
                              is_new_request));

  // Add message to the sent list.
  std::pair<MsgId, std::string> new_msg(msgid, std::move(dup));
  std::unique_lock<std::mutex> lock(worker_data.message_sent_mutex);
  bool added = worker_data.messages_sent.insert(new_msg).second;
  lock.unlock();

  assert(added);

  // Send to event loop for processing (the loop will free it).
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
      // Update subscription state accordingly.
      storage_->Update(elem);
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

  if (storage_ && !restore.empty()) {
    storage_->Load(std::move(restore));
  }
  for (int worker_id = 0; worker_id < msg_loop_->GetNumWorkers(); ++worker_id) {
    if (!subscribe[worker_id].empty()) {
      IssueSubscriptions(subscribe[worker_id], worker_id);
    }
  }
}

void ClientImpl::IssueSubscriptions(const std::vector<TopicPair> &topics,
                                    int worker_id) {
  const bool is_new_request = true;
  // Construct message.
  MessageMetadata message(tenant_id_,
                          MessageMetadata::MetaType::Request,
                          msg_loop_->GetClientId(worker_id),
                          topics);

  // Get a serialized version of the message
  std::string serialized;
  message.SerializeToString(&serialized);

  // Construct command.
  std::unique_ptr<Command> command(
    new SerializedSendCommand(std::move(serialized),
                              copilot_host_id_.ToClientId(),
                              env_->NowMicros(),
                              is_new_request));
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
    storage_->Update(std::move(request));
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

  // Extract topic id from message.
  TopicID topic_id(data->GetNamespaceId(), data->GetTopicName().ToString());

  // Get the topic map for this thread.
  int worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& topic_map = worker_data_[worker_id].topic_map;

  // verify that we are subscribed to this topic
  auto iter = topic_map.find(topic_id);
  // No active subscription to this topic, ignore message
  if (iter == topic_map.end()) {
    LOG_INFO(info_log_,
      "Discarded message (%.16s) due to missing subcription for Topic(%d, %s)",
      data->GetPayload().ToString().c_str(),
      data->GetNamespaceId(),
      topic_id.topic_name.c_str());
    return;
  }
  SequenceNumber last_msg_received = iter->second;

  // Old message, ignore iter
  if (data->GetSequenceNumber() <= last_msg_received) {
    LOG_INFO(info_log_,
      "Message (%.16s)@%lu received out of order on Topic(%d, %s)@%lu",
      data->GetPayload().ToString().c_str(),
      data->GetSequenceNumber(),
      data->GetNamespaceId(),
      topic_id.topic_name.c_str(),
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

  int worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& worker_data = worker_data_[worker_id];

  // For each ack'd message, if it was waiting for an ack then remove it
  // from the waiting list and let the application know about the ack.
  for (const auto& ack : ackMsg->GetAcks()) {
    // Find the message ID from the messages_sent list.
    std::unique_lock<std::mutex> lock1(worker_data.message_sent_mutex);
    auto it = worker_data.messages_sent.find(ack.msgid);
    bool successful_ack = it != worker_data.messages_sent.end();
    lock1.unlock();

    // If successful, invoke callback.
    if (successful_ack) {
      if (publish_callback_) {
        Status st;
        SequenceNumber seqno = 0;
        if (ack.status == MessageDataAck::AckStatus::Success) {
          st = Status::OK();
          seqno = ack.seqno;
        } else {
          st = Status::IOError("Publish failed");
        }

        std::unique_ptr<ClientResultStatus> result_status(
          new ClientResultStatus(st, std::move(it->second), seqno));
        publish_callback_(std::move(result_status));
      }

      // Remove sent message from list.
      std::unique_lock<std::mutex> lock2(worker_data.message_sent_mutex);
      worker_data.messages_sent.erase(it);
      lock2.unlock();
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

  // Get the topic map for this thread.
  int worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& topic_map = worker_data_[worker_id].topic_map;

  std::vector<TopicPair> pairs = meta->GetTopicInfo();
  // This is the response ack of a subscription request sent earlier.
  for (auto& elem : pairs) {
    // Record confirmed subscriptions
    TopicID topic_id(elem.namespace_id, std::move(elem.topic_name));
    // topic_map stores last received, so use -1 to say we want higher seqnos.
    topic_map[topic_id] = elem.seqno - 1;

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
