// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/client/client.h"

#include <cassert>
#include <chrono>
#include <functional>
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
#include "include/WakeLock.h"
#include "src/client/message_received.h"
#include "src/client/smart_wake_lock.h"
#include "src/messages/msg_loop_base.h"
#include "src/port/port.h"
#include "src/util/common/hash.h"

#ifndef USE_MQTTMSGLOOP
#include "src/messages/msg_loop.h"
#else
#include "rocketspeed/mqttclient/mqtt_msg_loop.h"
#include "rocketspeed/mqttclient/proxygen_mqtt.h"
#endif

namespace rocketspeed {

/** An implementation of the Client API that represents a creation error. */
class ClientCreationError : public Client {
 public:
  explicit ClientCreationError(const Status& creationStatus)
    : creationStatus_(creationStatus)
  {}

  virtual Status Start(SubscribeCallback,
                       MessageReceivedCallback,
                       RestoreStrategy) {
    return creationStatus_;
  }

  virtual PublishStatus Publish(const Topic&,
                                const NamespaceID&,
                                const TopicOptions&,
                                const Slice&,
                                PublishCallback,
                                const MsgId message_id) {
    return PublishStatus(creationStatus_, message_id);
  }

  virtual void ListenTopics(const std::vector<SubscriptionRequest>&) {}
  virtual void Acknowledge(const MessageReceived&) {}
  virtual void SaveSubscriptions(SnapshotCallback) {}

 private:
  Status creationStatus_;
};

Status Client::Create(ClientOptions options, std::unique_ptr<Client>* client) {
  assert (client);
  std::unique_ptr<ClientImpl> clientImpl;
  auto st = ClientImpl::Create(std::move(options), &clientImpl);
  if (st.ok()) {
    client->reset(clientImpl.release());
  } else {
    client->reset(new ClientCreationError(st));
  }
  return st;
}

Status ClientImpl::Create(ClientOptions options,
                          std::unique_ptr<ClientImpl>* client,
                          bool is_internal) {
  assert (client);
  if (!is_internal) {
    if (options.config.GetTenantID() <= 100) {
      return Status::InvalidArgument("TenantId must be greater than 100.");
    }
  }
  // Validate arguments.
  if (options.config.GetPilotHostIds().empty()) {
    return Status::InvalidArgument("Must have at least one pilot.");
  }
  if (options.config.GetCopilotHostIds().empty()) {
    return Status::InvalidArgument("Must have at least one copilot.");
  }
  if (!options.info_log) {
    options.info_log = std::make_shared<NullLogger>();
  }

#ifndef USE_MQTTMSGLOOP
  MsgLoop* msg_loop_ = new MsgLoop(options.env,
                                   EnvOptions(),
                                   0,
                                   options.config.GetNumWorkers(),
                                   options.info_log,
                                   "client",
                                   options.client_id);
#else
  MQTTMsgLoop* msg_loop_ = new MQTTMsgLoop(
      options.env,
      options.client_id,
      options.config.GetPilotHostIds().front(),
      options.username,
      options.access_token,
      true, // We enable SSL when talking over MQTT.
      options.info_log,
      &ProxygenMQTTClient::Create);
#endif

  // TODO(pja) 1 : Just using first pilot for now, should use some sort of map.
  client->reset(new ClientImpl(options.env,
                               options.wake_lock,
                               options.config.GetPilotHostIds().front(),
                               options.config.GetCopilotHostIds().front(),
                               options.config.GetTenantID(),
                               msg_loop_,
                               std::move(options.storage),
                               options.info_log,
                               is_internal));
  return Status::OK();
}

/** Receive status */
enum ReceiveStatus {
  kNotSubscribed = 0,
  kDuplicate,
  kOk,
};

/** State of a single subscription. */
class SubscriptionState {
 public:
  SubscriptionState() : acks_to_skip(-1) {
  }

  ReceiveStatus ReceiveMessage(SequenceNumber current, SequenceNumber prev) {
    assert(acks_to_skip >= -1);
    // We currently cannot reorder an ACK with a message on the topic, we also
    // cannot lose it, therefore we can drop any messages before the last ACK
    // we're waiting for arrives.
    if (acks_to_skip >= 0) {
      return ReceiveStatus::kNotSubscribed;
    }
    SequenceNumber expected = expected_seqno.get();
    // Update last seqno received for this topic, only if there is no pending
    // request on the topic.
    assert(acks_to_skip < 0);
    expected_seqno = current + 1;
    if (expected > current || expected < prev) {
      return ReceiveStatus::kDuplicate;
    }
    return ReceiveStatus::kOk;
  }

  bool IssueRequest(TopicPair* request) {
    assert(acks_to_skip >= -1);
    // If there is a pending request, we'll have to skip corresponding ACK,
    // otherwise we just note that we have one now.
    acks_to_skip++;
    // Update expected sequence number.
    expected_seqno = request->topic_type == MetadataType::mSubscribe
                         ? request->seqno
                         : SubscriptionStart();
    // If we had a pending before, we need to communicate its status to the
    // client, that's why we swap requests.
    if (acks_to_skip > 0) {
      std::swap(pending_request, *request);
      return true;
    } else {
      pending_request = std::move(*request);
      return false;
    }
  }

  bool AcknowledgeRequest(const TopicPair& request) {
    assert(acks_to_skip >= -1);
    if (acks_to_skip < 0) {
      // There is no pending request, don't announce status.
      return false;
    } else {
      // We're waiting for one less now.
      acks_to_skip--;
      // If that was the last one (the one currently ACKed)...
      if (acks_to_skip < 0) {
        // Pending request and current request must match, as we cannot lose or
        // reorder ACKs without losing entire subscription state, in which case
        // pending request will be the only one that we reissue on reconnection.
        assert(pending_request.namespace_id == request.namespace_id);
        assert(pending_request.topic_name == request.topic_name);
        assert(pending_request.seqno == 0 ||
               pending_request.seqno == request.seqno);
        assert(pending_request.topic_type == request.topic_type);
        assert(pending_request.topic_type == MetadataType::mUnSubscribe ||
               expected_seqno.get() == pending_request.seqno);
        // If the request was to start reading from tail of the topic, the
        // response carries the first seqno.
        if (pending_request.seqno == 0) {
          expected_seqno = request.seqno;
        }
        // Erase the task stored in the client (as it contains at least two
        // strings).
        pending_request = TopicPair();
        return true;
      }
      return false;
    }
  }

  void MarkAsLost() {
    acks_to_skip = -1;
  }

 private:
  /**
   * Next expected sequence number on this topic according to the most recently
   * issued request.
   * This is set when we issue subscription and advances on every message if
   * there is no pending request, otherwise remains unchanged.
   * In case we need to resubscribe to all topics (e.g. after reconnection),
   * this is the information that we use.
   * If the user requested to unsubscribe from the topic, this is absent.
   */
  SubscriptionStart expected_seqno;
  /**
   * The number of ACKs that should be handled silently, because corresponding
   * requests were replaced and the fact was communicated to the user.
   * Nonnegative value indicates presence of pending request.
   */
  int acks_to_skip;
  /**
   * Unacked subscription request.
   * We only keep the last pending request for each topic, and when the new one
   * is issued by the client before the previous one is acknowledged, we invoke
   * callback telling user that we replaced pending with the new one. ACK for
   * replaced request will be handled silently.
   */
  TopicPair pending_request;

  // Noncopyable
  SubscriptionState(const SubscriptionState&) = delete;
  SubscriptionState& operator=(const SubscriptionState&) = delete;
};

/**
 * State of a client. We have one such structure per worker thread, a single
 * topic can have its state in only one such structure. Aligned to avoid false
 * sharing.
 */
class alignas(CACHE_LINE_SIZE) ClientWorkerData {
 public:
  typedef std::unordered_map<TopicID, SubscriptionState> SubscriptionStateMap;

  /** Stream socket used by this worker to talk to the copilot. */
  StreamSocket copilot_socket;

  ClientWorkerData() = default;
  // Noncopyable
  ClientWorkerData(const ClientWorkerData&) = delete;
  ClientWorkerData& operator=(const ClientWorkerData&) = delete;

  ReceiveStatus ReceiveMessage(const MessageData* data) {
    thread_check_.Check();

    TopicID topic_id(data->GetNamespaceId().ToString(),
                     data->GetTopicName().ToString());
    auto iter = subscriptions_.find(topic_id);
    if (iter == subscriptions_.end()) {
      return ReceiveStatus::kNotSubscribed;
    }
    return iter->second.ReceiveMessage(data->GetSequenceNumber(),
                                       data->GetPrevSequenceNumber());
  }

  ReceiveStatus ReceiveMessage(const MessageGap* gap) {
    thread_check_.Check();

    TopicID topic_id(gap->GetNamespaceId().ToString(),
                     gap->GetTopicName().ToString());
    auto iter = subscriptions_.find(topic_id);
    if (iter == subscriptions_.end()) {
      return ReceiveStatus::kNotSubscribed;
    }
    return iter->second.ReceiveMessage(gap->GetEndSequenceNumber(),
                                       gap->GetStartSequenceNumber());
  }

  bool IssueRequest(TopicPair* request_inout) {
    thread_check_.Check();

    TopicID topic_id(request_inout->namespace_id, request_inout->topic_name);
    // Insert a ClientPerToppic for this topic if it doesn't exist.
    return subscriptions_[std::move(topic_id)].IssueRequest(request_inout);
  }

  bool AcknowledgeRequest(const TopicPair& request) {
    thread_check_.Check();

    TopicID topic_id(request.namespace_id, request.topic_name);
    auto iter = subscriptions_.find(topic_id);
    if (iter == subscriptions_.end()) {
      // We don't care about this topic.
      return false;
    }
    // Acknowledge in the ClientPerTopic.
    auto st = iter->second.AcknowledgeRequest(request);
    // If this request can be released and is an unsubscribe request, then we
    // can remove state as well.
    if (st && request.topic_type == MetadataType::mUnSubscribe) {
      subscriptions_.erase(iter);
    }
    return st;
  }

 private:
  /** Asserts that this part of a state is accessed from a single thread. */
  ThreadCheck thread_check_;
  /** Contains a shard of subscriptions requested by the user. */
  SubscriptionStateMap subscriptions_;
};

ClientImpl::ClientImpl(BaseEnv* env,
                       std::shared_ptr<WakeLock> wake_lock,
                       const HostId& pilot_host_id,
                       const HostId& copilot_host_id,
                       TenantID tenant_id,
                       MsgLoopBase* msg_loop,
                       std::unique_ptr<SubscriptionStorage> storage,
                       std::shared_ptr<Logger> info_log,
                       bool is_internal)
: env_(env)
, wake_lock_(std::move(wake_lock))
, copilot_host_id_(copilot_host_id)
, tenant_id_(tenant_id)
, msg_loop_(msg_loop)
, msg_loop_thread_spawned_(false)
, storage_(std::move(storage))
, info_log_(info_log)
, is_internal_(is_internal)
, publisher_(env, info_log, msg_loop, &wake_lock_, std::move(pilot_host_id)) {
  using std::placeholders::_1;

  // Setup callbacks.
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mDeliver] = [this] (std::unique_ptr<Message> msg) {
    ProcessData(std::move(msg));
  };
  callbacks[MessageType::mGap] = [this] (std::unique_ptr<Message> msg) {
    ProcessGap(std::move(msg));
  };
  callbacks[MessageType::mMetadata] = [this] (std::unique_ptr<Message> msg) {
    ProcessMetadata(std::move(msg));
  };

  // Create sharded state.
  worker_data_.reset(new ClientWorkerData[msg_loop_->GetNumWorkers()]);

  // Initialise stream socket for each worker, each of them is independent.
  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    worker_data_[i].copilot_socket = StreamSocket(
        copilot_host_id_.ToClientId(), msg_loop_->GetClientId(i) + 'C');
  }

  msg_loop_->RegisterCallbacks(callbacks);

  if (storage_) {
    // Initialize subscription storage.
    storage_->Initialize(
        std::bind(&ClientImpl::ProcessRestoredSubscription, this, _1),
        msg_loop_);
  }
}

Status ClientImpl::Start(SubscribeCallback subscribe_callback,
                         MessageReceivedCallback receive_callback,
                         RestoreStrategy restore_strategy) {
  subscription_callback_ = std::move(subscribe_callback);
  receive_callback_ = std::move(receive_callback);
  if (!storage_ && restore_strategy == RestoreStrategy::kRestoreOnly) {
    return Status::InvalidArgument(
        "Cannot restore subscriptions without subscription storage strategy.");
  }
  if (!storage_ && restore_strategy == RestoreStrategy::kResubscribe) {
    return Status::InvalidArgument(
        "Cannot resubscribe without subscription storage startegy.");
  }
  if (restore_strategy == RestoreStrategy::kRestoreOnly ||
      restore_strategy == RestoreStrategy::kResubscribe) {
    // Read initial state from snapshot.
    Status status = storage_->ReadSnapshot();
    if (!status.ok()) {
      return status;
    }
  }

  msg_loop_thread_ = env_->StartThread([this]() {
    msg_loop_->Run();
  }, "client");
  msg_loop_thread_spawned_ = true;

  Status st = msg_loop_->WaitUntilRunning();
  if (!st.ok()) {
    return st;
  }

  if (restore_strategy == RestoreStrategy::kResubscribe) {
    // Resubscribe to previously subscribed topics.
    storage_->LoadAll();
  }
  return Status::OK();
}

ClientImpl::~ClientImpl() {
  // Delete the message loop.
  // This stops the event loop, which may block.
  delete msg_loop_;

  if (msg_loop_thread_spawned_) {
    // Wait for thread to join.
    env_->WaitForJoin(msg_loop_thread_);
  }
}

PublishStatus ClientImpl::Publish(const Topic& name,
                                  const NamespaceID& namespace_id,
                                  const TopicOptions& options,
                                  const Slice& data,
                                  PublishCallback callback,
                                  const MsgId message_id) {
  if (!is_internal_) {
    if (IsReserved(namespace_id)) {
      return PublishStatus(
          Status::InvalidArgument(
              "NamespaceID is reserved for internal usage."),
          message_id);
    }
  }
  return publisher_.Publish(tenant_id_,
                            namespace_id,
                            name,
                            options,
                            data,
                            std::move(callback),
                            message_id);
}

void ClientImpl::ListenTopics(const std::vector<SubscriptionRequest>& topics) {
  std::vector<SubscriptionRequest> restore;

  // Determine which requests can be executed right away and which
  // subscriptions need to be restored.
  for (const auto& elem : topics) {
    if (storage_) {
      // Update subscription state accordingly.
      storage_->Update(elem);
    }

    // No need to restore subscription state if we want to unsubscribe.
    if (!elem.subscribe || elem.start) {
      auto type = elem.subscribe ? MetadataType::mSubscribe
                                 : MetadataType::mUnSubscribe;
      auto start = elem.subscribe ? elem.start.get() : 0;
      int worker_id = GetWorkerForTopic(elem.topic_name);
      TopicPair topic(start, elem.topic_name, type, elem.namespace_id);
      std::unique_ptr<Command> command(
          new ExecuteCommand([this, topic, worker_id]() {
            HandleSubscription(topic, worker_id);
          }));
      auto st = msg_loop_->SendCommand(std::move(command), worker_id);
      if (!st.ok() && subscription_callback_) {
        SubscriptionStatus error_msg;
        error_msg.status = std::move(st);
        error_msg.namespace_id = topic.namespace_id;
        error_msg.topic_name = std::move(topic.topic_name);
        subscription_callback_(std::move(error_msg));
      }
    } else {
      restore.push_back(elem);
    }
  }

  if (storage_ && !restore.empty()) {
    wake_lock_.AcquireForLoadingSubscriptions();
    storage_->Load(std::move(restore));
  }
}

void ClientImpl::AnnounceSubscriptionStatus(TopicPair request, Status status) {
  assert(request.topic_type != MetadataType::mNotinitialized);
  if (subscription_callback_) {
    bool subscribe = request.topic_type == MetadataType::mSubscribe;
    subscription_callback_(SubscriptionStatus(request.namespace_id,
                                              std::move(request.topic_name),
                                              request.seqno,
                                              subscribe,
                                              std::move(status)));
  }
}

void ClientImpl::HandleSubscription(TopicPair request, int worker_id) {
  auto& worker_data = worker_data_[worker_id];

  {  // Update state of this subscription.
    TopicPair request_inout = request;
    if (worker_data.IssueRequest(&request_inout)) {
      // Announce previous pending request if any.
      AnnounceSubscriptionStatus(std::move(request_inout),
                                 Status::NotInitialized());
    }
  }

  // Construct a message.
  MessageMetadata message(tenant_id_,
                          MessageMetadata::MetaType::Request,
                          msg_loop_->GetClientId(worker_id),
                          {std::move(request)});

  // Send to event loop for processing.
  wake_lock_.AcquireForSending();
  auto st =
      msg_loop_->SendRequest(message, &worker_data.copilot_socket, worker_id);
  if (!st.ok()) {
    AnnounceSubscriptionStatus(std::move(request), std::move(st));
  }
}

void ClientImpl::Acknowledge(const MessageReceived& message) {
  if (storage_) {
    // Note the +1. We store the next sequence number we want, but the
    // acknowledged message carries the number that we already know about.
    SubscriptionRequest request(message.GetNamespaceId().ToString(),
                                message.GetTopicName().ToString(),
                                true,
                                message.GetSequenceNumber() + 1);
    wake_lock_.AcquireForUpdatingSubscriptions();
    storage_->Update(std::move(request));
  }
}

void ClientImpl::SaveSubscriptions(SnapshotCallback snapshot_callback) {
  if (storage_) {
    storage_->WriteSnapshot(snapshot_callback);
  } else {
    snapshot_callback(Status::InternalError(
        "Cannot save subscriptions without subscription storage."));
  }
}

void ClientImpl::ProcessData(std::unique_ptr<Message> msg) {
  wake_lock_.AcquireForReceiving();

  const MessageData* data = static_cast<const MessageData*>(msg.get());
  // Extract topic from message.
  TopicID topic_id(data->GetNamespaceId().ToString(),
                   data->GetTopicName().ToString());
  LOG_INFO(info_log_,
           "Received message on Topic(%s, %.16s)",
           topic_id.namespace_id.c_str(),
           topic_id.topic_name.c_str());

  // Get worker data that this topic is assigned to.
  const auto worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& worker_data = worker_data_[worker_id];

  // Check if we can deliver the message.
  auto rcv_st = worker_data.ReceiveMessage(data);
  switch (rcv_st) {
    case ReceiveStatus::kNotSubscribed: {
      LOG_INFO(info_log_,
               "Not subscribed to Topic(%s, %s), dropping message (%.16s)",
               topic_id.namespace_id.c_str(),
               topic_id.topic_name.c_str(),
               data->GetPayload().ToString().c_str());
    } break;
    case ReceiveStatus::kDuplicate: {
      LOG_INFO(info_log_,
               "Duplicate message (%.16s)@%llu on Topic(%s, %s)",
               data->GetPayload().ToString().c_str(),
               static_cast<long long unsigned int>(data->GetSequenceNumber()),
               topic_id.namespace_id.c_str(),
               topic_id.topic_name.c_str());
    } break;
    case ReceiveStatus::kOk: {
      // Create message wrapper for client (do not copy payload).
      std::unique_ptr<MessageReceivedClient> newmsg(
          new MessageReceivedClient(std::move(msg)));
      // Deliver message to application.
      receive_callback_(std::move(newmsg));
    } break;
    default:
      LOG_ERROR(
          info_log_, "Unhandled ReceiveStatus: %d", static_cast<int>(rcv_st));
      assert(0);
  }
}

void ClientImpl::ProcessGap(std::unique_ptr<Message> msg) {
  wake_lock_.AcquireForReceiving();

  const MessageGap* gap = static_cast<const MessageGap*>(msg.get());
  // Extract topic from message.
  TopicID topic_id(gap->GetNamespaceId().ToString(),
                   gap->GetTopicName().ToString());
  LOG_INFO(info_log_,
           "Received gap on Topic(%s, %.16s)",
           topic_id.namespace_id.c_str(),
           topic_id.topic_name.c_str());

  // Get worker data that this topic is assigned to.
  const auto worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& worker_data = worker_data_[worker_id];

  // Check if we can deliver the message.
  auto rcv_st = worker_data.ReceiveMessage(gap);
  switch (rcv_st) {
    case ReceiveStatus::kNotSubscribed: {
      LOG_INFO(info_log_,
               "Not subscribed to Topic(%s, %s), dropping gap",
               topic_id.namespace_id.c_str(),
               topic_id.topic_name.c_str());
    } break;
    case ReceiveStatus::kDuplicate: {
      LOG_INFO(info_log_,
               "Duplicate gap %" PRIu64 "-%" PRIu64 " on Topic(%s, %s)",
               gap->GetStartSequenceNumber(),
               gap->GetEndSequenceNumber(),
               topic_id.namespace_id.c_str(),
               topic_id.topic_name.c_str());
    } break;
    case ReceiveStatus::kOk: {
      // Do nothing. Internal.
    } break;
    default:
      LOG_ERROR(
          info_log_, "Unhandled ReceiveStatus: %d", static_cast<int>(rcv_st));
      assert(0);
  }
}

void ClientImpl::ProcessMetadata(std::unique_ptr<Message> msg) {
  wake_lock_.AcquireForReceiving();

  const MessageMetadata* meta = static_cast<const MessageMetadata*>(msg.get());
  // The client should receive only responses to subscribe/unsubscribe.
  if (meta->GetMetaType() != MessageMetadata::MetaType::Response) {
    LOG_WARN(info_log_,
             "Received message type, which is not a response to "
             "subscribe/unsubscribe");
    return;
  }

  // Get worker data that all topics in the message are assigned to.
  const auto worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& worker_data = worker_data_[worker_id];

  // Acknowledge subscription requests.
  const std::vector<TopicPair>& requests = meta->GetTopicInfo();
  for (const auto& request : requests) {
    // If the ACK matches pending request, release the latter one.
    if (worker_data.AcknowledgeRequest(request)) {
      AnnounceSubscriptionStatus(request, Status::OK());
    }
  }
}

void ClientImpl::ProcessRestoredSubscription(
    const std::vector<SubscriptionRequest>& restored) {
  for (const auto& elem : restored) {
    Status st;
    if (!elem.subscribe) {
      // We shouldn't ever restore unsubscribe request.
      LOG_WARN(info_log_,
               "Restored unsubscribe request for namespace %s topic %s",
               elem.namespace_id.c_str(),
               elem.topic_name.c_str());
      assert(0);
      st = Status::InternalError("Restored unsubscribe request");
    } else if (elem.start) {
      int worker_id = GetWorkerForTopic(elem.topic_name);
      TopicPair topic(elem.start.get(),
                      elem.topic_name,
                      MetadataType::mSubscribe,
                      elem.namespace_id);
      std::unique_ptr<Command> command(
          new ExecuteCommand([this, topic, worker_id]() {
            HandleSubscription(topic, worker_id);
          }));
      st = msg_loop_->SendCommand(std::move(command), worker_id);
    } else {
      // We couldn't restore
      st = Status::NotFound();
    }

    if (!st.ok() && subscription_callback_) {
      // Inform the user that subscription restoring failed.
      SubscriptionStatus failed_restore;
      // This is the status returned when we failed to restore subscription.
      failed_restore.status = std::move(st);
      failed_restore.namespace_id = elem.namespace_id;
      failed_restore.topic_name = elem.topic_name;
      subscription_callback_(std::move(failed_restore));
    }
  }
}

Statistics ClientImpl::GetStatistics() const {
  return msg_loop_->GetStatistics();
}

int ClientImpl::GetWorkerForTopic(const Topic& name) const {
  return static_cast<int>(MurmurHash2<std::string>()(name) %
                          msg_loop_->GetNumWorkers());
}

MessageReceivedClient::~MessageReceivedClient() {
}

}  // namespace rocketspeed
