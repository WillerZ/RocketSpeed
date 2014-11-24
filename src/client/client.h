// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "include/RocketSpeed.h"
#include "src/client/client_env.h"
#include "src/client/message_received.h"
#include "src/messages/msg_loop.h"
#include "src/util/common/logger.h"

namespace rocketspeed {

// Internal implementation of the Client API.
class ClientImpl : public Client {
 public:
  virtual ~ClientImpl();

  virtual PublishStatus Publish(const Topic& name,
                                const NamespaceID namespaceId,
                                const TopicOptions& options,
                                const Slice& data,
                                const MsgId messageId = MsgId());

  virtual void ListenTopics(const std::vector<SubscriptionRequest>& names);

  virtual void Acknowledge(const MessageReceived& message);

  ClientImpl(const ClientID& client_id,
             const HostId& pilot_host_id,
             const HostId& copilot_host_id,
             TenantID tenant_id,
             PublishCallback publish_callback,
             SubscribeCallback subscription_callback,
             MessageReceivedCallback receive_callback,
             std::unique_ptr<SubscriptionStorage> storage,
             std::shared_ptr<Logger> info_log);

  const Statistics& GetStatistics() const {
    return msg_loop_->GetStatistics();
  }

 private:
  // Callback for a Data message
  void ProcessData(std::unique_ptr<Message> msg);

  // Callback for MessageDataAck message.
  void ProcessDataAck(std::unique_ptr<Message> msg);

  // Callback for MessageMetadata message.
  void ProcessMetadata(std::unique_ptr<Message> msg);

  // Handler for SubscriptionStorage load all events.
  void ProcessRestoredSubscription(
      const std::vector<SubscriptionRequest>& restored);

  void IssueSubscriptions(const std::vector<TopicPair> &topics);

  // The environment
  ClientEnv* env_;

  // The identifier for the client
  const ClientID client_id_;

  // HostId of pilot/copilot machines to send messages to.
  HostId pilot_host_id_;
  HostId copilot_host_id_;

  // Tenant ID of this producer.
  TenantID tenant_id_;

  // Incoming message loop object.
  MsgLoop* msg_loop_ = nullptr;
  std::thread msg_loop_thread_;

  // Messages sent, awaiting ack.
  std::unordered_set<MsgId, MsgId::Hash> messages_sent_;
  std::mutex message_sent_mutex_;  // mutex for operators on messages_sent_

  // callback for incoming data message
  PublishCallback publish_callback_;

  // callback for incoming ack message for a subscription/unsubscription
  SubscribeCallback subscription_callback_;

  // callback for incoming data messages
  MessageReceivedCallback receive_callback_;

  // Persistent subscription storage
  std::unique_ptr<SubscriptionStorage> storage_;

  // Map a subscribed topic name to the last sequence number
  // received for this topic.
  std::unordered_map<Topic, SequenceNumber>  topic_map_;
};

}  // namespace rocketspeed
