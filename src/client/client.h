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
#include "include/RocketSpeed.h"
#include "include/Env.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/msg_loop.h"
#include "message_received.h"

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
  virtual void ListenTopics(std::vector<SubscriptionPair>& names,
                            const TopicOptions& options);

  ClientImpl(const HostId& pilot_host_id,
             const HostId& copilot_host_id,
             TenantID tenant_id,
             int port,
             PublishCallback publish_callback,
             SubscribeCallback subscription_callback,
             MessageReceivedCallback receive_callback,
             std::shared_ptr<Logger> info_log);

 private:
  // Callback for a Data message
  void ProcessData(std::unique_ptr<Message> msg);

  // Callback for MessageDataAck message.
  void ProcessDataAck(std::unique_ptr<Message> msg);

  // Callback for MessageMetadata message.
  void ProcessMetadata(std::unique_ptr<Message> msg);

  // The environment
  Env* env_;

  // HostId of this machine, i.e. the one the producer is running on.
  HostId host_id_;

  // HostId of pilot/copilot machines to send messages to.
  HostId pilot_host_id_;
  HostId copilot_host_id_;

  // Tenant ID of this producer.
  TenantID tenant_id_;

  // Incoming message loop object.
  MsgLoop* msg_loop_ = nullptr;
  std::thread msg_loop_thread_;

  // Messages sent, awaiting ack.
  std::set<MsgId> messages_sent_;
  std::mutex message_sent_mutex_;  // mutex for operators on messages_sent_

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

}  // namespace rocketspeed
