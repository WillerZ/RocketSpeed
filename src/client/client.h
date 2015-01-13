// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
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
#include "src/client/topic_id.h"
#include "src/client/message_received.h"
#include "src/messages/msg_loop_base.h"
#include "src/port/port.h"

namespace rocketspeed {

class Logger;

// Internal implementation of the Client API.
class ClientImpl : public Client {
 public:
  virtual ~ClientImpl();

  virtual PublishStatus Publish(const Topic& name,
                                const NamespaceID namespaceId,
                                const TopicOptions& options,
                                const Slice& data,
                                PublishCallback callback,
                                const MsgId messageId = MsgId());

  virtual void ListenTopics(const std::vector<SubscriptionRequest>& names);

  virtual void Acknowledge(const MessageReceived& message);

  ClientImpl(const HostId& pilot_host_id,
             const HostId& copilot_host_id,
             TenantID tenant_id,
             MsgLoopBase* msg_loop,
             SubscribeCallback subscription_callback,
             MessageReceivedCallback receive_callback,
             std::unique_ptr<SubscriptionStorage> storage,
             std::shared_ptr<Logger> info_log);

  Statistics GetStatistics() const;

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

  void IssueSubscriptions(const std::vector<TopicPair> &topics, int worker_id);

  int GetWorkerForTopic(const Topic& name) const;

  // The environment
  ClientEnv* env_;

  // HostId of pilot/copilot machines to send messages to.
  HostId pilot_host_id_;
  HostId copilot_host_id_;

  // Tenant ID of this producer.
  TenantID tenant_id_;

  // Incoming message loop object.
  MsgLoopBase* msg_loop_ = nullptr;
  std::thread msg_loop_thread_;

  // callback for incoming ack message for a subscription/unsubscription
  SubscribeCallback subscription_callback_;

  // callback for incoming data messages
  MessageReceivedCallback receive_callback_;

  // Persistent subscription storage
  std::unique_ptr<SubscriptionStorage> storage_;

  // Main logger for the client
  const std::shared_ptr<Logger> info_log_;

  // Published message awaiting response.
  struct PendingAck {
    PendingAck(PublishCallback _callback, std::string _data)
    : callback(std::move(_callback))
    , data(std::move(_data)) {}

    PublishCallback callback;
    std::string data;
  };

  // Data per worker thread.
  // Aligned to avoid false sharing.
  struct alignas(CACHE_LINE_SIZE) WorkerData {
    // Map a subscribed topic name to the last sequence number
    // received for this topic (one per worker thread).
    std::unordered_map<TopicID, SequenceNumber> topic_map;

    // Messages sent, awaiting ack.
    // Maps message ID -> pre-serialized message.
    std::unordered_map<MsgId, PendingAck, MsgId::Hash> messages_sent;
    std::mutex message_sent_mutex;  // mutex for operators on messages_sent_
  };
  std::unique_ptr<WorkerData[]> worker_data_;

  // Worker ID to send next message from.
  // This loops in a round robin fashion.
  std::atomic<int> next_worker_id_;
};

}  // namespace rocketspeed
