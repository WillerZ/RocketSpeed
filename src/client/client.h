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
#include "src/client/topic_id.h"
#include "src/client/publisher.h"
#include "src/client/smart_wake_lock.h"
#include "src/messages/messages.h"
#include "src/util/common/base_env.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

class ClientEnv;
class ClientWorkerData;
class MessageReceived;
class Logger;
class WakeLock;

/** Implementation of the client interface. */
class ClientImpl : public Client {
 public:
  static Status Create(ClientOptions client_options,
                       std::unique_ptr<ClientImpl>* client,
                       bool is_internal = false);

  virtual ~ClientImpl();

  virtual Status Start(SubscribeCallback subscription_callback,
                       MessageReceivedCallback receive_callback,
                       RestoreStrategy restore_strategy);

  virtual PublishStatus Publish(const Topic& name,
                                const NamespaceID namespaceId,
                                const TopicOptions& options,
                                const Slice& data,
                                PublishCallback callback,
                                const MsgId messageId);

  virtual void ListenTopics(const std::vector<SubscriptionRequest>& names);

  virtual void Acknowledge(const MessageReceived& message);

  virtual void SaveSubscriptions(SnapshotCallback snapshot_callback);

  ClientImpl(BaseEnv* env,
             std::shared_ptr<WakeLock> wake_lock,
             const HostId& pilot_host_id,
             const HostId& copilot_host_id,
             TenantID tenant_id,
             MsgLoopBase* msg_loop,
             std::unique_ptr<SubscriptionStorage> storage,
             std::shared_ptr<Logger> info_log,
             bool is_internal);

  Statistics GetStatistics() const;

 private:
  // Callback for a Data message
  void ProcessData(std::unique_ptr<Message> msg);

  // Callback for MessageMetadata message.
  void ProcessMetadata(std::unique_ptr<Message> msg);

  // Handler for SubscriptionStorage load all events.
  void ProcessRestoredSubscription(
      const std::vector<SubscriptionRequest>& restored);

  void IssueSubscriptions(std::vector<TopicPair> topics, int worker_id);

  int GetWorkerForTopic(const Topic& name) const;

  /** A non-owning pointer to the environment. */
  BaseEnv* env_;

  // A wake lock used on mobile devices.
  SmartWakeLock wake_lock_;

  // HostId of copilot machines to send messages to.
  HostId copilot_host_id_;

  // Tenant ID of this producer.
  TenantID tenant_id_;

  // Incoming message loop object.
  MsgLoopBase* msg_loop_ = nullptr;
  BaseEnv::ThreadId msg_loop_thread_;
  bool msg_loop_thread_spawned_;

  // callback for incoming ack message for a subscription/unsubscription
  SubscribeCallback subscription_callback_;

  // callback for incoming data messages
  MessageReceivedCallback receive_callback_;

  // Persistent subscription storage
  std::unique_ptr<SubscriptionStorage> storage_;

  // Main logger for the client
  const std::shared_ptr<Logger> info_log_;

  /** State of the Client, sharded by workers. */
  std::unique_ptr<ClientWorkerData[]> worker_data_;

  // If this is an internal client, then we will skip TenantId
  // checks and namespaceid checks.
  const bool is_internal_;

  /** The publisher object, which handles write path in the client. */
  PublisherImpl publisher_;
};

}  // namespace rocketspeed
