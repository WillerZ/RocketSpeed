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
#include "src/messages/stream_socket.h"
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

  virtual PublishStatus Publish(const TenantID tenant_id,
                                const Topic& name,
                                const NamespaceID& namespaceId,
                                const TopicOptions& options,
                                const Slice& data,
                                PublishCallback callback,
                                const MsgId messageId);

  virtual void ListenTopics(const TenantID tenant_id,
                            const std::vector<SubscriptionRequest>& names);

  virtual void Acknowledge(const MessageReceived& message);

  virtual void SaveSubscriptions(SnapshotCallback snapshot_callback);

  ClientImpl(BaseEnv* env,
             std::shared_ptr<Configuration> config,
             std::shared_ptr<WakeLock> wake_lock,
             std::unique_ptr<MsgLoopBase> msg_loop,
             std::unique_ptr<SubscriptionStorage> storage,
             std::shared_ptr<Logger> info_log,
             bool is_internal);

  Statistics GetStatisticsSync() const;

 private:
  // Handler for SubscriptionStorage load all events.
  void ProcessRestoredSubscription(
    const std::vector<SubscriptionRequest>& restored);

  /** A non-owning pointer to the environment. */
  BaseEnv* env_;

  // Configuration.
  std::shared_ptr<Configuration> config_;

  // A wake lock used on mobile devices.
  SmartWakeLock wake_lock_;

  // Incoming message loop object.
  std::unique_ptr<MsgLoopBase> msg_loop_;
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

  /** Shards topics into workers */
  int GetWorkerForTopic(const Topic& name) const;

  /** Informs the client about a status of a subscription request. */
  void AnnounceSubscriptionStatus(const TenantID tenant_id,
                                  TopicPair request, Status status);

  /** Handles subscription requests from the client. */
  void HandleSubscription(const TenantID tenant_id,
                          TopicPair request, int worker_id);

  /** Handler for messages received on some topic. */
  void ProcessData(std::unique_ptr<Message> msg, StreamID origin);

   /** Handler for messages received on some topic. */
  void ProcessGap(std::unique_ptr<Message> msg, StreamID origin);

  /** Handler for metadata messages received. */
  void ProcessMetadata(std::unique_ptr<Message> msg, StreamID origin);
};

}  // namespace rocketspeed
