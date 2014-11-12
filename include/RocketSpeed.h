// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"

/**
 * This is the RocketSpeed interface. The interface is partitioned
 * into two parts, the Publish and the Listen.
 * 'Publish' request that messages be written to a topic. The PublishCallback
 * in invoked when the message is reliably acknowledged by the system.
 * 'Listen' requests that messages for a specified topic(s) be delivered
 * to the application. The SubscribeCallback is invoked when the system
 * confirmed the promise to deliver messages for that topic. A message
 * arriving on a subscribed topic causes the MessageReceivedCallback
 * to be invoked.
 */

namespace rocketspeed {

/*
 * The various type of callbacks used by the RocketSpeed Interface.
 * These are described in more details in those apis where they are used.
 */
typedef std::function<void(ResultStatus)> PublishCallback;
typedef std::function<void(SubscriptionStatus)> SubscribeCallback;
typedef std::function<void(std::unique_ptr<MessageReceived>)>
                                          MessageReceivedCallback;

/*
 * The Client is used to produce and consumer messages for a single topic
 * or multiple topics.
 */
class Client {
 public:
  /**
   * Opens a Client. This can create connections to the Cloud Service
   * Provider, validate credentials, etc.
   *
   * @param config The configuration of this service provider
   * @param client_id Identifier for this client
   * @param publish_callback Invoked when a sent-message is acknowledged
   * @param subscription_callback Invoked when a subscription is confirmed
   * @param receive_callback Invoked when a message is received
   * @param client Output parameter for constructred client
   * @return on success returns OK(), otherwise errorcode
   *
   * All or any of the callbacks can be NULL.
   */
  static Status Open(const Configuration* config,
                     const ClientID& client_id,
                     PublishCallback publish_callback,
                     SubscribeCallback subscription_callback,
                     MessageReceivedCallback receive_callback,
                     Client** client);

  /**
   * Closes this Client's connection to the Cloud Service Provider.
   */
  virtual ~Client();

  /**
   * Asynchronously publishes a new message to the Topic. The return parameter
   * indicates whether the publish was successfully enqueued.
   *
   * @param topic_name Name of this topic to be opened
   * @param topic_namespace Namespace of this topic name
   * @param options Quality of service for this Topic
   * @param data Payload of message
   * @params message_id The provided message_id, optional
   * @return the status and message ID of the published message.
   */
  virtual PublishStatus Publish(const Topic& topic_name,
                                const NamespaceID topic_namespace,
                                const TopicOptions& options,
                                const Slice& data,
                                const MsgId message_id = MsgId()) = 0;

  /**
   * Initiates the chain of events to subscribe to a topic.
   * The SubscribeCallback is invoked when this client
   * is successfully subscribed to the specified topic(s).
   *
   * Messages arriving for this topic will be returned to the
   * application via invocation to MessageReceivedCallback.
   *
   * @param name Name of this topic to be opened
   * @param options Quality of service for this Topic
   */
  virtual void ListenTopics(std::vector<SubscriptionPair>& names,
                            const TopicOptions& options) = 0;
};
}  // namespace rocketspeed
