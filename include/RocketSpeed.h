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
#include "include/SubscriptionStorage.h"

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
typedef std::function<void(std::unique_ptr<ResultStatus>)> PublishCallback;
typedef std::function<void(SubscriptionStatus)> SubscribeCallback;
typedef std::function<void(std::unique_ptr<MessageReceived>)>
                                          MessageReceivedCallback;

/**
 * An opaque logger type.
 */
class Logger;

/**
 * Describes the Client object to be created by Client::Open() call.
 */
struct ClientOptions {
  // Configuration of this service provider.
  const Configuration& config;

  // Identifier for this client.
  ClientID client_id;

  // Username for authentication.
  std::string username;

  // Access Token for authentication.
  std::string access_token;

  // Invoked when a subscription is confirmed.
  // Default: nullptr.
  SubscribeCallback subscription_callback;

  // Invoked when a message is received.
  // Default: nullptr.
  MessageReceivedCallback receive_callback;

  // Strategy for storing subscription state.
  // Default: nullptr.
  std::unique_ptr<SubscriptionStorage> storage;

  // Logger that is used for info messages.
  // Default: nullptr.
  std::shared_ptr<Logger> info_log;

  // Constructor which fills default values.
  ClientOptions(const Configuration& _config,
                ClientID _client_id);
};

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
   * @param options The description of client object to be created
   * @return on success returns OK(), otherwise errorcode
   */
  static Status Open(ClientOptions&& client_options,
                     Client** client);

  /**
   * See above for doumentation, this is a wrapper which uses smart pointer to
   * return Client object.
   */
  static Status Open(ClientOptions&& client_options,
                     std::unique_ptr<Client>* client);

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
   * @param callback Callback to call with response from RocketSpeed. This will
   *                 only be called when Publish was successful. The result will
   *                 be ok if the message was succesfully commited into
   *                 RocketSpeed, otherwise an error will be provided.
   * @params message_id The provided message_id, optional
   * @return the status and message ID of the published message.
   */
  virtual PublishStatus Publish(const Topic& topic_name,
                                const NamespaceID topic_namespace,
                                const TopicOptions& options,
                                const Slice& data,
                                PublishCallback callback,
                                const MsgId message_id = MsgId()) = 0;

  /**
   * Initiates the chain of events to subscribe to a topic.
   * The SubscribeCallback is invoked when this client
   * is successfully subscribed to the specified topic(s).
   *
   * Messages arriving for this topic will be returned to the
   * application via invocation to MessageReceivedCallback.
   *
   * @param requests a vector describing subscription/unsubscription requests
   */
  virtual void ListenTopics(const std::vector<SubscriptionRequest>& names) = 0;

  /**
   * Asynchronously acknowledges message to the client.
   *
   * All sequence numbers no later than the sequence number of given message
   * are considered to be processed by the application.
   * Consequently, when application closes the client and then attempts to
   * renew subscription on the same topic, it will be restarted from the next
   * sequence number.
   *
   * @param message the message to be acknowledged.
   */
  virtual void Acknowledge(const MessageReceived& message) = 0;
};

}  // namespace rocketspeed
