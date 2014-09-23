// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <vector>

#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"

/**
 * This is the RocketSpeed interface. The interface is partitioned
 * into two parts, the Producer and the Consumer. A Producer is used
 * to produce messages for a topic(s) and a Consumer is used to consume
 * messages from a topic.
 */

namespace rocketspeed {

class ConsumerHandle;

typedef std::function<void(ResultStatus)> PublishCallback;

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
   * @param publish_callback Callback for when a sent-message is acknowledged.
   * @param receive_callback Callback for when a message is received.
   * @param client Output parameter for constructred client.
   * @return on success returns OK(), otherwise errorcode
   */
  static Status Open(const Configuration* config,
                     PublishCallback publish_callback,
                     Client** client);

  /**
   * Closes this Client's connection to the Cloud Service Provider.
   */
  virtual ~Client();

  /**
   * Asynchronously publishes a new message to the Topic. The return parameter
   * indicates whether the publish was successfully enqueued.
   *
   * @param name Name of this topic to be opened
   * @param options Quality of service for this Topic
   * @param data Payload of message
   * @return the status and message ID of the published message.
   */
  virtual PublishStatus Publish(const Topic& name,
                                const TopicOptions& options,
                                const Slice& data) = 0;

  /**
   * Opens a Topic for reading.
   * Messages arriving for this topic will be added to the mailbox,
   * accessible through the ConsumerHandle.
   *
   * If a ConsumerHandle is already listening on the Topic then an
   * InvalidArgument status is returned.
   *
   * @param name Name of this topic to be opened
   * @param options Quality of service for this Topic
   * @param handle Output parameter for returned ConsumerHandle
   * @return on success returns OK(), otherwise errorcode
   */
  virtual Status ListenTopic(const Topic& name,
                             const TopicOptions& options,
                             ConsumerHandle** handle) = 0;

  /**
   * Opens the specified Topics and returns a ConsumerHandle that listens
   * to those topics. A vector of statuses is returned as some subscriptions
   * may have failed. The ConsumerHandle only listens on successfully
   * subscribed topics.
   *
   * If a ConsumerHandle is already listening on any Topic then an
   * InvalidArgument status is returned for that topic.
   *
   * @param names Names of the topics to be opened
   * @param options Quality of service for this Topic
   * @param handle Output parameter for returned ConsumerHandle
   * @return for each topic, on success returns OK(), otherwise errorcode
   */
  virtual std::vector<Status> ListenTopics(std::vector<Topic>& names,
                                           const TopicOptions& options,
                                           ConsumerHandle** handle) = 0;
};

/**
 * The ConsumerHandle is used to listen to new messages to one
 * or more topics. This class is not thread-safe.
 */
class ConsumerHandle {
 public:
  /**
   * Deleting this handle means that messages for the corresponding
   * topic are not interesting to this producer any more.
   */
  virtual ~ConsumerHandle();

  /**
   * Waits for a valid message to arrive. This call will block until a message
   * is received.
   *
   * @return the received message.
   */
  virtual MessageReceived Receive();

  /**
   * Checks for an available message and returns immediately. If a message is
   * available then the returned message will have OK() as its status, and the
   * message contents. If no message is available, the status will be NotFound()
   * and the message contents will be empty.
   *
   * @return the received message.
   */
  virtual MessageReceived Peek();
};

}  // namespace rocketspeed
