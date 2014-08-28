// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

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

class ProducerHandle;
class ConsumerHandle;

/*
 * The Producer is used to produce messages for a single topic
 * or multiple topics.
 */
class Producer {
 public:
  /**
   * Opens a Producer. This can create connections to the Cloud Service
   * Provider, validate credentials, etc.
   *
   * @param config  The configuration of this service provider
   * @return on success returns OK(), otherwise errorcode
   */
  static Status Open(const Configuration& config,
                     Producer** producer);


  /**
   * Closes this Producer's connection to the Cloud Service Provider.
   * After deletion, publishing from a ProducerHandle opened from this producer
   * will fail with an errorcode.
   */
  virtual ~Producer();

  /**
   * Creates or opens a Topic. New messages can be sent by
   * using the ProducerHandle.
   *
   * @param name Name of this topic to be opened
   * @param options  Quality of service for this Topic
   * @return on success returns OK(), otherwise errorcode
   */
  virtual Status OpenTopic(const Topic& name,
                           const TopicOptions& options,
                           ProducerHandle** handle);

  /**
   * Creates or opens the specified Topics. Each Topic is
   * associated with the handle. It is possible that some
   * topic were opened successfuly while some others failed.
   * ProducerHandles are returned only for those topics
   * that were opened successfully.
   *
   * @param names Names of the topics to be opened
   * @param options  Quality of service for this Topic
   * @return on success returns OK(), otherwise errorcode
   */
  virtual std::vector<Status> OpenTopics(std::vector<Topic>& names,
                                 const TopicOptions& options,
                                 std::vector<ProducerHandle*>* handles);
};

/**
 * The ProducerHandle is used to publish new messages to a
 * single topic
 */
class ProducerHandle {
 public:
  /**
   * Publishes a new message to the Topic. The return parameter indicates
   * whether the publish was successful, and if so it returns the
   * messageid and sequenceId of this message.
   */
  virtual ResultStatus Publish(const Slice& data);

  virtual ~ProducerHandle();
};

/**
 * A Consumer object is used to consume messages from multiple topics.
 *
 * Consumer is completely thread-safe.
 */
class Consumer {
 public:
  /**
   * Opens a Consumer. This can create connections to the Cloud Service
   * Provider, validate credentials, etc.
   *
   * @param config  The configuration of this service provider
   * @return on success returns OK(), otherwise errorcode
   */
  static Status Open(const Configuration& config,
                     Consumer** consumer);

  /**
   * Closes this Consumer's connection to the Cloud Service Provider.
   * All ConsumerHandles created through this Consumer will continue to be
   * accessible, but will no longer receive any messages.
   */
  virtual ~Consumer();

  /**
   * Opens a Topic for reading. The Topic should already exist.
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
                             ConsumerHandle** handle);

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
                                           ConsumerHandle** handle);
};


/**
 * The ConsumerHandle is used to listen to new messages to one
 * or more topics. ConsumerHandle must only be used from one thread, but does
 * not need be the same thread it was created on.
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
