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
  /**
   * Opens a Producer. This can create connections to the service
   * provider, validate credentials, etc.
   *
   * @param config  The configuration of this service provider
   * @return on success returns OK(), otherwise errorcode
   */
  static Status Open(const Configuration& config,
                     Producer** producer);

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
  /**
   * Publishes a new message to the Topic. The return parameter indicates
   * whether the publish was successful, and if so it returns the
   * messageid and sequenceId of this message.
   */
   virtual ResultStatus publish(const Slice& data);

   virtual ~ProducerHandle();
};

/**
 * A Consumer object is used to consume messages from multiple topics
 */
class Consumer {
  /**
   * Opens a Consumer. This can create connections to the service
   * provider, validate credentials, etc.
   *
   * @param config  The configuration of this service provider
   * @return on success returns OK(), otherwise errorcode
   */
  static Status Open(const Configuration& config,
                     ConsumerHandle** consumer);
  /**
   * Opens a Topic for reading. The Topic should already exist.
   * The callback is invoked for all arriving messages for
   * this topic.
   *
   * @param name Name of this topic to be opened
   * @param options  Quality of service for this Topic
   * @return on success returns OK(), otherwise errorcode
   */
  virtual Status ListenTopic(const Topic& name,
                             const TopicOptions& options,
                             const ReceiveCallback& callback,
                             ConsumerHandle** handle);

  /**
   * Opens the specified Topics. Each Topic is
   * associated with the handle. It is possible that some
   * topic were opened successfuly while some others failed.
   * ConsumerHandles are returned only for those topics
   * that were opened successfully.
   *
   * @param names Names of the topics to be opened
   * @param options  Quality of service for this Topic
   * @return on success returns OK(), otherwise errorcode
   */
  virtual std::vector<Status> ListenTopics(std::vector<Topic>& names,
                                 const TopicOptions& options,
                                 const std::vector<ReceiveCallback>& callbacks,
                                 std::vector<ConsumerHandle*>* handles);
};


/**
 * The ConsumerHandle is used to listen to new messages to a
 * single topic
 */
class ConsumerHandle {
  /**
   * Deleting this handle means that messages for the corresponding
   * topic are not interesting to this producer any more.
   */
   virtual ~ConsumerHandle();
};

}
