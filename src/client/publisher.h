// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <vector>

#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/stream_socket.h"

namespace rocketspeed {

class BaseEnv;
class Message;
class MessageData;
class MsgLoopBase;
class SmartWakeLock;
class Logger;
class PublisherWorkerData;

/**
 * A part of RocketSpeed Client responsible for publishing messages only.
 */
class PublisherImpl {
 public:
  /**
   * Creates publisher object from provided parameters.
   *
   * @param env the environment used by the client
   * @param config configuration of RocketSpeed service
   * @param info_log a logger object
   * @param wake_lock a non-owning pointer to the wake lock
   * @param msg_loop a non-owning pointer to the message loop
   */
  PublisherImpl(BaseEnv* env,
                std::shared_ptr<Configuration> config,
                std::shared_ptr<Logger> info_log,
                MsgLoopBase* msg_loop,
                SmartWakeLock* wake_lock);

  ~PublisherImpl();

  /**
   * Publishes a message on behalf of an arbitrary tenant.
   */
  PublishStatus Publish(TenantID tenant_id,
                        const NamespaceID& namespaceId,
                        const Topic& name,
                        const TopicOptions& options,
                        const Slice& data,
                        PublishCallback callback,
                        const MsgId messageId);

  /** Handles goodbye messages for publisher streams. */
  // TODO(stupaq) hide and register callback once we get multi guest MsgLoop
  void ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin);

 private:
  friend class PublisherWorkerData;

  const std::shared_ptr<Configuration> config_;
  const std::shared_ptr<Logger> info_log_;
  MsgLoopBase* const msg_loop_;
  SmartWakeLock* const wake_lock_;

  /** State of the publisher sharded by worker threads. */
  std::vector<PublisherWorkerData> worker_data_;

  /** Handles acknowledgements for published messages. */
  void ProcessDataAck(std::unique_ptr<Message> msg, StreamID origin);

  /** Decides how to shard requests into workers. */
  int GetWorkerForTopic(const Topic& name) const;
};

}  // namespace rocketspeed
