// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"

namespace rocketspeed {

class BaseEnv;
class Message;
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
   * @param info_log a logger object
   * @param wake_lock a non-owning pointer to the wake lock
   * @param pilot_host a host id of a pilot that this publisher talks to
   * @param msg_loop a non-owning pointer to the message loop
   */
  PublisherImpl(BaseEnv* env,
                std::shared_ptr<Logger> info_log,
                MsgLoopBase* msg_loop,
                SmartWakeLock* wake_lock,
                HostId pilot_host);

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

 private:
  const std::shared_ptr<Logger> info_log_;
  /** A non-owning pointer to the message loop. */
  MsgLoopBase* msg_loop_;
  /** A non-owning pointer to the wake lock. */
  SmartWakeLock* wake_lock_;
  /** State of the publisher sharded by worker threads. */
  std::unique_ptr<PublisherWorkerData[]> worker_data_;

  HostId pilot_host_;

  /** Handles acknowledgements for published messages. */
  void ProcessDataAck(std::unique_ptr<Message> msg);

  /** Decides how to shard requests into workers. */
  int GetWorkerForTopic(const Topic& name) const;
};

}  // namespace rocketspeed
