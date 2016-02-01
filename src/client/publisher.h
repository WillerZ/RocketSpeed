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
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"

namespace rocketspeed {

class BaseEnv;
class ClientOptions;
class Flow;
class Message;
class MessageData;
class MsgLoop;
class SmartWakeLock;
class Logger;
class PublisherWorkerData;

/**
 * A part of RocketSpeed Client responsible for publishing messages only.
 */
class PublisherImpl : public NonMovable, public NonCopyable {
 public:
  /**
   * Creates publisher object from provided parameters.
   *
   * @param options A client options.
   * @param msg_loop a non-owning pointer to the message loop
   * @param wake_lock a non-owning pointer to the wake lock
   */
  PublisherImpl(const ClientOptions& options,
                MsgLoop* msg_loop,
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

 private:
  friend class PublisherWorkerData;

  const std::shared_ptr<PublisherRouter> publisher_;
  const std::shared_ptr<Logger> info_log_;
  MsgLoop* const msg_loop_;
  SmartWakeLock* const wake_lock_;

  /** State of the publisher sharded by worker threads. */
  std::vector<std::unique_ptr<PublisherWorkerData>> worker_data_;

  /** Decides how to shard requests into workers. */
  int GetWorkerForTopic(const Topic& name) const;
};

}  // namespace rocketspeed
