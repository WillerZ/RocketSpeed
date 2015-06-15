// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "include/RocketSpeed.h"
#include "src/rollcall/RollCall.h"
#include "src/client/client.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

/**
 * An implementation or the RollcallStream capable of both reading and writing
 * Rollcall entires.
 */
class RollcallImpl : public RollcallStream {
 public:
  RollcallImpl(std::unique_ptr<ClientImpl> client, TenantID tenant_id);

  RollcallShard GetNumShards(const NamespaceID& namespace_id) override;

  Status Subscribe(const NamespaceID& namespace_id,
                   RollcallShard shard_id,
                   RollCallback callback) override;

  /** Writes an entry to the rollcall topic. */
  Status WriteEntry(const TenantID tenant_id,
                    const Topic& topic_name,
                    const NamespaceID& nsid,
                    bool isSubscription,
                    PublishCallback publish_callback);

  virtual ~RollcallImpl() = default;

 private:
  static const NamespaceID kRollcallNamespace;

  const std::unique_ptr<ClientImpl> client_;
  const TenantID tenant_id_;

  RollcallShard GetRollcallShard(const NamespaceID& namespace_id,
                                 const Topic& topic);

  Topic GetRollcallTopicName(const NamespaceID& nsid, RollcallShard shard_id);
};

}  // namespace rocketspeed
