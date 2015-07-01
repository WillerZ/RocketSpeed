// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <tuple>

#include "include/RocketSpeed.h"
#include "src/rollcall/RollCall.h"
#include "src/client/client.h"
#include "src/util/common/coding.h"
#include "src/util/common/hash.h"
#include "src/util/common/statistics.h"
#include "src/util/common/thread_check.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

struct TopicUUID;

/**
 * An implementation or the RollcallStream capable of both reading and writing
 * Rollcall entires.
 */
class RollcallImpl : public RollcallStream {
 public:
  RollcallImpl(std::shared_ptr<ClientImpl> client,
               TenantID tenant_id,
               std::string stats_prefix = "rollcall");

  RollcallShard GetNumShards(const NamespaceID& namespace_id) override;

  Status Subscribe(const NamespaceID& namespace_id,
                   RollcallShard shard_id,
                   RollCallback callback) override;

  /**
   * Writes an entry to the rollcall topic. This isn't written to RocketSpeed
   * until FlushBatch is called.
   */
  Status WriteEntry(const TenantID tenant_id,
                    const TopicUUID& topic,
                    size_t shard_affinity,
                    bool isSubscription,
                    std::function<void(Status)> publish_callback,
                    size_t max_batch_size_bytes);

  /**
   * Flushes all batches that are older than timeout.
   *
   * @param timeout Minimum age of batches to flush.
   */
  void CheckBatchTimeouts(std::chrono::milliseconds timeout);

  const Statistics& GetStatistics() const {
    return stats_.all;
  }

  virtual ~RollcallImpl() = default;

 private:
  static const NamespaceID kRollcallNamespace;

  // A single batch of rollcall entries.
  // These will be written with one RocketSpeed Publish.
  struct Batch {
    std::string payload;
    std::vector<std::function<void(Status)>> callbacks;
  };

  // Batches are keyed by shard, namespace ID, and tenant ID.
  using BatchKey = std::tuple<RollcallShard, NamespaceID, TenantID>;

  struct BatchKeyHash {
    size_t operator()(const BatchKey& key) const {
      return MurmurHash2<size_t, NamespaceID, size_t>()(
        static_cast<size_t>(std::get<0>(key)),
        std::get<1>(key),
        static_cast<size_t>(std::get<2>(key)));
    }
  };

  const std::shared_ptr<ClientImpl> client_;
  const TenantID tenant_id_;
  std::unordered_map<BatchKey, Batch, BatchKeyHash> batches_;
  ThreadCheck thread_check_;
  TimeoutList<BatchKey, BatchKeyHash> batch_timeouts_;

  struct Stats {
    explicit Stats(std::string prefix);

    Statistics all;
    Histogram* batch_size_bytes;
    Histogram* batch_size_entries;
    Counter* batch_writes;
    Counter* entry_writes;
    Counter* batch_size_writes;
    Counter* batch_timeout_writes;
  } stats_;

  RollcallShard GetRollcallShard(const NamespaceID& namespace_id,
                                 size_t shard_affinity);

  Topic GetRollcallTopicName(const NamespaceID& nsid, RollcallShard shard_id);

  /**
   * Flushes any pending writes to the rollcall topic.
   *
   * @param key The key for the batch to flush.
   */
  Status FlushBatch(const BatchKey& key);
};

}  // namespace rocketspeed
